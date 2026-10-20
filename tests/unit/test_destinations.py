from backend.destinations.sql_destination import DIALECTS, SqlDestination, create_destination
from tests.fakes import FakeDbApiConnection


def test_upsert_sql_per_dialect():
    for name, expect in {
        "postgresql": 'ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"',
        "mysql": "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)",
        "mssql": "MERGE INTO [dbo].[t] WITH (HOLDLOCK) tgt USING (SELECT ? AS [id], ? AS [name]) src ON (tgt.[id] = src.[id]) WHEN MATCHED THEN UPDATE SET tgt.[name] = src.[name]",
        "oracle": 'USING (SELECT :1 AS "id", :2 AS "name" FROM dual) src ON (tgt."id" = src."id") WHEN MATCHED THEN UPDATE SET "name" = src."name"',
        "sqlite": 'ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"',
    }.items():
        d = SqlDestination(name, lambda: FakeDbApiConnection())
        sql, builder = d.upsert_sql("dbo" if name == "mssql" else "s", "t", ["id", "name"], ["id"])
        assert expect in sql, (name, sql)
        assert builder({"id": 1, "name": "x"}) == [1, "x"]
    # key-only rows: no UPDATE clause for MERGE dialects, dummy update for ON CONFLICT dialects
    sql, _ = SqlDestination("mssql", lambda: None).upsert_sql("dbo", "t", ["id"], ["id"])
    assert "WHEN MATCHED" not in sql
    sql, _ = SqlDestination("postgresql", lambda: None).upsert_sql("s", "t", ["id"], ["id"])
    assert 'DO UPDATE SET "id" = excluded."id"' in sql


def test_mssql_bulk_upsert_uses_temp_table_and_fast_executemany_only_for_inserts():
    conn = FakeDbApiConnection()
    conn.add(r"INFORMATION_SCHEMA.COLUMNS", (["COLUMN_NAME", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION"],
                                            [["id", "int", None, 10, 0, None], ["name", "nvarchar", -1, None, None, None],
                                             ["amt", "decimal", None, 18, 2, None], ["ts", "datetime2", None, None, None, 7]]))
    d = SqlDestination("mssql", lambda: conn)
    d.mssql_bulk_threshold = 2
    rows = [{"id": i, "name": "n", "amt": 1.5, "ts": None} for i in range(3)]
    assert d.upsert_rows("dbo", "T", rows, ["id"]) >= 1   # fake cursor reports rowcount 1 for the MERGE
    sqls = [c[0] for c in conn.calls]
    assert any(s.startswith("CREATE TABLE #cdcsync_T ([id] int NULL, [name] nvarchar(max) NULL, [amt] decimal(18,2) NULL, [ts] datetime2(7) NULL)") for s in sqls)
    assert sum(1 for s in sqls if s.startswith("INSERT INTO #cdcsync_T")) == 3
    assert any(s.startswith("MERGE INTO [dbo].[T] WITH (HOLDLOCK) tgt USING #cdcsync_T src") for s in sqls)
    assert sqls[-1] == "DROP TABLE #cdcsync_T" and conn.commits >= 1
    # small batch -> direct MERGE, no temp table
    conn.calls.clear()
    d.upsert_rows("dbo", "T", rows[:1], ["id"])
    assert conn.calls[0][0].startswith("MERGE INTO")


def test_upsert_delete_and_metadata_sqlite():
    import sqlite3
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.execute("CREATE TABLE t (id INTEGER, k2 TEXT, v TEXT, PRIMARY KEY (id, k2))")
    d = SqlDestination("sqlite", lambda: c)
    assert d.get_primary_key(None, "t") == ["id", "k2"]
    d.insert_rows(None, "t", [{"id": 1, "k2": "a", "v": "x"}])
    d.upsert_rows(None, "t", [{"id": 1, "k2": "a", "v": "y"}], ["id", "k2"])
    assert d.query('SELECT v FROM t WHERE id = 1 AND k2 = ?', ["a"])[0]["v"] == "y"
    d.delete_rows(None, "t", [{"id": 1, "k2": "a"}], ["id", "k2"])
    assert d.row_count(None, "t") == 0
    assert d.is_connected()
    d.disconnect()
    assert not d.is_connected()


def test_create_destination_factories():
    assert create_destination({"engine": "sqlite", "path": ":memory:"}).dialect.name == "sqlite"
    for engine in ("mssql", "postgresql", "mysql", "oracle"):
        assert create_destination({"engine": engine, "server": "x", "database": "d"}).dialect.name == DIALECTS[engine].name
    import pytest
    with pytest.raises(ValueError):
        create_destination({"engine": "neo4j"})
