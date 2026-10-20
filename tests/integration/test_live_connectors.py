"""Live connector tests - run against the docker-compose environment.

    docker compose -f docker/docker-compose.integration.yml up -d
    CDCSYNC_LIVE_TESTS=1 pytest tests/integration -m live -v

Each test validates the same contract for an engine: prerequisites are
reported, capture can be enabled, a watermark exists, a change is captured
with the right operation/row image, keyset snapshot chunks work.
"""
import os
import socket
import time

import pytest

from backend.connectors import create_connector
from backend.connectors.mariadb import MariaDBConnector
from backend.connectors.mongodb import MongoConnector
from backend.connectors.mssql import MSSQLConnector
from backend.connectors.oracle import OracleConnector
from backend.connectors.postgresql import PostgresConnector
from backend.connectors.base import Operation

pytestmark = pytest.mark.live

LIVE = os.environ.get("CDCSYNC_LIVE_TESTS") == "1"


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _need(host, port, module=None):
    if not LIVE:
        pytest.skip("set CDCSYNC_LIVE_TESTS=1 to run live connector tests")
    if not _reachable(host, port):
        pytest.skip(f"{host}:{port} not reachable (docker compose up?)")
    if module:
        pytest.importorskip(module)


def _collect(connector, tables, from_pos, timeout=30):
    events, pos = [], from_pos
    end = time.time() + timeout
    while time.time() < end and not events:
        events, pos = connector.read_changes(tables, pos)
        if not events:
            time.sleep(1)
    return events, pos


def test_postgres_live():
    _need("localhost", 5432, "psycopg")
    c = create_connector({"engine": "postgresql", "id": "pg", "server": "localhost", "port": 5432, "database": "source",
                          "username": "cdc", "password": "cdc", "plugin": os.environ.get("PG_PLUGIN", "test_decoding")})
    assert isinstance(c, PostgresConnector)
    c.connect()
    checks = {ch.name: ch for ch in c.check_prerequisites([("public", "customers")])}
    assert checks["wal_level_logical"].ok and checks["role_replication"].ok
    ok, msg = c.enable_capture_database()
    assert ok, msg
    start = c.current_position()
    c._query("INSERT INTO public.customers VALUES (1, 'Ann', 'a@x', 'Oslo') ON CONFLICT (customer_id) DO UPDATE SET name = 'Ann'")
    c._query("UPDATE public.customers SET city = 'Rome' WHERE customer_id = 1")
    c._query("DELETE FROM public.customers WHERE customer_id = 1")
    events, pos = _collect(c, [("public", "customers")], start)
    ops = [e.operation for e in events]
    assert Operation.UPDATE in ops and Operation.DELETE in ops
    upd = next(e for e in events if e.operation == Operation.UPDATE)
    assert upd.after["city"] == "Rome" and upd.before is not None
    c.confirm_position(pos)
    c._query("INSERT INTO public.customers VALUES (2, 'B', NULL, NULL)")
    rows = c.read_snapshot_chunk("public", "customers", ["customer_id", "name"], ["customer_id"], None, 10)
    assert rows and rows[0]["customer_id"] == 2
    c.drop_slot()
    c.disconnect()


def test_mariadb_live():
    _need("localhost", 3306, "pymysqlreplication")
    c = create_connector({"engine": "mariadb", "id": "mdb", "server": "localhost", "port": 3306, "database": "source",
                          "username": "cdc", "password": "cdc", "server_id": 4379})
    assert isinstance(c, MariaDBConnector)
    c.connect()
    checks = {ch.name: ch for ch in c.check_prerequisites([("source", "customers")])}
    assert checks["log_bin"].ok and checks["binlog_format_row"].ok
    start = c.current_position()
    c._query("REPLACE INTO source.customers VALUES (1, 'Ann', 'a@x', 'Oslo')")
    c._query("UPDATE source.customers SET city = 'Rome' WHERE customer_id = 1")
    events, pos = _collect(c, [("source", "customers")], start)
    assert any(e.operation == Operation.UPDATE and e.after["city"] == "Rome" and e.before["city"] == "Oslo" for e in events)
    assert pos.value["file"]
    rows = c.read_snapshot_chunk("source", "customers", ["customer_id", "name"], ["customer_id"], None, 10)
    assert rows[0]["customer_id"] == 1
    c.disconnect()


def test_mongo_live():
    _need("localhost", 27017, "pymongo")
    c = create_connector({"engine": "mongodb", "id": "mg", "server": "localhost", "port": 27017, "database": "source",
                          "options": "directConnection=true", "auth_database": "admin"})
    assert isinstance(c, MongoConnector)
    c.connect()
    checks = {ch.name: ch for ch in c.check_prerequisites([("source", "customers")])}
    assert checks["replica_set_or_sharded"].ok, checks["replica_set_or_sharded"].detail
    coll = c._db()["customers"]
    coll.delete_many({})
    import threading
    stop = threading.Event()
    got = []

    def reader():
        for ev in c.stream_changes([("source", "customers")], None, stop):
            got.append(ev)
            if len(got) >= 3:
                stop.set()
    t = threading.Thread(target=reader, daemon=True)
    t.start()
    time.sleep(2)
    coll.insert_one({"_id": 1, "name": "Ann"})
    coll.update_one({"_id": 1}, {"$set": {"name": "Bob"}})
    coll.delete_one({"_id": 1})
    t.join(30)
    assert [e.operation for e in got] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert got[1].after["name"] == "Bob" and got[2].key == {"_id": 1}
    c.disconnect()


def test_mssql_live():
    _need("localhost", 1433, "pyodbc")
    c = create_connector({"engine": "mssql", "id": "ms", "server": "localhost", "port": 1433, "database": "source",
                          "username": "sa", "password": "CdcSync!2024", "driver": os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")})
    assert isinstance(c, MSSQLConnector)
    c.connect()
    checks = {ch.name: ch for ch in c.check_prerequisites([("dbo", "Customers")])}
    assert checks["database_cdc_enabled"].ok and checks["table_cdc_enabled:dbo.Customers"].ok
    start = c.current_position()
    c._execute("INSERT INTO dbo.Customers (CustomerID, Name) VALUES (1, 'Ann')")
    c._execute("UPDATE dbo.Customers SET City = 'Rome' WHERE CustomerID = 1")
    c._execute("DELETE FROM dbo.Customers WHERE CustomerID = 1")
    events, pos = _collect(c, [("dbo", "Customers")], start, timeout=60)  # capture job latency ~5s
    assert [e.operation for e in events] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert events[1].after["City"] == "Rome" and events[1].source_ts is not None
    assert c.changed_capture_instances([("dbo", "Customers")], {"dbo.Customers": start})
    c.disconnect()


def test_oracle_live():
    _need("localhost", 1521, "oracledb")
    c = create_connector({"engine": "oracle", "id": "ora", "server": "localhost", "port": 1521, "service_name": "FREEPDB1",
                          "username": "cdc", "password": "cdc", "schema": "CDC"})
    assert isinstance(c, OracleConnector)
    c.connect()
    checks = {ch.name: ch for ch in c.check_prerequisites([("CDC", "CUSTOMERS")])}
    assert checks["archivelog_mode"].ok, checks["archivelog_mode"].detail
    start = c.current_position()
    c._execute("INSERT INTO cdc.customers (customer_id, name) VALUES (1, 'Ann')")
    c._execute("UPDATE cdc.customers SET city = 'Rome' WHERE customer_id = 1")
    c._execute("DELETE FROM cdc.customers WHERE customer_id = 1")
    time.sleep(2)
    events, pos = _collect(c, [("CDC", "CUSTOMERS")], start, timeout=60)
    assert [e.operation for e in events] == [Operation.INSERT, Operation.UPDATE, Operation.DELETE]
    assert events[1].after["CITY"] == "Rome"
    c.disconnect()
