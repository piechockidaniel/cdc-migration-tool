"""Parser for Oracle LogMiner ``SQL_REDO`` statements.

LogMiner reconstructs DML as SQL text such as::

    insert into "HR"."EMP"("ID","NAME","HIRED") values ('1','Ann',TO_DATE('2024-01-02 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));
    update "HR"."EMP" set "NAME" = 'Bob' where "ID" = '1' and "NAME" = 'Ann';
    delete from "HR"."EMP" where "ID" = '1' and "NAME" = 'Bob' and "HIRED" IS NULL;

This module turns them back into ``(operation, owner, table, set_values, where_values)``
without executing anything. It handles quoted identifiers, ``''`` escapes,
``NULL``/``IS NULL``, numbers, ``TO_DATE``/``TO_TIMESTAMP``/``TO_TIMESTAMP_TZ``,
``HEXTORAW``, ``UNISTR``, ``EMPTY_CLOB()``/``EMPTY_BLOB()`` and trailing
``ROWID`` predicates.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class RedoParseError(ValueError):
    pass


@dataclass
class ParsedRedo:
    operation: str                     # INSERT | UPDATE | DELETE
    owner: str
    table: str
    values: Dict[str, Any] = field(default_factory=dict)   # after image (insert values / update set)
    where: Dict[str, Any] = field(default_factory=dict)    # before image (update/delete where)


_FUNC_VALUE = re.compile(r"^(?P<fn>TO_DATE|TO_TIMESTAMP|TO_TIMESTAMP_TZ|HEXTORAW|UNISTR|TO_NUMBER)\s*\(", re.I)


class _Tokenizer:
    """Minimal SQL tokenizer sufficient for LogMiner output."""

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.n = len(text)

    def skip_ws(self) -> None:
        while self.pos < self.n and self.text[self.pos] in " \t\r\n":
            self.pos += 1

    def peek(self, s: str) -> bool:
        self.skip_ws()
        return self.text[self.pos: self.pos + len(s)].lower() == s.lower()

    def expect(self, s: str) -> None:
        if not self.peek(s):
            raise RedoParseError(f"expected {s!r} at {self.pos}: {self.text[self.pos:self.pos+40]!r}")
        self.pos += len(s)

    def eof(self) -> bool:
        self.skip_ws()
        return self.pos >= self.n or self.text[self.pos] == ";"

    def identifier(self) -> str:
        self.skip_ws()
        if self.text[self.pos] == '"':
            end = self.pos + 1
            while True:
                end = self.text.index('"', end)
                if end + 1 < self.n and self.text[end + 1] == '"':
                    end += 2
                    continue
                break
            ident = self.text[self.pos + 1: end].replace('""', '"')
            self.pos = end + 1
            return ident
        m = re.match(r"[A-Za-z_][A-Za-z0-9_$#]*", self.text[self.pos:])
        if not m:
            raise RedoParseError(f"identifier expected at {self.pos}")
        self.pos += m.end()
        return m.group(0)

    def qualified_name(self) -> Tuple[str, str]:
        first = self.identifier()
        if self.peek("."):
            self.pos += 1
            return first, self.identifier()
        return "", first

    def string(self) -> str:
        self.skip_ws()
        assert self.text[self.pos] == "'"
        out = []
        i = self.pos + 1
        while i < self.n:
            ch = self.text[i]
            if ch == "'":
                if i + 1 < self.n and self.text[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                self.pos = i + 1
                return "".join(out)
            out.append(ch)
            i += 1
        raise RedoParseError("unterminated string")

    def balanced_call(self) -> str:
        """Return text of ``FN(...)`` including nested parentheses and strings."""
        self.skip_ws()
        start = self.pos
        depth = 0
        i = self.pos
        while i < self.n:
            ch = self.text[i]
            if ch == "'":
                self.pos = i
                self.string()
                i = self.pos
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    self.pos = i + 1
                    return self.text[start: self.pos]
            i += 1
        raise RedoParseError("unbalanced parentheses")

    def value(self) -> Any:
        self.skip_ws()
        ch = self.text[self.pos]
        if ch == "'":
            return self.string()
        rest = self.text[self.pos:]
        if re.match(r"NULL\b", rest, re.I):
            self.pos += 4
            return None
        if re.match(r"EMPTY_[CB]LOB\s*\(\s*\)", rest, re.I):
            m = re.match(r"EMPTY_[CB]LOB\s*\(\s*\)", rest, re.I)
            self.pos += m.end()
            return ""
        m = _FUNC_VALUE.match(rest)
        if m:
            call = self.balanced_call()
            return _convert_function(m.group("fn").upper(), call)
        m = re.match(r"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?", rest)
        if m:
            self.pos += m.end()
            txt = m.group(0)
            try:
                return int(txt) if re.fullmatch(r"[-+]?\d+", txt) else float(txt)
            except ValueError:  # pragma: no cover
                return txt
        raise RedoParseError(f"cannot parse value at {self.pos}: {rest[:40]!r}")


def _convert_function(fn: str, call: str) -> Any:
    inner = call[call.index("(") + 1: -1].strip()
    t = _Tokenizer(inner)
    if fn in ("TO_DATE", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ"):
        literal = t.string()
        fmt = None
        if t.peek(","):
            t.pos += 1
            fmt = t.string()
        return _parse_oracle_datetime(literal, fmt)
    if fn == "HEXTORAW":
        return bytes.fromhex(t.string())
    if fn == "UNISTR":
        raw = t.string()
        return re.sub(r"\\([0-9A-Fa-f]{4})", lambda m: chr(int(m.group(1), 16)), raw)
    if fn == "TO_NUMBER":
        txt = t.string()
        try:
            return int(txt) if re.fullmatch(r"[-+]?\d+", txt) else float(txt)
        except ValueError:
            return txt
    return call  # pragma: no cover


_ORACLE_FMT = {
    "YYYY": "%Y", "RRRR": "%Y", "MM": "%m", "DD": "%d", "HH24": "%H", "HH12": "%I", "HH": "%I",
    "MI": "%M", "SS": "%S", "AM": "%p", "PM": "%p", "TZH": "%z", "TZM": "",
}


def _parse_oracle_datetime(literal: str, fmt: Optional[str]) -> Any:
    if not fmt:
        for candidate in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(literal, candidate)
            except ValueError:
                continue
        return literal
    py_fmt = fmt.upper()
    # fractional seconds FF / FF3 / FF6 / FF9
    py_fmt = re.sub(r"FF\d?", "%f", py_fmt)
    for k in sorted(_ORACLE_FMT, key=len, reverse=True):
        py_fmt = py_fmt.replace(k, _ORACLE_FMT[k])
    lit = literal
    if "%f" in py_fmt:
        # python wants max 6 digits in %f
        lit = re.sub(r"(\.\d{6})\d+", r"\1", lit)
    # drop timezone fragments we cannot model reliably
    py_fmt = py_fmt.replace("%z:", "%z").replace(":%z", "%z")
    try:
        return datetime.strptime(lit, py_fmt)
    except ValueError:
        try:
            return datetime.strptime(lit.split(" ")[0], "%Y-%m-%d")
        except ValueError:
            return literal


def _parse_assignments(t: _Tokenizer, stop_words: Tuple[str, ...]) -> Dict[str, Any]:
    """``"A" = 1 and "B" IS NULL`` (where) or ``"A" = 1, "B" = 2`` (set)."""
    out: Dict[str, Any] = {}
    while not t.eof():
        for w in stop_words:
            if t.peek(w + " "):
                return out
        name = t.identifier()
        if name.upper() == "ROWID":
            t.expect("=")
            t.value()
        else:
            t.skip_ws()
            if t.peek("IS NULL"):
                t.pos += len("IS NULL")
                out[name] = None
            else:
                t.expect("=")
                out[name] = t.value()
        t.skip_ws()
        if t.peek(","):
            t.pos += 1
        elif t.peek("and "):
            t.pos += 3
        else:
            break
    return out


def parse_redo(sql: str) -> ParsedRedo:
    text = sql.strip()
    if text.endswith(";"):
        text = text[:-1]
    t = _Tokenizer(text)
    if t.peek("insert into"):
        t.pos += len("insert into")
        owner, table = t.qualified_name()
        t.expect("(")
        cols: List[str] = []
        while True:
            cols.append(t.identifier())
            if t.peek(","):
                t.pos += 1
                continue
            t.expect(")")
            break
        t.expect("values")
        t.expect("(")
        vals: List[Any] = []
        while True:
            vals.append(t.value())
            if t.peek(","):
                t.pos += 1
                continue
            t.expect(")")
            break
        if len(cols) != len(vals):
            raise RedoParseError("column/value count mismatch")
        return ParsedRedo("INSERT", owner, table, dict(zip(cols, vals)), {})
    if t.peek("update"):
        t.pos += len("update")
        owner, table = t.qualified_name()
        t.expect("set")
        values = _parse_assignments(t, ("where",))
        where: Dict[str, Any] = {}
        if t.peek("where"):
            t.pos += len("where")
            where = _parse_assignments(t, ())
        return ParsedRedo("UPDATE", owner, table, values, where)
    if t.peek("delete from"):
        t.pos += len("delete from")
        owner, table = t.qualified_name()
        where = {}
        if t.peek("where"):
            t.pos += len("where")
            where = _parse_assignments(t, ())
        return ParsedRedo("DELETE", owner, table, {}, where)
    raise RedoParseError(f"unsupported redo statement: {text[:60]!r}")
