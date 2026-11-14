"""Sampling validation of real upstream data against an active contract.

The validator does two jobs:

* **Structural conformance** — compare the physical upstream schema with the
  contract (missing table/columns, unexpected extra columns).
* **Value conformance** — evaluate each declared constraint per sampled row and
  emit violations that carry the offending row's primary key and a snapshot, so
  the console can drill down to the exact row.

Sampling is deterministic: rows are ordered by MD5 of their primary key and the
first N are taken.  Re-running the same validation produces the same sample,
which makes violation counts and evaluations reproducible.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .contract import Column, Contract, Rule
from .expr import ExpressionError, compile_expression, evaluate_expression


@dataclass
class Violation:
    rule_name: str
    rule_kind: str
    severity: str
    column: str | None
    detail: str
    row_pk: dict[str, Any] | None = None
    row_data: dict[str, Any] | None = None

    def key(self, contract_id: str, version: int) -> str:
        """Stable identity of the problem (not the run) for alert deduplication."""
        pk = json.dumps(self.row_pk, sort_keys=True, default=str) if self.row_pk else ""
        raw = f"{contract_id}|{version}|{self.rule_name}|{self.column or ''}|{pk}|{self.detail}"
        return hashlib.sha256(raw.encode()).hexdigest()[:20]


@dataclass
class ValidationResult:
    contract_id: str
    version: int
    rows_checked: int = 0
    rows_clean: int = 0
    rows_total: int | None = None
    sampled: bool = False
    violations: list[Violation] = field(default_factory=list)
    by_rule: dict[str, int] = field(default_factory=dict)
    freshness_checked: bool = False
    structural_errors: list[str] = field(default_factory=list)

    @property
    def violation_count(self) -> int:
        return len(self.violations)

    @property
    def clean_ratio(self) -> float:
        if not self.rows_checked:
            return 1.0
        return self.rows_clean / self.rows_checked


# --------------------------------------------------------------------------
# Type validation helpers
# --------------------------------------------------------------------------

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def value_valid_for_type(value: Any, col: Column) -> tuple[bool, str]:
    """Check a raw Python value against the logical type. Returns (ok, detail)."""
    t = col.type
    if t in {"int", "bigint"}:
        if isinstance(value, bool) or not isinstance(value, int):
            if isinstance(value, float) and value.is_integer():
                return True, ""  # SQLite may widen INTEGER to REAL
            return False, f"expected {t}, got {type(value).__name__} {value!r}"
        return True, ""
    if t == "decimal":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False, f"expected decimal, got {type(value).__name__} {value!r}"
        if col.scale is not None:
            # Excess decimal places are lossy: the stored value is not the value
            # the contract promises.  Compare on a string rendering to avoid
            # binary-float noise (0.1 + 0.2 style).
            rendered = f"{float(value):.{col.scale}f}"
            if abs(float(rendered) - float(value)) > 1e-9:
                return False, f"value {value} has more than {col.scale} decimal places (scale overflow)"
        if col.precision is not None and col.scale is not None:
            int_digits = len(str(int(abs(float(value)))))
            if int_digits > col.precision - col.scale:
                return False, f"value {value} exceeds precision {col.precision},{col.scale}"
        return True, ""
    if t == "bool":
        # SQLite has no BOOLEAN type; drivers round-trip it as 0/1.
        if isinstance(value, bool):
            return True, ""
        if isinstance(value, int) and value in (0, 1):
            return True, ""
        return False, f"expected bool, got {type(value).__name__} {value!r}"
    if t == "date":
        if isinstance(value, datetime):
            return False, f"expected date, got timestamp {value!r}"
        if isinstance(value, date):
            return True, ""
        if isinstance(value, str):
            # Surrounding whitespace is a serialization artifact warehouses
            # routinely ship; it is not a different value.  Report the raw
            # value in the drilldown rather than flagging it as a type error.
            text = value.strip()
            if _DATE_RE.match(text):
                try:
                    date.fromisoformat(text)
                    return True, ""
                except ValueError:
                    pass
            return False, f"expected date (YYYY-MM-DD), got {value!r}"
        return False, f"expected date (YYYY-MM-DD), got {value!r}"
    if t == "timestamp":
        if isinstance(value, datetime):
            return True, ""
        if isinstance(value, str):
            try:
                datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
                return True, ""
            except ValueError:
                return False, f"invalid timestamp {value!r}"
        return False, f"expected timestamp, got {type(value).__name__} {value!r}"
    if t == "string":
        if not isinstance(value, str):
            return False, f"expected string, got {type(value).__name__} {value!r}"
        return True, ""
    if t == "json":
        if isinstance(value, (dict, list)):
            return True, ""
        if isinstance(value, str):
            try:
                json.loads(value)
                return True, ""
            except (TypeError, ValueError):
                return False, "invalid JSON text"
        return False, f"expected JSON, got {type(value).__name__}"
    if t == "bytes":
        return isinstance(value, (bytes, bytearray)), "expected bytes"
    return True, ""


def _ordered(value: Any) -> Any:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        for parser in (datetime.fromisoformat, lambda s: datetime.fromisoformat(s + "T00:00:00+00:00")):
            try:
                return parser(text)
            except ValueError:
                continue
        try:
            return float(text)
        except ValueError:
            return value
    return value


def _compare(a: Any, b: Any) -> int | None:
    """Compare two observed/declared values; returns -1/0/1, or None if incomparable."""
    if isinstance(a, (date, datetime)) or isinstance(b, (date, datetime)):
        a, b = _ordered(a), _ordered(b)
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    if isinstance(a, (int, float)) and not isinstance(a, bool) and isinstance(b, (int, float)) and not isinstance(b, bool):
        return (a > b) - (a < b)
    try:
        return (a > b) - (a < b)
    except TypeError:
        return None


def _bound(value: Any, col: Column) -> Any:
    if col.type in {"date", "timestamp"}:
        return _ordered(value)
    return value


def _norm_value(value: Any, col: Column) -> Any:
    """Normalize for enum comparison across date/datetime/string representations."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
    if col.type == "date" and isinstance(value, str):
        try:
            return date.fromisoformat(value).isoformat()
        except ValueError:
            return value
    if col.type == "timestamp":
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None).isoformat()
            except ValueError:
                return value
        if isinstance(value, datetime):
            return value.replace(tzinfo=None).isoformat()
    if col.type == "decimal" and isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 8)
    return value


# --------------------------------------------------------------------------
# Row access
# --------------------------------------------------------------------------


def row_primary_key(contract: Contract, row: dict[str, Any], idx: int) -> dict[str, Any]:
    if contract.primary_key and all(k in row for k in contract.primary_key):
        return {k: row.get(k) for k in contract.primary_key}
    for key in contract.unique_keys:
        if all(k in row for k in key):
            return {k: row.get(k) for k in key}
    return {"_row_index": idx}


def _row_digest(row: dict[str, Any]) -> str:
    return hashlib.md5(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


def _sample_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(rows) <= limit:
        return rows
    ordered = sorted(rows, key=_row_digest)
    return ordered[:limit]


def _read_sqlite(db_path: str, table: str) -> tuple[list[str], list[dict[str, Any]]]:
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(f"SELECT * FROM {_quote_ident(table)}")
        cols = [d[0] for d in cur.description]
        rows = [dict(r) for r in cur.fetchall()]
        return cols, rows
    finally:
        conn.close()


def _quote_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"unsafe identifier {name!r}")
    return f'"{name}"'


# --------------------------------------------------------------------------
# Conformance checks
# --------------------------------------------------------------------------


def structural_issues(contract: Contract, actual_columns: Iterable[str]) -> list[str]:
    actual = set(actual_columns)
    expected = set(contract.column_names)
    issues = []
    for name in sorted(expected - actual):
        issues.append(f"contract column {name!r} is missing from the upstream table")
    for name in sorted(actual - expected):
        issues.append(f"upstream column {name!r} is not declared in the contract")
    return issues


def _snapshot(row: dict[str, Any], limit: int = 40) -> dict[str, Any]:
    out = {}
    for k, v in list(row.items())[:limit]:
        if isinstance(v, (bytes, bytearray)):
            out[k] = f"<{len(v)} bytes>"
        else:
            out[k] = v
    return out


def validate_rows(
    contract: Contract,
    rows: list[dict[str, Any]],
    *,
    actual_columns: list[str] | None = None,
    now: datetime | None = None,
    max_violations_per_rule: int = 50,
) -> ValidationResult:
    now = now or datetime.now(timezone.utc)
    res = ValidationResult(contract_id=contract.contract_id, version=contract.version, rows_total=len(rows))

    if actual_columns is not None:
        res.structural_errors = structural_issues(contract, actual_columns)

    cols = {c.name: c for c in contract.columns}
    per_rule: dict[str, int] = {}

    def add(rule: str, kind: str, sev: str, column: str | None, detail: str, row: dict[str, Any], idx: int) -> None:
        n = per_rule.get(rule, 0)
        per_rule[rule] = n + 1
        if n >= max_violations_per_rule:
            return
        res.violations.append(
            Violation(
                rule_name=rule,
                rule_kind=kind,
                severity=sev,
                column=column,
                detail=detail,
                row_pk=row_primary_key(contract, row, idx),
                row_data=_snapshot(row),
            )
        )

    seen_unique: dict[str, dict[str, Any]] = {}
    pk_seen: dict[tuple, int] = {}
    for idx, row in enumerate(rows):
        row_ok = True
        for col in contract.columns:
            if col.name not in row:
                continue
            value = row[col.name]
            rule_name = f"column.{col.name}"
            if value is None:
                if col.required:
                    add(f"{rule_name}.not_null", "not_null", "high", col.name,
                        f"NULL in NOT NULL column {col.name}", row, idx)
                    row_ok = False
                continue
            ok, detail = value_valid_for_type(value, col)
            if not ok:
                add(f"{rule_name}.type", "type", "critical", col.name,
                    f"{col.name} violates type {col.type}: {detail}", row, idx)
                row_ok = False
                continue
            if col.enum is not None:
                allowed = {_norm_value(v, col) for v in col.enum}
                observed = _norm_value(value, col)
                if isinstance(observed, str):
                    observed = observed.strip()
                if observed not in allowed:
                    add(f"{rule_name}.enum", "enum", "high", col.name,
                        f"{col.name}={value!r} not in enum {col.enum}", row, idx)
                    row_ok = False
            if col.minimum is not None:
                c = _compare(value, _bound(col.minimum, col))
                if c is not None and c < 0:
                    add(f"{rule_name}.min", "range", "medium", col.name,
                        f"{col.name}={value!r} below minimum {col.minimum}", row, idx)
                    row_ok = False
            if col.maximum is not None:
                c = _compare(value, _bound(col.maximum, col))
                if c is not None and c > 0:
                    add(f"{rule_name}.max", "range", "medium", col.name,
                        f"{col.name}={value!r} above maximum {col.maximum}", row, idx)
                    row_ok = False

        if contract.primary_key and all(k in row for k in contract.primary_key):
            pk = tuple(json.dumps(row[k], default=str) for k in contract.primary_key)
            if pk in pk_seen:
                add("table.primary_key", "unique", "critical", None,
                    f"duplicate primary key {dict(zip(contract.primary_key, pk))}", row, idx)
                row_ok = False
            else:
                pk_seen[pk] = idx

        for col in contract.columns:
            if not col.unique or col.name not in row:
                continue
            v = row[col.name]
            if v is None:
                continue
            nv = _norm_value(v, col)
            if nv in seen_unique.setdefault(col.name, {}):
                add(f"column.{col.name}.unique", "unique", "high", col.name,
                    f"duplicate value {v!r} in unique column {col.name}", row, idx)
                row_ok = False
            else:
                seen_unique[col.name][nv] = idx

        for key in contract.unique_keys:
            if not all(k in row for k in key):
                continue
            kv = tuple(json.dumps(row[k], sort_keys=True, default=str) for k in key)
            store_key = "uk:" + "|".join(sorted(key))
            bucket = seen_unique.setdefault(store_key, {})
            if kv in bucket:
                add(f"table.unique_key.{'+'.join(sorted(key))}", "unique", "high", None,
                    f"duplicate unique key {key}={ {k: row[k] for k in key} }", row, idx)
                row_ok = False
            else:
                bucket[kv] = idx

        for rule in contract.rules:
            if rule.kind in {"not_null", "enum", "range"}:
                ok, detail = _eval_column_rule(rule, row)
                if not ok:
                    add(rule.name, rule.kind, rule.severity, rule.column, detail, row, idx)
                    row_ok = False
            elif rule.kind == "expression":
                try:
                    tree = compile_expression(rule.expression or "", contract.column_names)
                    value = evaluate_expression(tree, row)
                except ExpressionError as e:
                    add(rule.name, "expression", rule.severity, rule.column,
                        f"invalid expression {rule.expression!r}: {e}", row, idx)
                    row_ok = False
                    continue
                if value is False:
                    add(rule.name, "expression", rule.severity, rule.column,
                        f"expression failed: {rule.expression}", row, idx)
                    row_ok = False

        if row_ok:
            res.rows_clean += 1
        res.rows_checked += 1

    # table-level rules
    for rule in contract.rules:
        if rule.kind == "row_count":
            n = len(rows)
            bad = None
            if rule.minimum is not None and n < float(rule.minimum):
                bad = f"row count {n} below minimum {rule.minimum}"
            if rule.maximum is not None and n > float(rule.maximum):
                bad = f"row count {n} above maximum {rule.maximum}"
            if bad:
                res.violations.append(Violation(rule.name, "row_count", rule.severity, None, bad))
        elif rule.kind == "null_ratio":
            if not rule.column:
                continue
            present = [r for r in rows if rule.column in r]
            nulls = sum(1 for r in present if r.get(rule.column) is None)
            ratio = (nulls / len(present)) if present else 0.0
            bad = None
            if rule.maximum is not None and ratio > float(rule.maximum):
                bad = f"null ratio {ratio:.3f} above maximum {rule.maximum}"
            if rule.minimum is not None and ratio < float(rule.minimum):
                bad = f"null ratio {ratio:.3f} below minimum {rule.minimum}"
            if bad:
                res.violations.append(
                    Violation(rule.name, "null_ratio", rule.severity, rule.column, bad)
                )

    # freshness
    if contract.freshness and contract.freshness.max_age:
        res.freshness_checked = True
        fcol = contract.freshness.column
        max_age = parse_duration(contract.freshness.max_age)
        if max_age is None:
            res.violations.append(
                Violation("freshness", "freshness", contract.freshness.severity or "high", fcol,
                          f"invalid max_age {contract.freshness.max_age!r}")
            )
        elif rows:
            values = []
            for r in rows:
                v = r.get(fcol) if fcol else _latest_any_timestamp(r)
                if v is None:
                    continue
                try:
                    values.append(_ordered(v))
                except Exception:
                    continue
            if values:
                parsed = [v for v in values if isinstance(v, (datetime, date))]
                if parsed:
                    latest = max(
                        _ordered(v) if isinstance(v, date) and not isinstance(v, datetime) else v
                        for v in parsed
                    )
                    if latest.tzinfo is None:
                        latest = latest.replace(tzinfo=timezone.utc)
                    age = now - latest
                    if age > max_age:
                        res.violations.append(
                            Violation("freshness", "freshness", contract.freshness.severity or "high", fcol,
                                      f"latest data is {_fmt_age(age)} old, exceeds {contract.freshness.max_age}")
                        )
                else:
                    res.violations.append(
                        Violation("freshness", "freshness", contract.freshness.severity or "high", fcol,
                                  "no parseable timestamp found for freshness check")
                    )

    if res.structural_errors:
        for issue in res.structural_errors:
            res.violations.append(Violation("schema.conformance", "schema", "critical", None, issue))

    for v in res.violations:
        res.by_rule[v.rule_name] = res.by_rule.get(v.rule_name, 0) + 1
    # by_rule must reflect true counts even when sample storage is capped
    for k, n in per_rule.items():
        res.by_rule[k] = n
    return res


def _eval_column_rule(rule: Rule, row: dict[str, Any]) -> tuple[bool, str]:
    col = rule.column
    value = row.get(col) if col else None
    if rule.kind == "not_null":
        if value is None:
            return False, f"NULL in column {col} (rule {rule.name})"
        return True, ""
    if value is None:
        return True, ""
    if rule.kind == "enum":
        if value not in (rule.enum or []):
            return False, f"{col}={value!r} not in rule enum {rule.enum}"
        return True, ""
    if rule.kind == "range":
        if rule.minimum is not None:
            c = _compare(value, rule.minimum)
            if c is not None and c < 0:
                return False, f"{col}={value!r} below rule minimum {rule.minimum}"
        if rule.maximum is not None:
            c = _compare(value, rule.maximum)
            if c is not None and c > 0:
                return False, f"{col}={value!r} above rule maximum {rule.maximum}"
    if rule.kind == "unique":
        return True, ""  # handled via seen_unique below
    return True, ""


def _latest_any_timestamp(row: dict[str, Any]) -> Any:
    best = None
    for v in row.values():
        if isinstance(v, datetime):
            if best is None or v > best:
                best = v
        elif isinstance(v, str):
            try:
                d = datetime.fromisoformat(v.replace("Z", "+00:00"))
                if best is None or d > best:
                    best = d
            except ValueError:
                continue
    return best


_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+(?:\.\d+)?)D)?(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def parse_duration(text: str) -> timedelta | None:
    m = _DURATION_RE.match(text.strip().upper())
    if not m:
        return None
    parts = {k: float(v) for k, v in m.groupdict().items() if v is not None}
    if not parts:
        return None
    return timedelta(**parts)


def _fmt_age(age: timedelta) -> str:
    secs = int(age.total_seconds())
    if secs >= 86400:
        return f"{secs // 86400}d{(secs % 86400) // 3600}h"
    if secs >= 3600:
        return f"{secs // 3600}h{(secs % 3600) // 60}m"
    return f"{secs}s"


# --------------------------------------------------------------------------
# Submission-time sample gate
# --------------------------------------------------------------------------


def validate_submission_sample(contract: Contract, sample: list[dict[str, Any]]) -> list[str]:
    """Validate the sample attached to a submission. Returns blocking errors.

    The sample must cover every contract column and may not contain undeclared
    columns; each row is then checked against the contract's constraints.  This
    is intentionally stricter than the daily sampler: a submission whose own
    example data does not satisfy its own contract is rejected outright.
    """
    errors: list[str] = []
    if not sample:
        return ["submission sample is empty"]
    if len(sample) > 10000:
        return ["submission sample exceeds 10000 rows"]

    declared = set(contract.column_names)
    for i, row in enumerate(sample):
        if not isinstance(row, dict):
            errors.append(f"sample row {i}: not an object")
            continue
        extra = set(row) - declared
        if extra:
            errors.append(f"sample row {i}: undeclared columns {sorted(extra)}")
            break
        missing = declared - set(row)
        if missing:
            errors.append(f"sample row {i}: missing columns {sorted(missing)}")
            break

    if errors:
        return errors

    res = validate_rows(contract, sample, actual_columns=None, max_violations_per_rule=1000)
    for v in res.violations[:50]:
        loc = f" (row {v.row_pk})" if v.row_pk else ""
        errors.append(f"sample violates contract: [{v.rule_name}] {v.detail}{loc}")
    if len(res.violations) > 50:
        errors.append(f"... and {len(res.violations) - 50} more sample violations")
    return errors


# --------------------------------------------------------------------------
# Run against a registered upstream table
# --------------------------------------------------------------------------


@dataclass
class UpstreamData:
    columns: list[str]
    rows: list[dict[str, Any]]
    sampled: bool
    total: int


def fetch_upstream(db_path: str, table: str, sample_limit: int) -> UpstreamData:
    cols, rows = _read_sqlite(db_path, table)
    total = len(rows)
    sampled_rows = _sample_rows(rows, sample_limit)
    return UpstreamData(columns=cols, rows=sampled_rows, sampled=len(sampled_rows) < total, total=total)


def run_validation(
    contract: Contract,
    upstream: UpstreamData,
    *,
    now: datetime | None = None,
) -> ValidationResult:
    res = validate_rows(contract, upstream.rows, actual_columns=upstream.columns, now=now)
    res.sampled = upstream.sampled
    res.rows_total = upstream.total
    return res
