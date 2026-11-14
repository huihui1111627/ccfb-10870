"""Contract model: loading, validation, normalization and hashing.

A contract is a YAML/JSON document describing one upstream table.  Columns carry
type/nullability/enum/range/uniqueness constraints; table-level sections carry
primary keys, freshness and data-quality rules that the daily sampler enforces.

The loader is intentionally strict: anything the platform cannot enforce is an
error rather than silently ignored, because a contract that lies is worse than
no contract at all.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

# Canonical type taxonomy.  `decimal` keeps precision/scale in `precision`.
TYPE_NAMES = {
    "string",
    "int",
    "bigint",
    "decimal",
    "bool",
    "date",
    "timestamp",
    "json",
    "bytes",
}

# Types that the sampler can range-check / enum-check.
ORDERED_TYPES = {"int", "bigint", "decimal", "date", "timestamp"}
NUMERIC_TYPES = {"int", "bigint", "decimal"}
STRINGISH_TYPES = {"string", "json", "bytes"}

REQUIREDITY = {"required", "nullable"}

_SEVERITIES = {"critical", "high", "medium", "low"}

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ContractError(ValueError):
    """Raised when a contract document is structurally or semantically invalid."""

    def __init__(self, message: str, path: str | None = None):
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)


@dataclass
class Column:
    name: str
    type: str
    #: Physical SQL type string, optional. Used by the mock upstreams and by
    #: consumers that need to map the logical contract onto a database.
    sql_type: str | None = None
    required: bool = True
    enum: list[Any] | None = None
    minimum: Any = None
    maximum: Any = None
    unique: bool = False
    precision: int | None = None
    scale: int | None = None
    description: str | None = None
    #: Rename provenance: set by the diff engine when a rename is inferred.
    renamed_from: str | None = None
    #: Physical type is carried for materialization but is deliberately excluded
    #: from the canonical contract hash: contracts describe logical guarantees.
    _include_sql_type: bool = False

    def to_dict(self) -> dict[str, Any]:
        d = {
            "name": self.name,
            "type": self.type,
            "required": self.required,
        }
        if self.sql_type:
            d["sql_type"] = self.sql_type
        if self.enum is not None:
            # Enum order is not semantic: sorting keeps hashes stable when an
            # upstream merely reshuffles the list.
            d["enum"] = sorted(self.enum, key=lambda v: (str(type(v)), repr(v)))
        if self.minimum is not None:
            d["minimum"] = self.minimum
        if self.maximum is not None:
            d["maximum"] = self.maximum
        if self.unique:
            d["unique"] = True
        if self.precision is not None:
            d["precision"] = self.precision
        if self.scale is not None:
            d["scale"] = self.scale
        if self.description:
            d["description"] = self.description
        if self.renamed_from:
            d["renamed_from"] = self.renamed_from
        return d


@dataclass
class Rule:
    """A table-level data quality rule enforced by the sampler."""

    name: str
    kind: str  # not_null | enum | range | unique | expression | row_count | null_ratio
    column: str | None = None
    enum: list[Any] | None = None
    minimum: Any = None
    maximum: Any = None
    #: For `expression`: a whitelisted SQL-ish predicate checked by the sampler.
    expression: str | None = None
    severity: str = "high"
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"name": self.name, "kind": self.kind, "severity": self.severity}
        for k in ("column", "enum", "minimum", "maximum", "expression", "description"):
            v = getattr(self, k)
            if v is not None:
                d[k] = v
        return d


@dataclass
class Freshness:
    #: Maximum age of the data, ISO-8601 duration, e.g. "PT2H", "P1D".
    max_age: str | None = None
    #: Column holding the event/ingest timestamp, if different from contract time.
    column: str | None = None
    severity: str = "high"

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        for k in ("max_age", "column", "severity"):
            v = getattr(self, k)
            if v is not None:
                d[k] = v
        return d


@dataclass
class Contract:
    contract_id: str
    version: int
    upstream_system: str
    table: str
    description: str = ""
    columns: list[Column] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    unique_keys: list[list[str]] = field(default_factory=list)
    freshness: Freshness | None = None
    row_count: dict[str, Any] | None = None
    rules: list[Rule] = field(default_factory=list)
    #: Downstream owners registered as consumers.  Destructive changes need all
    #: of them to sign off before being released.
    consumers: list[str] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    status: str = "draft"  # draft | active | pending_signoff | rejected | superseded
    schema_hash: str | None = None

    @property
    def pk(self) -> list[str]:
        return self.primary_key or []

    def column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def canonical(self) -> dict[str, Any]:
        """Order-independent canonical JSON of the logical contract.

        Physical DDL details (``sql_type``) and constraint ordering are excluded:
        two contracts that promise the same logical guarantees are the same
        contract even if one database spells its types differently.
        """
        return {
            "contract_id": self.contract_id,
            "upstream_system": self.upstream_system,
            "table": self.table,
            "columns": [_logical_column(c) for c in sorted(self.columns, key=lambda c: c.name)],
            "primary_key": list(self.primary_key),
            "unique_keys": [sorted(k) for k in sorted(self.unique_keys, key=lambda k: sorted(k))],
            "freshness": self.freshness.to_dict() if self.freshness else None,
            "row_count": self.row_count,
            "rules": [r.to_dict() for r in sorted(self.rules, key=lambda r: r.name)],
        }

    def compute_hash(self) -> str:
        blob = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        """Full persisted document. Includes physical details the hash ignores."""
        d = {
            "contract_id": self.contract_id,
            "upstream_system": self.upstream_system,
            "table": self.table,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": list(self.primary_key),
            "unique_keys": [list(k) for k in self.unique_keys],
            "freshness": self.freshness.to_dict() if self.freshness else None,
            "row_count": self.row_count,
            "rules": [r.to_dict() for r in self.rules],
            "version": self.version,
            "description": self.description,
            "consumers": list(self.consumers),
            "owners": list(self.owners),
            "status": self.status,
            "schema_hash": self.schema_hash,
        }
        return d


def _logical_column(c: Column) -> dict[str, Any]:
    """Column projection used for hashing: logical guarantees only."""
    d = {
        "name": c.name,
        "type": c.type,
        "required": c.required,
    }
    if c.enum is not None:
        d["enum"] = sorted(c.enum, key=lambda v: (str(type(v)), repr(v)))
    for key in ("minimum", "maximum", "precision", "scale"):
        v = getattr(c, key)
        if v is not None:
            d[key] = v
    if c.unique:
        d["unique"] = True
    return d


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


def _err(path: str, msg: str) -> ContractError:
    return ContractError(msg, path)


def _column_from_dict(name: str, raw: Any, path: str) -> Column:
    if not isinstance(raw, dict):
        raise _err(path, "column definition must be a mapping")
    if not _NAME_RE.match(name):
        raise _err(path, f"invalid column name {name!r}")
    ctype = raw.get("type")
    if ctype not in TYPE_NAMES:
        raise _err(f"{path}.type", f"unknown type {ctype!r}; expected one of {sorted(TYPE_NAMES)}")

    required = True
    if "required" in raw:
        required = bool(raw["required"])
    if "nullable" in raw:
        nullable = bool(raw["nullable"])
        if "required" in raw and nullable == required:
            raise _err(path, "`required` and `nullable` disagree")
        required = not nullable

    enum = raw.get("enum")
    if enum is not None:
        if not isinstance(enum, list) or not enum:
            raise _err(f"{path}.enum", "enum must be a non-empty list")
        if ctype in NUMERIC_TYPES or ctype in {"date", "timestamp"}:
            for v in enum:
                _check_scalar(v, ctype, f"{path}.enum")
        if ctype in {"json", "bytes"}:
            raise _err(f"{path}.enum", f"enum is not meaningful for type {ctype}")

    minimum, maximum = raw.get("minimum"), raw.get("maximum")
    if minimum is not None or maximum is not None:
        if ctype not in ORDERED_TYPES:
            raise _err(path, f"minimum/maximum require an ordered type, got {ctype!r}")
        for label, v in (("minimum", minimum), ("maximum", maximum)):
            if v is not None:
                try:
                    _coerce(v, ctype)
                except ValueError as e:
                    raise _err(f"{path}.{label}", str(e))
        if minimum is not None and maximum is not None:
            lo, hi = _coerce(minimum, ctype), _coerce(maximum, ctype)
            if lo > hi:
                raise _err(path, "minimum is greater than maximum")

    precision, scale = raw.get("precision"), raw.get("scale")
    if ctype == "decimal":
        if precision is not None and (not isinstance(precision, int) or precision <= 0):
            raise _err(f"{path}.precision", "precision must be a positive integer")
        if scale is not None and (not isinstance(scale, int) or scale < 0):
            raise _err(f"{path}.scale", "scale must be a non-negative integer")
        if precision is not None and scale is not None and scale >= precision:
            raise _err(path, "scale must be less than precision")
    elif precision is not None or scale is not None:
        raise _err(path, "precision/scale are only valid for decimal")

    unique = bool(raw.get("unique", False))
    if unique and ctype in {"json", "bytes"}:
        raise _err(path, f"unique is not meaningful for type {ctype}")

    return Column(
        name=name,
        type=ctype,
        sql_type=raw.get("sql_type"),
        required=required,
        enum=enum,
        minimum=minimum,
        maximum=maximum,
        unique=unique,
        precision=precision,
        scale=scale,
        description=raw.get("description"),
        renamed_from=raw.get("renamed_from"),
    )


def _check_scalar(value: Any, ctype: str, path: str) -> None:
    try:
        _coerce(value, ctype)
    except ValueError as e:
        raise _err(path, str(e))


def _coerce(value: Any, ctype: str) -> Any:
    """Coerce/validate a literal against the logical type; raise ValueError."""
    if ctype == "int" or ctype == "bigint":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"expected integer, got {value!r}")
        return value
    if ctype == "decimal":
        if isinstance(value, bool):
            raise ValueError(f"expected number, got {value!r}")
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError(f"expected number, got {value!r}") from None
    if ctype == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"expected boolean, got {value!r}")
        return value
    if ctype == "date":
        from datetime import date

        if isinstance(value, date) and not hasattr(value, "hour"):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            raise ValueError(f"expected ISO date (YYYY-MM-DD), got {value!r}") from None
    if ctype == "timestamp":
        from datetime import datetime

        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            raise ValueError(f"expected ISO timestamp, got {value!r}") from None
    if ctype == "string":
        if not isinstance(value, str):
            raise ValueError(f"expected string, got {value!r}")
        return value
    return value


def _rule_from_dict(raw: Any, idx: int, column_names: set[str]) -> Rule:
    path = f"rules[{idx}]"
    if not isinstance(raw, dict):
        raise _err(path, "rule must be a mapping")
    name = raw.get("name")
    kind = raw.get("kind")
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise _err(f"{path}.name", "rule name must match [A-Za-z_][A-Za-z0-9_]*")
    valid_kinds = {"not_null", "enum", "range", "unique", "expression", "row_count", "null_ratio"}
    if kind not in valid_kinds:
        raise _err(f"{path}.kind", f"unknown rule kind {kind!r}; expected one of {sorted(valid_kinds)}")
    severity = raw.get("severity", "high")
    if severity not in _SEVERITIES:
        raise _err(f"{path}.severity", f"unknown severity {severity!r}")

    col = raw.get("column")
    if kind != "row_count" and kind != "expression":
        if not col:
            raise _err(f"{path}.column", f"rule kind {kind} requires a column")
        if col not in column_names:
            raise _err(f"{path}.column", f"unknown column {col!r}")
    if col is not None and col not in column_names:
        raise _err(f"{path}.column", f"unknown column {col!r}")

    enum = raw.get("enum")
    minimum, maximum = raw.get("minimum"), raw.get("maximum")
    expression = raw.get("expression")
    if kind == "enum" and (not isinstance(enum, list) or not enum):
        raise _err(f"{path}.enum", "enum rule requires a non-empty enum list")
    if kind == "range" and minimum is None and maximum is None:
        raise _err(path, "range rule requires minimum and/or maximum")
    if kind == "expression" and not expression:
        raise _err(f"{path}.expression", "expression rule requires an expression")
    if kind == "row_count" and minimum is None and maximum is None:
        raise _err(path, "row_count rule requires minimum and/or maximum")
    if kind == "null_ratio" and (minimum is None and maximum is None):
        raise _err(path, "null_ratio rule requires maximum (and optionally minimum)")

    return Rule(
        name=name,
        kind=kind,
        column=col,
        enum=enum,
        minimum=minimum,
        maximum=maximum,
        expression=expression,
        severity=severity,
        description=raw.get("description"),
    )


def parse_contract(doc: dict[str, Any], *, version: int | None = None) -> Contract:
    """Parse a raw contract document into a Contract, validating everything."""
    if not isinstance(doc, dict):
        raise _err("$", "contract document must be a mapping")

    contract_id = doc.get("contract_id")
    if not isinstance(contract_id, str) or not _NAME_RE.match(contract_id.replace(".", "_")):
        raise _err("contract_id", "contract_id must be a dotted slug like orders.order_header")
    upstream = doc.get("upstream_system")
    if not isinstance(upstream, str) or not upstream.strip():
        raise _err("upstream_system", "upstream_system is required")
    table = doc.get("table")
    if not isinstance(table, str) or not table.strip():
        raise _err("table", "table is required")

    raw_columns = doc.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise _err("columns", "columns must be a non-empty list")
    columns: list[Column] = []
    seen: set[str] = set()
    for i, raw in enumerate(raw_columns):
        if not isinstance(raw, dict) or "name" not in raw:
            raise _err(f"columns[{i}]", "each column needs a name")
        name = raw["name"]
        if name in seen:
            raise _err(f"columns[{i}]", f"duplicate column {name!r}")
        seen.add(name)
        columns.append(_column_from_dict(name, raw, f"columns[{i}]"))
    if len(columns) > 400:
        raise _err("columns", "too many columns (limit 400)")

    column_names = set(seen)
    primary_key = doc.get("primary_key") or []
    if not isinstance(primary_key, list):
        raise _err("primary_key", "primary_key must be a list of column names")
    for c in primary_key:
        if c not in column_names:
            raise _err("primary_key", f"unknown column {c!r}")
    if len(set(primary_key)) != len(primary_key):
        raise _err("primary_key", "duplicate column in primary_key")

    unique_keys = doc.get("unique_keys") or []
    if not isinstance(unique_keys, list):
        raise _err("unique_keys", "unique_keys must be a list of column-name lists")
    for i, key in enumerate(unique_keys):
        if not isinstance(key, list) or not key:
            raise _err(f"unique_keys[{i}]", "unique key must be a non-empty list")
        for c in key:
            if c not in column_names:
                raise _err(f"unique_keys[{i}]", f"unknown column {c!r}")

    freshness = None
    raw_fresh = doc.get("freshness")
    if raw_fresh is not None:
        if not isinstance(raw_fresh, dict):
            raise _err("freshness", "freshness must be a mapping")
        fcol = raw_fresh.get("column")
        if fcol is not None and fcol not in column_names:
            raise _err("freshness.column", f"unknown column {fcol!r}")
        fsev = raw_fresh.get("severity", "high")
        if fsev not in _SEVERITIES:
            raise _err("freshness.severity", f"unknown severity {fsev!r}")
        freshness = Freshness(
            max_age=raw_fresh.get("max_age"),
            column=fcol,
            severity=fsev,
        )

    rules = []
    raw_rules = doc.get("rules") or []
    if not isinstance(raw_rules, list):
        raise _err("rules", "rules must be a list")
    rule_names: set[str] = set()
    for i, raw in enumerate(raw_rules):
        r = _rule_from_dict(raw, i, column_names)
        if r.name in rule_names:
            raise _err(f"rules[{i}].name", f"duplicate rule name {r.name!r}")
        rule_names.add(r.name)
        rules.append(r)

    row_count = doc.get("row_count")
    if row_count is not None and not isinstance(row_count, dict):
        raise _err("row_count", "row_count must be a mapping")

    consumers = doc.get("consumers") or []
    if not isinstance(consumers, list) or not all(isinstance(x, str) for x in consumers):
        raise _err("consumers", "consumers must be a list of strings")
    owners = doc.get("owners") or []
    if not isinstance(owners, list) or not all(isinstance(x, str) for x in owners):
        raise _err("owners", "owners must be a list of strings")

    c = Contract(
        contract_id=contract_id,
        version=version if version is not None else int(doc.get("version") or 1),
        upstream_system=upstream,
        table=table,
        description=doc.get("description") or "",
        columns=columns,
        primary_key=list(primary_key),
        unique_keys=[list(k) for k in unique_keys],
        freshness=freshness,
        row_count=row_count,
        rules=rules,
        consumers=sorted(set(consumers)),
        owners=sorted(set(owners)),
        status=doc.get("status") or "draft",
    )
    c.schema_hash = c.compute_hash()
    return c


def load_contract_text(text: str, *, version: int | None = None) -> Contract:
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ContractError(f"invalid YAML: {e}") from e
    return parse_contract(doc, version=version)


def load_contract_file(path: str, *, version: int | None = None) -> Contract:
    with open(path, "r", encoding="utf-8") as fh:
        return load_contract_text(fh.read(), version=version)


def dump_contract_flow(c: Contract) -> dict[str, Any]:
    """Contract in the shape accepted by parse_contract (round-trippable)."""
    return {
        "contract_id": c.contract_id,
        "upstream_system": c.upstream_system,
        "table": c.table,
        "description": c.description,
        "columns": [col.to_dict() for col in c.columns],
        "primary_key": c.primary_key,
        "unique_keys": c.unique_keys,
        "freshness": c.freshness.to_dict() if c.freshness else None,
        "row_count": c.row_count,
        "rules": [r.to_dict() for r in c.rules],
        "consumers": c.consumers,
        "owners": c.owners,
        "status": c.status,
    }
