"""Change simulator: mutate upstream tables the way real upstreams do, with ground truth.

Every mutation returns the mutated `SimTable` plus a ground-truth label:

* ``breaking`` — should be classified breaking by the diff engine.
* ``compatible`` — should be classified backward compatible.
* ``dirty`` — data-quality injections that should be caught by validation.

The ground truth is deliberately independent of ``dcp.diff``: it is written by
the person who made the change, which is what makes the accuracy measurement
meaningful rather than circular.
"""

from __future__ import annotations

import hashlib
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from dcpsim.upstreams import ColSpec, SimTable

UTC = timezone.utc


class NotApplicable(Exception):
    """Raised by a mutation factory when the table has no suitable column."""


@dataclass
class Mutation:
    kind: str
    label: str  # breaking | compatible
    description: str
    apply: Callable[[SimTable, random.Random], SimTable]
    #: Column pair affected, for scoring rename detection specifically.
    rename_pair: tuple[str, str] | None = None
    #: Columns whose data may need re-validation after the change.
    affected_columns: list[str] = field(default_factory=list)
    #: False when no contract-aware observer could distinguish this change from
    #: a no-op; such cases are reported separately, not scored as misses.
    detectable: bool = True
    undetectable_reason: str | None = None

    def __call__(self, table: SimTable, rng: random.Random) -> SimTable:
        return self.apply(table, rng)


@dataclass
class SimulationRecord:
    upstream: str
    table: str
    mutation: Mutation
    new_table: SimTable
    seed: int


# --------------------------------------------------------------------------
# Naming pools for rename mutations
# --------------------------------------------------------------------------

_RENAME_STYLES = {
    "same_meaning": ["{base}_value", "{base}_v2", "{base}_new", "new_{base}"],
    "abbrev": ["cust_id", "usr_id", "amt", "qty", "ts", "dt"],
    "synonym": {
        "phone": ["mobile_no", "tel", "contact_phone"],
        "nickname": ["display_name", "screen_name", "alias"],
        "total_amount": ["order_amount", "payable_amount", "grand_total"],
        "created_at": ["create_time", "create_ts", "gmt_create"],
        "updated_at": ["update_time", "modify_ts", "gmt_modified"],
        "user_id": ["uid", "buyer_id", "member_id"],
        "status": ["order_state", "state", "order_status"],
        "qty": ["quantity", "num", "item_count"],
        "device_id": ["did", "device_uid", "phone_device_id"],
        "score": ["risk_score", "points", "grade_score"],
        "merchant_id": ["mid", "seller_id", "shop_id"],
    },
}


def _rename_candidates(base: str, rng: random.Random) -> list[str]:
    out = list(_RENAME_STYLES["synonym"].get(base, []))
    out += [s.format(base=base) for s in _RENAME_STYLES["same_meaning"]]
    if base.endswith("_id"):
        out.append(base.replace("_id", "_key"))
    if base.endswith("_at"):
        out.append(base.replace("_at", "_time"))
    rng.shuffle(out)
    return [c for c in out if c != base]


# --------------------------------------------------------------------------
# Mutation library
# --------------------------------------------------------------------------


def _mut_rename(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns]
    col = rng.choice(candidates)
    cands = _rename_candidates(col.name, rng)
    new_name = cands[0] if cands else col.name + "_renamed"

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for c in t.columns:
            if c.name == col.name:
                c.name = new_name
        t.primary_key = [new_name if k == col.name else k for k in t.primary_key]
        t.unique_keys = [[new_name if k == col.name else k for k in key] for key in t.unique_keys]
        for rule in t.rules:
            if rule.get("column") == col.name:
                rule["column"] = new_name
            if "expression" in rule:
                rule["expression"] = _replace_token(rule["expression"], col.name, new_name)
        for row in t.rows:
            if col.name in row:
                row[new_name] = row.pop(col.name)
        if t.freshness and t.freshness.get("column") == col.name:
            t.freshness["column"] = new_name
        return t

    return Mutation(
        kind="rename_column",
        label="breaking",
        description=f"rename {col.name} -> {new_name} (same semantics)",
        apply=apply,
        rename_pair=(col.name, new_name),
        affected_columns=[col.name],
    )


def _replace_token(expr: str, old: str, new: str) -> str:
    import re

    return re.sub(rf"\b{re.escape(old)}\b", new, expr)


def _mut_rename_hard(table: SimTable, rng: random.Random) -> Mutation:
    """A rename whose new name shares almost nothing with the old one.

    Only data profiling (value overlap between the old and new samples) can
    identify these reliably; name similarity alone will miss them.  This is the
    case that motivates shipping a sample with every submission.
    """
    pools = {
        "phone": ["mobile_no", "contact_tel"],
        "nickname": ["display_name", "screen_alias"],
        "total_amount": ["grand_total", "payable_sum"],
        "created_at": ["gmt_create", "born_ts"],
        "updated_at": ["gmt_modified", "touched_ts"],
        "user_id": ["buyer_ref", "member_ref"],
        "status": ["state_code", "lifecycle_phase"],
        "qty": ["item_count", "pieces"],
        "device_id": ["handset_ref", "terminal_uid"],
        "score": ["grade_points", "rating_value"],
        "merchant_id": ["seller_ref", "shop_ref"],
        "amount": ["gross_sum", "settle_value"],
        "fee": ["commission_sum", "charge_value"],
        "currency": ["ccy_code", "settle_ccy"],
    }
    options = [(c, name) for c in table.columns for name in pools.get(c.name, [])]
    if not options:
        raise NotApplicable("no hard-rename candidate column")
    col, new_name = options[0]
    if table.column(new_name):
        raise NotApplicable("hard-rename target already exists")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for c in t.columns:
            if c.name == col.name:
                c.name = new_name
        t.primary_key = [new_name if k == col.name else k for k in t.primary_key]
        t.unique_keys = [[new_name if k == col.name else k for k in key] for key in t.unique_keys]
        for rule in t.rules:
            if rule.get("column") == col.name:
                rule["column"] = new_name
            if "expression" in rule:
                rule["expression"] = _replace_token(rule["expression"], col.name, new_name)
        for row in t.rows:
            if col.name in row:
                row[new_name] = row.pop(col.name)
        if t.freshness and t.freshness.get("column") == col.name:
            t.freshness["column"] = new_name
        return t

    return Mutation(
        kind="rename_hard",
        label="breaking",
        description=f"rename {col.name} -> {new_name} (no name resemblance)",
        apply=apply,
        rename_pair=(col.name, new_name),
        affected_columns=[col.name],
    )


def _mut_rename_with_tightening(table: SimTable, rng: random.Random) -> Mutation:
    """Rename plus a constraint narrowing: should yield two changes."""
    base_mut = _mut_rename(table, rng)
    old_name, new_name = base_mut.rename_pair  # type: ignore[misc]
    col = table.column(old_name)
    assert col is not None

    def apply(t: SimTable, r: random.Random) -> SimTable:
        base_mut.apply(t, r)
        c = t.column(new_name)
        assert c is not None
        if c.type in {"int", "bigint", "decimal"}:
            lo = c.minimum if c.minimum is not None else 0
            c.minimum = (float(lo) + 1) if isinstance(lo, float) else int(lo) + 1
        elif c.type == "string" and c.enum:
            c.enum = c.enum[: max(1, len(c.enum) - 1)]
        else:
            c.required = True
        return t

    return Mutation(
        kind="rename_with_tightening",
        label="breaking",
        description=f"rename {old_name} -> {new_name} and narrow constraints",
        apply=apply,
        rename_pair=(old_name, new_name),
        affected_columns=[new_name],
    )


def _mut_drop(table: SimTable, rng: random.Random) -> Mutation:
    optional = [c for c in table.columns if c.name not in table.primary_key]
    if not optional:
        raise NotApplicable("no non-key column to drop")
    col = optional[0]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.columns = [c for c in t.columns if c.name != col.name]
        t.unique_keys = [k for k in t.unique_keys if col.name not in k]
        t.rules = [r for r in t.rules if r.get("column") != col.name]
        for row in t.rows:
            row.pop(col.name, None)
        return t

    return Mutation("drop_column", "breaking", f"drop column {col.name}", apply,
                    affected_columns=[col.name])


def _mut_add_nullable(table: SimTable, rng: random.Random) -> Mutation:
    name = f"ext_{rng.choice(['src', 'tag', 'channel', 'biz'])}_code"
    while table.column(name):
        name += "_x"
    col = ColSpec(name=name, type="string", sql_type="TEXT", required=False, gen="text")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.columns.append(col)
        for row in t.rows:
            row[name] = None
        return t

    return Mutation("add_nullable_column", "compatible", f"add nullable column {name}", apply)


def _mut_add_required(table: SimTable, rng: random.Random) -> Mutation:
    name = f"mandatory_{rng.choice(['flag', 'code', 'ref'])}"
    while table.column(name):
        name += "_x"
    col = ColSpec(name=name, type="string", sql_type="TEXT", required=True, gen="text")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.columns.append(col)
        for i, row in enumerate(t.rows):
            row[name] = f"F{i:05d}"
        return t

    return Mutation("add_required_column", "breaking", f"add NOT NULL column {name}", apply)


def _mut_type_widen(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.type in {"int", "bigint"}]
    if not candidates:
        raise NotApplicable("no int/bigint column to widen")
    col = rng.choice(candidates)
    new_type = "bigint" if col.type == "int" else "decimal"

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.type = new_type
        c.sql_type = {"bigint": "INTEGER", "decimal": "REAL"}.get(new_type, c.sql_type)
        return t

    return Mutation("type_widen", "compatible", f"widen {col.name} {col.type}->{new_type}", apply,
                    affected_columns=[col.name])


def _mut_type_narrow(table: SimTable, rng: random.Random) -> Mutation:
    numeric = [c for c in table.columns if c.type == "decimal"]
    if not numeric:
        raise NotApplicable("no decimal column to narrow")
    col = numeric[0]
    old = col.type

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        if c.type == "decimal":
            c.type = "int"
            c.precision = None
            c.scale = None
            c.sql_type = "INTEGER"
            for row in t.rows:
                if isinstance(row.get(col.name), float):
                    row[col.name] = int(row[col.name])
        else:
            c.required = True
        return t

    return Mutation("type_narrow", "breaking", f"narrow {col.name} {old}->int", apply,
                    affected_columns=[col.name])


def _mut_enum_tighten(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.enum and len(c.enum) >= 2]
    if not candidates:
        raise NotApplicable("no enum column with 2+ values")
    col = rng.choice(candidates)
    removed = col.enum[-1]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None and c.enum
        c.enum = [e for e in c.enum if e != removed]
        return t

    return Mutation("enum_remove_value", "breaking", f"remove enum value {removed!r} from {col.name}", apply)


def _mut_enum_widen(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.enum]
    if not candidates:
        raise NotApplicable("no enum column")
    col = rng.choice(candidates)
    pool = [e for e in (col.enum or [])]
    new_val = f"{pool[0]}_v2" if pool else "new"

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.enum = list(c.enum or []) + [new_val]
        return t

    return Mutation("enum_add_value", "compatible", f"add enum value {new_val!r} to {col.name}", apply)


def _mut_range_tighten(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.maximum is not None and c.type in {"int", "bigint", "decimal"}]
    if not candidates:
        raise NotApplicable("no numeric column with a maximum")
    col = rng.choice(candidates)
    old_max = col.maximum
    new_max = old_max / 2 if isinstance(old_max, float) else int(old_max) // 2

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.maximum = new_max
        return t

    return Mutation("range_tighten", "breaking", f"tighten {col.name} max {old_max}->{new_max}", apply)


def _mut_range_widen(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.maximum is not None]
    if not candidates:
        raise NotApplicable("no column with a maximum")
    col = rng.choice(candidates)
    old_max = col.maximum
    new_max = old_max * 2

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.maximum = new_max
        return t

    return Mutation("range_widen", "compatible", f"widen {col.name} max {old_max}->{new_max}", apply)


def _mut_required_tighten(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if not c.required]
    if not candidates:
        raise NotApplicable("no nullable column to tighten")
    col = rng.choice(candidates)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.required = True
        return t

    return Mutation("required_tighten", "breaking", f"make {col.name} NOT NULL", apply)


def _mut_required_relax(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.required and c.name not in table.primary_key]
    if not candidates:
        raise NotApplicable("no required non-key column to relax")
    col = rng.choice(candidates)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.required = False
        return t

    return Mutation("required_relax", "compatible", f"make {col.name} nullable", apply)


def _mut_unique_tighten(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if not c.unique and c.type not in {"json", "bytes"} and c.name not in table.primary_key]
    if not candidates:
        raise NotApplicable("no non-unique column")
    col = rng.choice(candidates)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.unique = True
        return t

    return Mutation("unique_tighten", "breaking", f"add uniqueness on {col.name}", apply)


def _mut_unique_relax(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.unique]
    if not candidates:
        raise NotApplicable("no unique column to relax")
    col = rng.choice(candidates)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.unique = False
        return t

    return Mutation("unique_relax", "compatible", f"remove uniqueness on {col.name}", apply)


def _mut_precision_narrow(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.type == "decimal" and c.scale is not None]
    if not candidates:
        raise NotApplicable("no decimal column with a scale")
    col = rng.choice(candidates)
    old = (col.precision, col.scale)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.scale = max(0, (c.scale or 2) - 1)
        return t

    return Mutation("precision_narrow", "breaking", f"reduce scale on {col.name} {old}", apply)


def _mut_pk_change(table: SimTable, rng: random.Random) -> Mutation:
    if not table.primary_key:
        raise NotApplicable("no primary key to extend")
    extra = [c for c in table.columns if c.name not in table.primary_key]
    if not extra:
        raise NotApplicable("no extra column for a composite key")
    col = rng.choice(extra)
    old = list(table.primary_key)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.primary_key = list(t.primary_key) + [col.name]
        return t

    return Mutation("primary_key_change", "breaking", f"extend PK {old} with {col.name}", apply)


def _mut_freshness_tighten(table: SimTable, rng: random.Random) -> Mutation:
    if not table.freshness or not table.freshness.get("max_age"):
        raise NotApplicable("no existing freshness SLA to tighten")
    old = (table.freshness or {}).get("max_age")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        if t.freshness is None:
            t.freshness = {"max_age": "PT1H", "severity": "high"}
        else:
            t.freshness["max_age"] = "PT1H"
        return t

    return Mutation("freshness_tighten", "breaking", f"tighten freshness {old}->PT1H", apply)


def _mut_freshness_relax(table: SimTable, rng: random.Random) -> Mutation:
    if not table.freshness or not table.freshness.get("max_age"):
        raise NotApplicable("no existing freshness SLA to relax")
    old = (table.freshness or {}).get("max_age")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        if t.freshness is None:
            t.freshness = {"max_age": "P7D", "severity": "high"}
        else:
            t.freshness["max_age"] = "P7D"
        return t

    return Mutation("freshness_relax", "compatible", f"relax freshness {old}->P7D", apply)


def _mut_add_rule(table: SimTable, rng: random.Random) -> Mutation:
    nums = [c for c in table.columns if c.type in {"int", "bigint", "decimal"}]
    if not nums:
        raise NotApplicable("no numeric column for a rule")
    col = rng.choice(nums)
    name = f"rule_{col.name}_nonneg"

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.rules.append({"name": name, "kind": "range", "column": col.name, "minimum": 0, "severity": "medium"})
        return t

    return Mutation("rule_added", "breaking", f"add rule {name}", apply)


def _mut_description(table: SimTable, rng: random.Random) -> Mutation:
    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.description = (t.description or "") + " (doc updated)"
        return t

    return Mutation("description_change", "compatible", "description-only change", apply)


def _mut_drop_and_swap(table: SimTable, rng: random.Random) -> Mutation:
    """Drop one column and add an unrelated one: must NOT be read as a rename."""
    optional = [c for c in table.columns if c.name not in table.primary_key]
    if not optional:
        raise NotApplicable("no non-key column")
    col = optional[0]
    new_name = "segment_code"
    while table.column(new_name):
        new_name += "_x"

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.columns = [c for c in t.columns if c.name != col.name]
        t.rules = [r for r in t.rules if r.get("column") != col.name]
        for row in t.rows:
            row.pop(col.name, None)
        t.columns.append(
            ColSpec(
                name=new_name,
                type="string",
                sql_type="TEXT",
                required=False,
                gen="text",
            )
        )
        for row in t.rows:
            row[new_name] = None
        return t

    return Mutation(
        "drop_and_add_unrelated",
        "breaking",
        f"drop {col.name} and add unrelated {new_name}",
        apply,
        affected_columns=[col.name, new_name],
    )


def _mut_noop(table: SimTable, rng: random.Random) -> Mutation:
    """A change that must NOT produce a diff: physical DDL with no contract impact."""

    def apply(t: SimTable, r: random.Random) -> SimTable:
        return t

    return Mutation("noop", "compatible", "no semantic change", apply)


def _mut_reorder(table: SimTable, rng: random.Random) -> Mutation:
    def apply(t: SimTable, r: random.Random) -> SimTable:
        r.shuffle(t.columns)
        return t

    return Mutation("reorder_columns", "compatible", "reorder columns", apply)





# --------------------------------------------------------------------------
# Dirty-data injections
# --------------------------------------------------------------------------


@dataclass
class DirtyInjection:
    kind: str
    description: str
    apply: Callable[[SimTable, random.Random], SimTable]
    #: Columns that should end up with at least one violation.
    affected_columns: list[str] = field(default_factory=list)
    detectable: bool = True


def _affected(table: SimTable, rng: random.Random, n: int, predicate) -> list[dict[str, Any]]:
    candidates = [r for r in table.rows if predicate(r)]
    return rng.sample(candidates, min(n, len(candidates))) if candidates else []


def dirty_injection_library(table: SimTable, rng: random.Random) -> list[DirtyInjection]:
    """Build a table-specific set of dirty injections, each with ground truth."""
    out: list[DirtyInjection] = []
    required_cols = [c for c in table.columns if c.required and c.name not in table.primary_key]
    typed = [c for c in table.columns if c.type in {"int", "bigint", "decimal"}]
    enum_cols = [c for c in table.columns if c.enum]
    unique_cols = [c for c in table.columns if c.unique]
    ts_cols = [c for c in table.columns if c.type == "timestamp"]
    num_cols = [c for c in table.columns if c.type in {"int", "bigint", "decimal"}]

    def null_required(t: SimTable, r: random.Random, col=None) -> SimTable:
        c = col or required_cols[0]
        rows = _affected(t, r, 3, lambda row: row.get(c.name) is not None)
        for row in rows:
            row[c.name] = None
        return t

    if required_cols:
        out.append(
            DirtyInjection(
                "null_in_required",
                "set NULL in NOT NULL column",
                lambda t, r: null_required(t, r),
                affected_columns=[c.name for c in required_cols[:1]],
            )
        )

    def bad_enum(t: SimTable, r: random.Random) -> SimTable:
        c = enum_cols[0]
        rows = _affected(t, r, 3, lambda row: row.get(c.name) is not None)
        for i, row in enumerate(rows):
            row[c.name] = f"unexpected_{i}"
        return t

    if enum_cols:
        out.append(
            DirtyInjection("invalid_enum", "write a value outside the enum", bad_enum,
                           affected_columns=[enum_cols[0].name])
        )

    def out_of_range(t: SimTable, r: random.Random) -> SimTable:
        c = ([x for x in num_cols if x.maximum is not None] or num_cols)[0]
        if c.maximum is None:
            return t
        rows = _affected(t, r, 3, lambda row: isinstance(row.get(c.name), (int, float)))
        for row in rows:
            row[c.name] = c.maximum * 10 + 1000
        return t

    if any(c.maximum is not None for c in num_cols):
        out.append(DirtyInjection("value_out_of_range", "push numbers past the declared maximum", out_of_range))

    def negative(t: SimTable, r: random.Random) -> SimTable:
        c = ([x for x in num_cols if x.minimum is not None and x.minimum >= 0] or num_cols)[0]
        rows = _affected(t, r, 3, lambda row: isinstance(row.get(c.name), (int, float)))
        for row in rows:
            row[c.name] = -abs(row[c.name] or 1) - 7
        return t

    if any(c.minimum is not None for c in num_cols):
        out.append(DirtyInjection("negative_value", "negative value where minimum is 0", negative))

    def duplicate_unique(t: SimTable, r: random.Random) -> SimTable:
        c = unique_cols[0]
        if len(t.rows) < 2:
            return t
        a, b = r.sample(t.rows, 2)
        b[c.name] = a[c.name]
        return t

    if unique_cols:
        out.append(
            DirtyInjection("duplicate_unique", "copy a value into another row's unique column", duplicate_unique,
                           affected_columns=[unique_cols[0].name])
        )

    def duplicate_pk(t: SimTable, r: random.Random) -> SimTable:
        if len(t.rows) < 2 or not t.primary_key:
            return t
        a, b = r.sample(t.rows, 2)
        for k in t.primary_key:
            b[k] = a[k]
        return t

    if table.primary_key:
        out.append(DirtyInjection("duplicate_primary_key", "duplicate primary key", duplicate_pk,
                                  affected_columns=list(table.primary_key)))

    def bad_timestamp(t: SimTable, r: random.Random) -> SimTable:
        # Pick a timestamp column that actually carries values in this table;
        # a NULL-only column cannot be dirtied.
        c = next((x for x in ts_cols if any(row.get(x.name) is not None for row in t.rows)), None)
        if c is None:
            return t
        rows = _affected(t, r, 2, lambda row: row.get(c.name) is not None)
        for row in rows:
            row[c.name] = "not-a-timestamp"
        return t

    if ts_cols:
        out.append(DirtyInjection("invalid_timestamp", "unparseable timestamp string", bad_timestamp,
                                  affected_columns=[]))

    def stale(t: SimTable, r: random.Random) -> SimTable:
        c = t.freshness.get("column") if t.freshness else None
        col = t.column(c) if c else (ts_cols[0] if ts_cols else None)
        if col is None:
            return t
        stale_ts = (datetime.now(UTC) - timedelta(days=30)).replace(microsecond=0).isoformat()
        for row in t.rows:
            row[col.name] = stale_ts
        return t

    if table.freshness:
        out.append(
            DirtyInjection("stale_data", "all rows older than the freshness SLA", stale,
                           affected_columns=[table.freshness.get("column", "")])
        )

    def wrong_type(t: SimTable, r: random.Random) -> SimTable:
        c = typed[0]
        rows = _affected(t, r, 3, lambda row: row.get(c.name) is not None)
        for row in rows:
            row[c.name] = "twelve"  # string where a number is declared
        return t

    if typed:
        out.append(DirtyInjection("type_mismatch", "string where a number is declared", wrong_type,
                                  affected_columns=[typed[0].name]))

    # ---- subtle injections: near-misses that naive checks let through ----
    def boundary_plus_one(t: SimTable, r: random.Random) -> SimTable:
        c = next((x for x in num_cols if x.maximum is not None), None)
        if c is None:
            return t
        rows = _affected(t, r, 2, lambda row: isinstance(row.get(c.name), (int, float)))
        for row in rows:
            row[c.name] = c.maximum + (1 if isinstance(c.maximum, int) else 0.01)
        return t

    if any(c.maximum is not None for c in num_cols):
        out.append(DirtyInjection("boundary_plus_epsilon", "value one unit past the declared maximum",
                                  boundary_plus_one))

    def enum_case_mismatch(t: SimTable, r: random.Random) -> SimTable:
        c = enum_cols[0] if enum_cols else None
        if c is None or not all(isinstance(e, str) for e in (c.enum or [])):
            return t
        rows = _affected(t, r, 2, lambda row: isinstance(row.get(c.name), str))
        for row in rows:
            row[c.name] = row[c.name].upper()
        return t

    if enum_cols:
        out.append(DirtyInjection("enum_case_mismatch", "enum value with different letter case",
                                  enum_case_mismatch))

    def numeric_string(t: SimTable, r: random.Random) -> SimTable:
        c = typed[0] if typed else None
        if c is None:
            return t
        rows = _affected(t, r, 2, lambda row: isinstance(row.get(c.name), (int, float)))
        for row in rows:
            row[c.name] = str(row[c.name])
        return t

    if typed:
        out.append(DirtyInjection("numeric_as_string", "number serialized as a string", numeric_string))

    def off_scale(t: SimTable, r: random.Random) -> SimTable:
        c = next((x for x in table.columns if x.type == "decimal" and x.scale is not None), None)
        if c is None:
            return t
        rows = _affected(t, r, 2, lambda row: isinstance(row.get(c.name), (int, float)))
        for row in rows:
            row[c.name] = round(row[c.name] + 0.0001, 6)
        return t

    if any(c.type == "decimal" and c.scale is not None for c in table.columns):
        out.append(DirtyInjection("scale_overflow", "value with more decimal places than the declared scale",
                                  off_scale))

    def dup_with_null(t: SimTable, r: random.Random) -> SimTable:
        # NULLs must not be treated as duplicate values, even in a unique column.
        c = next((x for x in unique_cols if x.required), None)
        if c is None or len(t.rows) < 2:
            return t
        for row in t.rows[: min(5, len(t.rows))]:
            row[c.name] = None
        return t

    if unique_cols:
        out.append(DirtyInjection("null_in_unique", "NULLs in a unique column (legal; must not be flagged)",
                                  dup_with_null))

    for rule in [x for x in table.rules if x.get("kind") == "expression"]:
        expr = rule.get("expression", "")
        cols = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr))
        left = right = None
        m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*(<=|>=|<|>)\s*([A-Za-z_][A-Za-z0-9_]*)", expr)
        if m:
            left, op, right = m.group(1), m.group(2), m.group(3)
        distractor = next((c.name for c in table.columns if c.name not in cols and c.type in {"int", "bigint", "decimal"}), None)

        def break_expression(t: SimTable, r: random.Random, left=left, right=right, distractor=distractor) -> SimTable:
            if left and right:
                rows = [row for row in t.rows if row.get(left) is not None and row.get(right) is not None]
                for row in r.sample(rows, min(3, len(rows))):
                    # swap the two fields so the ordering constraint fails
                    row[left], row[right] = row[right], row[left]
            elif distractor:
                c = t.column(distractor)
                assert c is not None
                rows = [row for row in t.rows if isinstance(row.get(distractor), (int, float))]
                for row in r.sample(rows, min(3, len(rows))):
                    row[distractor] = (c.maximum or 1000) * 1000
            return t

        out.append(
            DirtyInjection(
                "cross_field_inconsistency",
                f"violate expression rule {rule.get('name')}",
                break_expression,
                affected_columns=sorted(cols),
            )
        )

    return out


# --------------------------------------------------------------------------
# Orchestration helpers
# --------------------------------------------------------------------------


def apply_structural(table: SimTable, factory: Callable[[SimTable, random.Random], Mutation], rng: random.Random) -> SimulationRecord:
    mutation = factory(table, rng)
    new_table = mutation.apply(table.copy(), rng)
    return SimulationRecord(table.system, table.name, mutation, new_table, seed=rng.randrange(1 << 30))


def sample_structural_mutations(table: SimTable, rng: random.Random, count: int) -> list[SimulationRecord]:
    """Sample distinct structural mutation kinds applicable to this table."""
    factories = list(ALL_STRUCTURAL_MUTATIONS)
    rng.shuffle(factories)
    records: list[SimulationRecord] = []
    for f in factories:
        try:
            rec = apply_structural(table, f, rng)
        except Exception:
            continue
        records.append(rec)
        if len(records) >= count:
            break
    return records


def apply_dirty(table: SimTable, injection: DirtyInjection, rng: random.Random) -> tuple[SimTable, DirtyInjection]:
    t = table.copy()
    injection.apply(t, rng)
    return t, injection

def _mut_rename_swap(table: SimTable, rng: random.Random) -> Mutation:
    """Swap the names of two columns of the same type.

    This is the hardest rename case: structurally it is two renames crossing
    over, and greedy matching can pair the wrong ends.  Ground truth is breaking
    (both names now mean something else).
    """
    by_type: dict[str, list[ColSpec]] = {}
    for c in table.columns:
        if c.name in table.primary_key:
            continue
        by_type.setdefault(c.type, []).append(c)
    pair = next((cs[:2] for cs in by_type.values() if len(cs) >= 2), None)
    if not pair:
        raise NotApplicable("no two same-type non-key columns to swap")
    a, b = pair
    fp_a = (a.type, a.required, a.enum, a.minimum, a.maximum, a.unique, a.precision, a.scale)
    fp_b = (b.type, b.required, b.enum, b.minimum, b.maximum, b.unique, b.precision, b.scale)
    identical = fp_a == fp_b

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for c in t.columns:
            if c.name == a.name:
                c.name = b.name + "__swap"
            elif c.name == b.name:
                c.name = a.name
        for c in t.columns:
            if c.name == b.name + "__swap":
                c.name = b.name
        for row in t.rows:
            if a.name in row and b.name in row:
                row[a.name], row[b.name] = row[b.name], row[a.name]
        return t

    return Mutation(
        "rename_swap", "breaking", f"swap names/values of {a.name} and {b.name}", apply,
        rename_pair=None, affected_columns=[a.name, b.name],
        detectable=not identical,
        undetectable_reason=(
            f"{a.name} and {b.name} have identical declarations; the contract cannot "
            "distinguish a value swap from an untouched table"
            if identical else None
        ),
    )


def _mut_rename_of_null_column(table: SimTable, rng: random.Random) -> Mutation:
    """Rename an all-NULL optional column.

    Undetectable without an explicit hint: there is no data to compare.  Marked
    with confidence="undetectable" so the harness reports it as a limitation
    instead of silently counting it either way.
    """
    candidates = [
        c for c in table.columns
        if not c.required and all(r.get(c.name) is None for r in table.rows)
    ]
    if not candidates:
        raise NotApplicable("no all-NULL optional column")
    col = candidates[0]
    new_name = "renamed_empty_" + col.name

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for c in t.columns:
            if c.name == col.name:
                c.name = new_name
        for row in t.rows:
            if col.name in row:
                row[new_name] = row.pop(col.name)
        return t

    return Mutation(
        "rename_null_column", "breaking",
        f"rename all-NULL column {col.name} -> {new_name} (no data evidence)", apply,
        rename_pair=(col.name, new_name), affected_columns=[col.name],
        detectable=False,
        undetectable_reason=(
            f"{col.name} is entirely NULL, so name-similarity and value-overlap are both "
            "silent; only an explicit renamed_from hint can identify this rename"
        ),
    )


def _mut_enum_reorder(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [c for c in table.columns if c.enum and len(c.enum) > 1]
    if not candidates:
        raise NotApplicable("no enum column to reorder")
    col = candidates[0]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None and c.enum
        c.enum = list(reversed(c.enum))
        return t

    return Mutation("enum_reorder", "compatible", f"reorder enum values of {col.name} (no semantic change)", apply)


def _mut_freshness_equivalent(table: SimTable, rng: random.Random) -> Mutation:
    """Represent the same SLA in different ISO-8601 spelling: PT24H -> P1D."""
    if not table.freshness or table.freshness.get("max_age") != "PT24H":
        raise NotApplicable("no PT24H freshness SLA")
    old = table.freshness["max_age"]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.freshness["max_age"] = "P1D"
        return t

    return Mutation("freshness_equivalent", "compatible", f"rewrite {old} as P1D (same SLA)", apply)


def _mut_add_unique_key(table: SimTable, rng: random.Random) -> Mutation:
    candidates = [[c.name for c in table.columns if c.name in table.primary_key]]
    extra = [c for c in table.columns if c.name not in table.primary_key and c.type in {"string", "int", "bigint"}]
    if not table.primary_key or not extra:
        raise NotApplicable("no PK or no extra column")
    new_key = list(table.primary_key[:1]) + [extra[0].name]
    if new_key in table.unique_keys:
        raise NotApplicable("key already declared")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.unique_keys = list(t.unique_keys) + [list(new_key)]
        return t

    return Mutation("unique_key_added", "breaking", f"add unique key {new_key}", apply,
                    affected_columns=new_key)


def _mut_drop_unique_key(table: SimTable, rng: random.Random) -> Mutation:
    if not table.unique_keys:
        raise NotApplicable("no unique key to drop")
    key = list(table.unique_keys[0])

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.unique_keys = [k for k in t.unique_keys if k != key]
        return t

    return Mutation("unique_key_removed", "compatible", f"drop unique key {key}", apply,
                    affected_columns=key)


def _mut_enum_value_type(table: SimTable, rng: random.Random) -> Mutation:
    """Numeric string enum becomes integers: same labels, different representation."""
    candidates = [c for c in table.columns if c.enum and all(isinstance(e, str) and e.isdigit() for e in c.enum)]
    if not candidates:
        raise NotApplicable("no numeric-string enum")
    col = candidates[0]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None and c.enum
        c.enum = [int(e) for e in c.enum]
        c.type = "int"
        for row in t.rows:
            v = row.get(col.name)
            if isinstance(v, str) and v.isdigit():
                row[col.name] = int(v)
        return t

    return Mutation("enum_value_type_change", "breaking",
                    f"enum on {col.name} changed representation strings->ints", apply,
                    affected_columns=[col.name])


def _mut_narrow_and_widen(table: SimTable, rng: random.Random) -> Mutation:
    """One column tightened, another relaxed: the version as a whole is breaking."""
    ranges = [c for c in table.columns if c.maximum is not None and c.minimum is not None
              and c.type in {"int", "bigint", "decimal"}]
    if len(ranges) < 2:
        raise NotApplicable("need two bounded numeric columns")
    a, b = ranges[0], ranges[1]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        ca, cb = t.column(a.name), t.column(b.name)
        assert ca and cb
        ca.maximum = ca.maximum / 2 if isinstance(ca.maximum, float) else int(ca.maximum) // 2
        cb.maximum = cb.maximum * 2
        return t

    return Mutation("narrow_and_widen", "breaking",
                    f"tighten {a.name} while widening {b.name}", apply,
                    affected_columns=[a.name, b.name])




def _mut_drop_and_add_decoy(table: SimTable, rng: random.Random) -> Mutation:
    """Drop a column and add a *different* column whose name looks similar.

    This attacks rename precision: a name-only matcher will confidently call
    this a rename, which is wrong and dangerous — it would tell downstream
    owners "just change the column name" when the semantic content is different.
    Value overlap is the only reliable discriminator here.
    """
    optional = [c for c in table.columns if c.name not in table.primary_key and c.type == "string"]
    if not optional:
        raise NotApplicable("no non-key string column")
    old = optional[0]
    name = _DECOYS.get(old.name)
    if name is None:
        name = old.name + "_hash"
    if table.column(name):
        raise NotApplicable("decoy name already exists")
    col = ColSpec(name=name, type="string", sql_type="TEXT", required=True, gen="hash_text")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        t.columns = [c for c in t.columns if c.name != old.name]
        t.rules = [r for r in t.rules if r.get("column") != old.name]
        for row in t.rows:
            row.pop(old.name, None)
        t.columns.append(col)
        for i, row in enumerate(t.rows):
            row[name] = hashlib.sha256(f"{i}:{name}".encode()).hexdigest()[:24]
        return t

    return Mutation(
        "drop_add_decoy",
        "breaking",
        f"drop {old.name} and add semantically different {name}",
        apply,
        affected_columns=[old.name, name],
    )


_DECOYS = {
    "nickname": "nickname_hash",
    "phone": "phone_hash",
    "currency": "currency_code_v2",
    "status": "status_hash",
    "device_type": "device_profile",
    "merchant_name": "merchant_name_hash",
    "os_version": "os_version_hash",
    "sku_id": "sku_hash",
    "channel": "channel_group",
    "category": "category_code",
    "reason": "reason_code",
}


def _mut_compound(table: SimTable, rng: random.Random) -> Mutation:
    """Apply two compatible-looking changes where one is actually breaking.

    Real upstreams ship several changes in one release.  A classifier that looks
    only at the first or the "most obvious" change will release a broken version.
    """
    parts = [_mut_add_nullable, _mut_range_tighten]

    # Every part must be applicable, otherwise the ground-truth label would
    # claim a breaking change that was never actually made.
    for factory in parts:
        factory(table, rng)

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for factory in parts:
            t = factory(t, r).apply(t, r)
        return t

    return Mutation(
        "compound_mixed",
        "breaking",
        "add nullable column + tighten a numeric range",
        apply,
    )


def _mut_compound_compatible(table: SimTable, rng: random.Random) -> Mutation:
    parts = [_mut_add_nullable, _mut_enum_widen, _mut_type_widen]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for factory in parts:
            try:
                t = factory(t, r).apply(t, r)
            except NotApplicable:
                continue
        return t

    return Mutation(
        "compound_compatible",
        "compatible",
        "add nullable column + widen enum + widen type",
        apply,
    )




def _mut_rename_plus_drop(table: SimTable, rng: random.Random) -> Mutation:
    """Rename one column AND drop another in the same change.

    Rename matching must still find the moved column while reporting the drop as
    destructive — the two must not collapse into "rename" or into "two drops".
    """
    if len(table.columns) < 4:
        raise NotApplicable("not enough columns")
    non_key = [c for c in table.columns if c.name not in table.primary_key]
    if len(non_key) < 2:
        raise NotApplicable("not enough non-key columns")
    moved = non_key[0]
    dropped = non_key[1]
    cands = _rename_candidates(moved.name, rng)
    new_name = cands[0] if cands else moved.name + "_moved"
    if table.column(new_name):
        raise NotApplicable("rename target exists")

    def apply(t: SimTable, r: random.Random) -> SimTable:
        for c in t.columns:
            if c.name == moved.name:
                c.name = new_name
        t.columns = [c for c in t.columns if c.name != dropped.name]
        t.unique_keys = [[new_name if k == moved.name else k for k in key] for key in t.unique_keys]
        t.unique_keys = [k for k in t.unique_keys if dropped.name not in k]
        t.primary_key = [new_name if k == moved.name else k for k in t.primary_key]
        t.rules = [x for x in t.rules if x.get("column") not in {dropped.name}]
        for row in t.rows:
            if moved.name in row:
                row[new_name] = row.pop(moved.name)
            row.pop(dropped.name, None)
        return t

    return Mutation(
        "rename_plus_drop", "breaking",
        f"rename {moved.name}->{new_name} and drop {dropped.name}", apply,
        rename_pair=(moved.name, new_name),
        affected_columns=[moved.name, dropped.name],
    )


def _mut_swap_required_flags(table: SimTable, rng: random.Random) -> Mutation:
    """Swap the NOT NULL flags of two same-typed columns.

    Both survive with identical names and types; only which one is required
    changes.  Contract-only diffing catches it; row sampling catches it too
    because one column's NULLs now violate.  Included as a precision stressor:
    a matcher that sees "column X changed constraints" must not also invent a
    rename.
    """
    same = {}
    for c in table.columns:
        if c.name in table.primary_key:
            continue
        same.setdefault((c.type, c.required), []).append(c)
    groups = [(k, v) for k, v in same.items() if len(v) >= 1]
    # find a pair with opposite requiredness and same type
    by_type: dict[str, list[ColSpec]] = {}
    for c in table.columns:
        if c.name not in table.primary_key:
            by_type.setdefault(c.type, []).append(c)
    pair = None
    for _, cols in by_type.items():
        req = [c for c in cols if c.required]
        opt = [c for c in cols if not c.required]
        if req and opt:
            pair = (req[0], opt[0])
            break
    if not pair:
        raise NotApplicable("no same-type required/optional pair")
    a, b = pair

    def apply(t: SimTable, r: random.Random) -> SimTable:
        ca, cb = t.column(a.name), t.column(b.name)
        assert ca and cb
        ca.required, cb.required = cb.required, ca.required
        return t

    return Mutation("required_flag_swap", "breaking",
                    f"swap NOT NULL between {a.name} and {b.name}", apply,
                    affected_columns=[a.name, b.name])


def _mut_retype_without_shape_change(table: SimTable, rng: random.Random) -> Mutation:
    """Change a logical type in a way the contract declares but rows cannot reveal.

    timestamp -> date on a column whose values are all midnight timestamps is
    indistinguishable from row data alone.  Expected to be caught by the diff
    engine (declaration changed) but worth measuring explicitly.
    """
    ts = [c for c in table.columns if c.type == "timestamp" and not c.required]
    if not ts:
        raise NotApplicable("no optional timestamp column")
    col = ts[0]

    def apply(t: SimTable, r: random.Random) -> SimTable:
        c = t.column(col.name)
        assert c is not None
        c.type = "date"
        c.sql_type = "TEXT"
        for row in t.rows:
            v = row.get(col.name)
            if isinstance(v, str):
                row[col.name] = v[:10]
        return t

    return Mutation("timestamp_to_date", "breaking",
                    f"narrow {col.name} timestamp->date", apply, affected_columns=[col.name])


ALL_STRUCTURAL_MUTATIONS = [
    _mut_rename,
    _mut_rename_hard,
    _mut_rename_with_tightening,
    _mut_drop,
    _mut_add_nullable,
    _mut_add_required,
    _mut_type_widen,
    _mut_type_narrow,
    _mut_enum_tighten,
    _mut_enum_widen,
    _mut_range_tighten,
    _mut_range_widen,
    _mut_required_tighten,
    _mut_required_relax,
    _mut_unique_tighten,
    _mut_unique_relax,
    _mut_precision_narrow,
    _mut_pk_change,
    _mut_freshness_tighten,
    _mut_freshness_relax,
    _mut_add_rule,
    _mut_description,
    _mut_reorder,
    _mut_noop,
    _mut_drop_and_swap,
    _mut_rename_swap,
    _mut_rename_of_null_column,
    _mut_enum_reorder,
    _mut_freshness_equivalent,
    _mut_add_unique_key,
    _mut_drop_unique_key,
    _mut_enum_value_type,
    _mut_narrow_and_widen,
    _mut_drop_and_add_decoy,
    _mut_compound,
    _mut_compound_compatible,
    _mut_rename_plus_drop,
    _mut_retype_without_shape_change,
    _mut_swap_required_flags,
]
