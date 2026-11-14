"""Contract diffing: classify a version bump as compatible or breaking.

Two things make this engine more than a structural compare:

1.  **Rename detection.**  A dropped column plus an added column is usually a
    rename.  Treating it as drop+add would page every downstream owner for a
    cosmetic change, while treating every drop+add as a rename would hide real
    removals.  We score candidate pairs on name similarity, type/constraint
    compatibility and — when data profiles are available — actual value overlap,
    then greedily assign the best pairs above a threshold.

2.  **Tightening detection.**  Relaxations (widening a type, adding enum values,
    dropping a uniqueness constraint) are backward compatible.  Tightenings
    (narrowing a range, adding an enum, making a column required) are breaking
    even though the column still "exists", because today's valid data may become
    tomorrow's violation.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from typing import Any

from .contract import Contract, Column, Rule

# Type widening lattice: maps type -> types it can widen into without loss.
_WIDENINGS = {
    "int": {"bigint", "decimal"},
    "bigint": {"decimal"},
    "date": {"timestamp"},
}
_NULLABLE_JSON_TYPES = {"json", "bytes"}

RENAME_AUTO_THRESHOLD = 0.72
RENAME_WEAK_THRESHOLD = 0.58


@dataclass
class Change:
    kind: str
    classification: str  # compatible | breaking
    severity: str  # info | low | medium | high | critical
    summary: str
    column: str | None = None
    old_column: str | None = None
    new_column: str | None = None
    before: Any = None
    after: Any = None
    reason: str = ""
    confidence: float | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    #: Whether the change must pass the sign-off gate and run in parallel.
    requires_signoff: bool = False
    requires_parallel: bool = False

    @property
    def breaking(self) -> bool:
        return self.classification == "breaking"

    def to_dict(self) -> dict[str, Any]:
        d = {
            "kind": self.kind,
            "classification": self.classification,
            "severity": self.severity,
            "summary": self.summary,
            "requires_signoff": self.requires_signoff,
            "requires_parallel": self.requires_parallel,
            "reason": self.reason,
        }
        for k in ("column", "old_column", "new_column", "before", "after", "confidence", "evidence"):
            v = getattr(self, k)
            if v not in (None, {}, []):
                d[k] = v
        return d


@dataclass
class ChangeSet:
    old_version: int | None
    new_version: int | None
    changes: list[Change] = field(default_factory=list)
    renames: list[dict[str, Any]] = field(default_factory=list)

    @property
    def breaking(self) -> list[Change]:
        return [c for c in self.changes if c.breaking]

    @property
    def compatible(self) -> list[Change]:
        return [c for c in self.changes if not c.breaking]

    @property
    def compatibility(self) -> str:
        if not self.changes:
            return "no_change"
        return "breaking" if self.breaking else "backward_compatible"

    @property
    def requires_signoff(self) -> bool:
        return any(c.requires_signoff for c in self.changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "old_version": self.old_version,
            "new_version": self.new_version,
            "compatibility": self.compatibility,
            "requires_signoff": self.requires_signoff,
            "breaking_count": len(self.breaking),
            "compatible_count": len(self.compatible),
            "changes": [c.to_dict() for c in self.changes],
            "renames": self.renames,
        }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _norm_name(name: str) -> str:
    tokens = re.split(r"[_\-.]", name.lower())
    return "".join(tokens)


def _name_similarity(a: str, b: str) -> float:
    na, nb = _norm_name(a), _norm_name(b)
    if na == nb:
        return 1.0
    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
    # token containment: cust -> customer_id / order_amt -> order_amount
    ta, tb = set(re.split(r"[_\-.]", a.lower())), set(re.split(r"[_\-.]", b.lower()))
    overlap = 0.0
    if ta and tb:
        overlap = len(ta & tb) / len(ta | tb)
    # common prefix/suffix beyond the plain ratio, e.g. "user_id" vs "uid"
    prefix = len(_common_prefix(na, nb)) / max(len(na), len(nb), 1)
    return max(ratio, 0.5 * ratio + 0.3 * overlap + 0.2 * prefix)


def _common_prefix(a: str, b: str) -> str:
    out = []
    for x, y in zip(a, b):
        if x != y:
            break
        out.append(x)
    return "".join(out)


def _constraint_fp(c: Column) -> tuple:
    return (
        c.type,
        c.required,
        frozenset(map(repr, c.enum)) if c.enum is not None else None,
        c.minimum,
        c.maximum,
        c.unique,
        c.precision,
        c.scale,
    )


def _constraint_similarity(a: Column, b: Column) -> float:
    if a.type != b.type:
        if b.type in _WIDENINGS.get(a.type, set()):
            return 0.6
        return 0.0
    parts = [
        a.required == b.required,
        (a.enum or None) == (b.enum or None),
        (a.minimum, a.maximum) == (b.minimum, b.maximum),
        a.unique == b.unique,
        (a.precision, a.scale) == (b.precision, b.scale),
    ]
    return sum(parts) / len(parts)


def _value_jaccard(old_profile: dict | None, new_profile: dict | None) -> float | None:
    """Jaccard of sampled values for a column, or None if incomparable."""
    if not old_profile or not new_profile:
        return None
    a = old_profile.get("sample_values")
    b = new_profile.get("sample_values")
    # A populated column does not turn into an all-NULL column by renaming.
    # Treat that asymmetry as strong negative evidence.
    if (a and not b) or (b and not a):
        return 0.0
    if not a or not b:
        return None
    sa = {repr(v) for v in a if v is not None}
    sb = {repr(v) for v in b if v is not None}
    if not sa or not sb:
        return None
    return len(sa & sb) / len(sa | sb)


def _type_widening(old: str, new: str) -> bool:
    return new in _WIDENINGS.get(old, set())


# --------------------------------------------------------------------------
# Rename detection
# --------------------------------------------------------------------------


def detect_renames(
    old: Contract,
    new: Contract,
    *,
    old_profiles: dict[str, dict] | None = None,
    new_profiles: dict[str, dict] | None = None,
    threshold: float = RENAME_WEAK_THRESHOLD,
) -> list[dict[str, Any]]:
    """Return candidate rename pairs between dropped and added columns."""
    old_names = {c.name: c for c in old.columns}
    new_names = {c.name: c for c in new.columns}
    removed = [c for n, c in old_names.items() if n not in new_names]
    added = [c for n, c in new_names.items() if n not in old_names]
    if not removed or not added:
        return []

    # Row-level evidence: value overlap is only meaningful when we have both an
    # old and a new sample for the candidate columns.
    pairs: list[tuple[float, Column, Column, dict[str, Any]]] = []
    for rc in removed:
        for ac in added:
            if ac.renamed_from:
                continue  # handled explicitly
            name_sim = _name_similarity(rc.name, ac.name)
            con_sim = _constraint_similarity(rc, ac)
            if con_sim == 0.0:
                continue
            data_sim = _value_jaccard((old_profiles or {}).get(rc.name), (new_profiles or {}).get(ac.name))
            # When both sides have sampled values, contradicting data is a veto:
            # a column whose values changed completely was not renamed, it was
            # replaced. Name resemblance alone must not override that.
            if data_sim is not None and data_sim < 0.20:
                continue
            score = 0.45 * name_sim + 0.25 * con_sim + 0.30 * (data_sim if data_sim is not None else name_sim)
            evidence = {
                "name_similarity": round(name_sim, 3),
                "constraint_similarity": round(con_sim, 3),
                "value_overlap": round(data_sim, 3) if data_sim is not None else None,
            }
            # A pure type-compatible column with identical value overlap is a
            # strong signal even when the name changed completely.
            if data_sim is not None and data_sim >= 0.85 and con_sim >= 0.6:
                score = max(score, 0.8)
            if score >= threshold and con_sim >= 0.6:
                pairs.append((score, rc, ac, evidence))

    pairs.sort(key=lambda p: p[0], reverse=True)
    used_old: set[str] = set()
    used_new: set[str] = set()
    result: list[dict[str, Any]] = []

    # Explicit hints always win, even if they fall below the threshold.
    for ac in added:
        hint = ac.renamed_from
        if hint and hint in old_names and hint not in new_names and hint not in used_old:
            rc = old_names[hint]
            result.append(
                {
                    "old_name": rc.name,
                    "new_name": ac.name,
                    "confidence": 1.0,
                    "evidence": {"explicit": True, "constraint_similarity": _constraint_similarity(rc, ac)},
                }
            )
            used_old.add(hint)
            used_new.add(ac.name)

    for score, rc, ac, evidence in pairs:
        if rc.name in used_old or ac.name in used_new:
            continue
        used_old.add(rc.name)
        used_new.add(ac.name)
        result.append(
            {
                "old_name": rc.name,
                "new_name": ac.name,
                "confidence": round(min(score, 1.0), 3),
                "evidence": evidence,
            }
        )
    return result


# --------------------------------------------------------------------------
# Diff
# --------------------------------------------------------------------------


def diff_contracts(
    old: Contract | None,
    new: Contract,
    *,
    old_profiles: dict[str, dict] | None = None,
    new_profiles: dict[str, dict] | None = None,
    rename_threshold: float = RENAME_WEAK_THRESHOLD,
) -> ChangeSet:
    if old is None:
        return ChangeSet(
            old_version=None,
            new_version=new.version,
            changes=[
                Change(
                    kind="contract_created",
                    classification="compatible",
                    severity="info",
                    summary=f"initial contract v{new.version} with {len(new.columns)} columns",
                )
            ],
        )

    cs = ChangeSet(old_version=old.version, new_version=new.version)
    changes = cs.changes
    old_cols = {c.name: c for c in old.columns}
    new_cols = {c.name: c for c in new.columns}

    renames = detect_renames(
        old, new, old_profiles=old_profiles, new_profiles=new_profiles, threshold=rename_threshold
    )
    cs.renames = renames
    renamed_old = {r["old_name"] for r in renames}
    renamed_new = {r["new_name"]: r for r in renames}

    # --- renames ---------------------------------------------------------
    for r in renames:
        rc, nc = old_cols[r["old_name"]], new_cols[r["new_name"]]
        changes.append(
            Change(
                kind="rename_column",
                classification="breaking",
                severity="high",
                summary=f"column {r['old_name']} renamed to {r['new_name']}",
                column=r["new_name"],
                old_column=r["old_name"],
                new_column=r["new_name"],
                reason="rename detected; downstream SQL and feature pipelines reference the old name",
                confidence=r["confidence"],
                evidence=r["evidence"],
                requires_signoff=True,
                requires_parallel=True,
            )
        )
        # A rename is a strong signal that the column changed in place; still
        # diff the surviving constraints so a rename + narrowing is reported as
        # two changes rather than one.
        _diff_column(rc, nc, changes, drop_kind_suppressed=True)

    # --- added / removed -------------------------------------------------
    for name in old_cols:
        if name not in new_cols and name not in renamed_old:
            rc = old_cols[name]
            changes.append(
                Change(
                    kind="drop_column",
                    classification="breaking",
                    severity="critical",
                    summary=f"column {name} removed",
                    column=name,
                    old_column=name,
                    reason="downstream consumers referencing this column will fail",
                    requires_signoff=True,
                    requires_parallel=True,
                )
            )

    for name in new_cols:
        if name not in old_cols and name not in renamed_new:
            nc = new_cols[name]
            if nc.required:
                # Required, new column: existing rows and writers cannot satisfy
                # it without a backfill, whatever constraints it also carries.
                changes.append(
                    Change(
                        kind="add_column",
                        classification="breaking",
                        severity="medium",
                        summary=f"required column {name} added",
                        column=name,
                        new_column=name,
                        after=nc.to_dict(),
                        reason="new NOT NULL column without a default; existing rows and writers cannot satisfy it",
                        requires_signoff=True,
                        requires_parallel=False,
                    )
                )
            else:
                changes.append(
                    Change(
                        kind="add_column",
                        classification="compatible",
                        severity="info",
                        summary=f"column {name} added",
                        column=name,
                        new_column=name,
                        after=nc.to_dict(),
                        reason="additive change; nullable columns do not break existing readers",
                    )
                )

    # --- surviving columns ------------------------------------------------
    for name in new_cols:
        if name in old_cols:
            _diff_column(old_cols[name], new_cols[name], changes)

    # --- table level -------------------------------------------------------
    _diff_table(old, new, changes)

    # Deterministic order for reproducible reports.
    changes.sort(key=lambda c: (_severity_rank(c.severity), c.kind, c.column or ""))
    return cs


def _severity_rank(sev: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}.get(sev, 9)


def _diff_column(old: Column, new: Column, changes: list[Change], *, drop_kind_suppressed: bool = False) -> None:
    name = new.name
    if old.type != new.type:
        widening = _type_widening(old.type, new.type)
        changes.append(
            Change(
                kind="type_change",
                classification="compatible" if widening else "breaking",
                severity="low" if widening else "critical",
                summary=f"column {name} type changed {old.type} -> {new.type}",
                column=name,
                before=old.type,
                after=new.type,
                reason=(
                    "type widening preserves every existing value"
                    if widening
                    else "narrowing or cross-family type change can truncate or reject existing values"
                ),
                requires_signoff=not widening,
                requires_parallel=not widening,
            )
        )

    if old.required != new.required:
        # required=True means NOT NULL
        tightening = new.required and not old.required
        changes.append(
            Change(
                kind="required_change",
                classification="breaking" if tightening else "compatible",
                severity="high" if tightening else "low",
                summary=(
                    f"column {name} became NOT NULL"
                    if tightening
                    else f"column {name} became nullable"
                ),
                column=name,
                before=old.required,
                after=new.required,
                reason="existing NULLs become violations" if tightening else "relaxation",
                requires_signoff=tightening,
                requires_parallel=tightening,
            )
        )

    _diff_enum(old, new, changes)
    _diff_range(old, new, changes)

    if old.unique != new.unique:
        tightening = new.unique and not old.unique
        changes.append(
            Change(
                kind="unique_change",
                classification="breaking" if tightening else "compatible",
                severity="high" if tightening else "low",
                summary=f"column {name} uniqueness {'added' if tightening else 'removed'}",
                column=name,
                before=old.unique,
                after=new.unique,
                reason="existing duplicates become violations" if tightening else "relaxation",
                requires_signoff=tightening,
                requires_parallel=tightening,
            )
        )

    if (old.precision, old.scale) != (new.precision, new.scale) and old.type == new.type == "decimal":
        old_scale = old.scale if old.scale is not None else 0
        new_scale = new.scale if new.scale is not None else 0
        old_prec = old.precision
        new_prec = new.precision
        tightening = new_scale < old_scale or (new_prec is not None and old_prec is not None and new_prec < old_prec)
        changes.append(
            Change(
                kind="precision_change",
                classification="breaking" if tightening else "compatible",
                severity="high" if tightening else "low",
                summary=f"column {name} precision/scale changed ({old_prec},{old_scale}) -> ({new_prec},{new_scale})",
                column=name,
                before=[old_prec, old_scale],
                after=[new_prec, new_scale],
                reason="reduced precision/scale can round or overflow values",
                requires_signoff=tightening,
                requires_parallel=tightening,
            )
        )

    if (old.description or "") != (new.description or ""):
        changes.append(
            Change(
                kind="description_change",
                classification="compatible",
                severity="info",
                summary=f"column {name} description changed",
                column=name,
                before=old.description,
                after=new.description,
            )
        )


def _diff_enum(old: Column, new: Column, changes: list[Change]) -> None:
    if old.enum is None and new.enum is None:
        return
    old_set = set(map(repr, old.enum or []))
    new_set = set(map(repr, new.enum or []))
    if old_set == new_set:
        return
    removed = old_set - new_set
    added = new_set - old_set
    kind = "enum_change"
    name = new.name
    if old.enum is None and new.enum is not None:
        changes.append(
            Change(
                kind=kind,
                classification="breaking",
                severity="high",
                summary=f"column {name} gained an enum constraint",
                column=name,
                before=None,
                after=new.enum,
                reason="existing values outside the enum become violations",
                requires_signoff=True,
                requires_parallel=True,
            )
        )
        return
    if old.enum is not None and new.enum is None:
        changes.append(
            Change(
                kind=kind,
                classification="compatible",
                severity="low",
                summary=f"column {name} dropped its enum constraint",
                column=name,
                before=old.enum,
                after=None,
                reason="relaxation",
            )
        )
        return
    if removed:
        changes.append(
            Change(
                kind=kind,
                classification="breaking",
                severity="high",
                summary=f"column {name} enum removed {len(removed)} value(s)",
                column=name,
                before=sorted(old_set),
                after=sorted(new_set),
                reason=f"removed values still occur in history: {sorted(removed)}",
                requires_signoff=True,
                requires_parallel=True,
            )
        )
    if added:
        changes.append(
            Change(
                kind=kind,
                classification="compatible",
                severity="low",
                summary=f"column {name} enum added {len(added)} value(s)",
                column=name,
                before=sorted(old_set),
                after=sorted(new_set),
                reason="new values are additive for existing consumers",
            )
        )


def _diff_range(old: Column, new: Column, changes: list[Change]) -> None:
    if old.minimum == new.minimum and old.maximum == new.maximum:
        return
    tightening = _range_tightened(old, new)
    changes.append(
        Change(
            kind="range_change",
            classification="breaking" if tightening else "compatible",
            severity="high" if tightening else "low",
            summary=f"column {new.name} range changed [{old.minimum}, {old.maximum}] -> [{new.minimum}, {new.maximum}]",
            column=new.name,
            before=[old.minimum, old.maximum],
            after=[new.minimum, new.maximum],
            reason="existing values fall outside the new bounds" if tightening else "widened bounds",
            requires_signoff=tightening,
            requires_parallel=tightening,
        )
    )


def _range_tightened(old: Column, new: Column) -> bool:
    def num(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        from datetime import date, datetime

        if isinstance(v, datetime):
            return v.timestamp()
        if isinstance(v, date):
            return datetime(v.year, v.month, v.day).timestamp()
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    old_lo, old_hi = num(old.minimum), num(old.maximum)
    new_lo, new_hi = num(new.minimum), num(new.maximum)
    if new_lo is not None and (old_lo is None or new_lo > old_lo):
        return True
    if new_hi is not None and (old_hi is None or new_hi < old_hi):
        return True
    return False


def _diff_table(old: Contract, new: Contract, changes: list[Change]) -> None:
    def _sev(kind: str, summary: str, before: Any, after: Any, reason: str, breaking: bool) -> None:
        changes.append(
            Change(
                kind=kind,
                classification="breaking" if breaking else "compatible",
                severity="critical" if breaking else "low",
                summary=summary,
                before=before,
                after=after,
                reason=reason,
                requires_signoff=breaking,
                requires_parallel=breaking,
            )
        )

    if old.primary_key != new.primary_key:
        _sev(
            "primary_key_change",
            f"primary key changed {old.primary_key or '[]'} -> {new.primary_key or '[]'}",
            old.primary_key,
            new.primary_key,
            "identity semantics change; downstream joins and dedupe depend on the key",
            True,
        )

    old_uk = sorted([sorted(k) for k in old.unique_keys])
    new_uk = sorted([sorted(k) for k in new.unique_keys])
    if old_uk != new_uk:
        tightened = any(k not in old_uk for k in new_uk)
        _sev(
            "unique_keys_change",
            "table unique keys changed",
            old_uk,
            new_uk,
            "new uniqueness constraints can reject existing rows" if tightened else "constraints relaxed",
            tightened,
        )

    old_f = old.freshness
    new_f = new.freshness
    if (old_f.max_age if old_f else None) != (new_f.max_age if new_f else None) or (
        old_f.column if old_f else None
    ) != (new_f.column if new_f else None):
        tightened = _freshness_tightened(old_f, new_f)
        _sev(
            "freshness_change",
            f"freshness changed {old_f.max_age if old_f else None} -> {new_f.max_age if new_f else None}",
            old_f.to_dict() if old_f else None,
            new_f.to_dict() if new_f else None,
            "stricter freshness SLA can break existing producers" if tightened else "relaxed SLA",
            tightened,
        )

    old_rules = {r.name: r for r in old.rules}
    new_rules = {r.name: r for r in new.rules}
    for name, nr in new_rules.items():
        orule = old_rules.get(name)
        if orule is None:
            _rule_change(changes, "rule_added", name, None, nr, new_rule=True)
        elif orule != nr:
            tightened = _rule_tightened(orule, nr)
            _sev(
                "rule_changed",
                f"rule {name} changed",
                orule.to_dict(),
                nr.to_dict(),
                "rule tightened" if tightened else "rule relaxed",
                tightened,
            )
    for name, orule in old_rules.items():
        if name not in new_rules:
            changes.append(
                Change(
                    kind="rule_removed",
                    classification="compatible",
                    severity="low",
                    summary=f"rule {name} removed",
                    before=orule.to_dict(),
                    after=None,
                    reason="rule removal is a relaxation",
                )
            )

    if (old.row_count or None) != (new.row_count or None):
        changes.append(
            Change(
                kind="row_count_change",
                classification="compatible",
                severity="info",
                summary="row count expectation changed",
                before=old.row_count,
                after=new.row_count,
            )
        )

    if old.consumers != new.consumers:
        changes.append(
            Change(
                kind="consumers_change",
                classification="compatible",
                severity="info",
                summary="registered consumers changed",
                before=old.consumers,
                after=new.consumers,
                reason="new consumers must also approve future breaking changes",
            )
        )

    if (old.description or "") != (new.description or ""):
        changes.append(
            Change(
                kind="description_change",
                classification="compatible",
                severity="info",
                summary="contract description changed",
                before=old.description,
                after=new.description,
            )
        )


def _rule_change(changes, kind, name, before, after, *, new_rule: bool) -> None:
    # A new rule constrains data; treat it as breaking because the upstream must
    # now be able to satisfy it.  That is deliberate: rules are part of the deal.
    changes.append(
        Change(
            kind=kind,
            classification="breaking",
            severity="medium",
            summary=f"rule {name} added ({after.kind})",
            before=before,
            after=after.to_dict(),
            reason="new validation rule constrains upstream data",
            requires_signoff=True,
            requires_parallel=True,
        )
    )


_NARROW_RULE_KINDS = {"not_null", "enum", "range", "unique", "expression", "row_count", "null_ratio"}


def _rule_tightened(old: Rule, new: Rule) -> bool:
    if old.kind != new.kind or old.column != new.column:
        return True
    if old.kind == "enum":
        return bool(set(map(repr, old.enum or [])) - set(map(repr, new.enum or [])))
    if old.kind in {"range", "row_count", "null_ratio"}:
        if new.minimum is not None and (old.minimum is None or _num(new.minimum) > _num(old.minimum)):
            return True
        if new.maximum is not None and (old.maximum is None or _num(new.maximum) < _num(old.maximum)):
            return True
        return False
    if old.kind == "expression":
        return old.expression != new.expression
    return True


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _parse_iso_duration(text: str | None) -> float | None:
    """Seconds in a small ISO-8601 subset: PnDTnHnMnS."""
    if not text:
        return None
    import re

    m = re.match(
        r"^P(?:(\d+(?:\.\d+)?)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$",
        text.strip().upper(),
    )
    if not m:
        return None
    d, h, mi, s = (float(x) if x else 0.0 for x in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + s


def _freshness_tightened(old, new) -> bool:
    """A stricter (shorter) max_age or a newly added SLA is breaking."""
    old_age = _parse_iso_duration(old.max_age) if old else None
    new_age = _parse_iso_duration(new.max_age) if new else None
    if old_age is None and new_age is None:
        return False
    if new_age is not None and (old_age is None or new_age < old_age):
        return True
    return False
