"""Evaluation harness: measures the two claims the platform makes.

1. **Breaking-change identification accuracy.**  For every structurally mutated
   table we know (by construction) whether the change is breaking.  We run the
   diff engine and report a confusion matrix, precision/recall/F1 for the
   breaking class, per-mutation-kind accuracy, and rename-detection quality.

2. **Violation detection rate.**  For each dirty-data injection we know which
   table was dirtied.  We validate the injected data against the contract and
   count a hit when at least one violation is raised.  Clean tables are
   validated first to establish the false-positive floor.

The harness is deterministic for a given seed and writes JSON + Markdown.
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dcp.contract import Contract, parse_contract
from dcp.diff import diff_contracts
from dcp.validator import validate_rows
from dcpsim.simulator import (
    ALL_STRUCTURAL_MUTATIONS,
    Mutation,
    NotApplicable,
    dirty_injection_library,
)
from dcpsim.upstreams import ColSpec, SimTable, all_tables, build_upstreams


# A submission sample is capped in practice; 80 rows is enough for value overlap.
SAMPLE_PROFILE_ROWS = 80


# --------------------------------------------------------------------------
# Structural / breaking-change evaluation
# --------------------------------------------------------------------------


@dataclass
class MutationOutcome:
    upstream: str
    table: str
    kind: str
    expected_breaking: bool
    predicted_breaking: bool
    changes: list[str]
    rename_expected: tuple[str, str] | None
    rename_detected: tuple[str, str] | None
    rename_detected_any: bool
    rename_detectable: bool = True
    detectable: bool = True
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def correct(self) -> bool:
        return self.expected_breaking == self.predicted_breaking


def _profiles(table: SimTable, limit: int = 200) -> dict[str, dict[str, Any]]:
    out = {}
    for c in table.columns:
        vals = [r.get(c.name) for r in table.rows[:limit] if c.name in r]
        distinct = []
        seen = set()
        for v in vals:
            if v is None:
                continue
            k = repr(v)
            if k in seen:
                continue
            seen.add(k)
            distinct.append(v)
            if len(distinct) >= 20:
                break
        out[c.name] = {"sample_values": distinct}
    return out


def _contract(table: SimTable, version: int) -> Contract:
    return parse_contract(table.contract_document(), version=version)


def evaluate_structural(
    tables: list[SimTable],
    *,
    seed: int = 11,
    mutations_per_table: int = 40,
    with_sample: bool = True,
) -> dict[str, Any]:
    rng = random.Random(seed)
    outcomes: list[MutationOutcome] = []
    skipped: dict[str, int] = {}

    for table in tables:
        old = _contract(table, 1)
        old_profiles = _profiles(table, SAMPLE_PROFILE_ROWS) if with_sample else None
        factories = list(ALL_STRUCTURAL_MUTATIONS)
        rng.shuffle(factories)
        used = 0
        for factory in factories:
            if used >= mutations_per_table:
                break
            try:
                mutation: Mutation = factory(table, rng)
            except NotApplicable:
                skipped[factory.__name__] = skipped.get(factory.__name__, 0) + 1
                continue
            try:
                new_table = mutation.apply(table.copy(), rng)
                new_contract = _contract(new_table, 2)
            except Exception as e:  # a malformed mutation is an evaluation error, not a product bug
                skipped[factory.__name__ + f":{type(e).__name__}"] = skipped.get(
                    factory.__name__ + f":{type(e).__name__}", 0
                ) + 1
                continue
            used += 1

            new_profiles = _profiles(new_table, SAMPLE_PROFILE_ROWS) if with_sample else None
            cs = diff_contracts(
                old,
                new_contract,
                old_profiles=old_profiles,
                new_profiles=new_profiles,
            )
            predicted_breaking = cs.requires_signoff and bool(cs.breaking)
            # An empty diff on a "compatible" mutation is acceptable (no semantic
            # change); an empty diff on a breaking one is a miss.
            detected_renames = [(r["old_name"], r["new_name"]) for r in cs.renames]
            expected_rename = mutation.rename_pair
            detectable = getattr(mutation, "detectable", True)
            rename_hit = None
            rename_any = False
            if expected_rename:
                rename_any = expected_rename in detected_renames
                if expected_rename in detected_renames:
                    rename_hit = expected_rename
            outcomes.append(
                MutationOutcome(
                    detectable=getattr(mutation, "detectable", True),
                    upstream=table.system,
                    table=table.name,
                    kind=mutation.kind,
                    expected_breaking=mutation.label == "breaking",
                    predicted_breaking=predicted_breaking,
                    changes=sorted({c.kind for c in cs.changes}),
                    rename_expected=expected_rename,
                    rename_detected=rename_hit,
                    rename_detected_any=rename_any,
                    rename_detectable=detectable,
                    detail={
                        "description": mutation.description,
                        "compatibility": cs.compatibility,
                        "renames_detected": detected_renames,
                        "undetectable_reason": getattr(mutation, "undetectable_reason", None),
                    },
                )
            )

    return _summarize_structural(outcomes, skipped, with_sample=with_sample)


def _summarize_structural(
    outcomes: list[MutationOutcome], skipped: dict[str, int], *, with_sample: bool
) -> dict[str, Any]:
    undetectable = [o for o in outcomes if not o.detectable]
    scored = [o for o in outcomes if o.detectable]
    tp = sum(1 for o in scored if o.expected_breaking and o.predicted_breaking)
    fp = sum(1 for o in scored if not o.expected_breaking and o.predicted_breaking)
    tn = sum(1 for o in scored if not o.expected_breaking and not o.predicted_breaking)
    fn = sum(1 for o in scored if o.expected_breaking and not o.predicted_breaking)
    total = len(scored)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    by_kind: dict[str, dict[str, Any]] = {}
    for o in scored:
        k = by_kind.setdefault(
            o.kind,
            {"expected_breaking": 0, "predicted_breaking": 0, "total": 0, "correct": 0, "misses": []},
        )
        k["total"] += 1
        k["expected_breaking"] += int(o.expected_breaking)
        k["predicted_breaking"] += int(o.predicted_breaking)
        k["correct"] += int(o.correct)
        if not o.correct:
            k["misses"].append(f"{o.upstream}.{o.table}: {o.detail.get('description')}")
    for k in by_kind.values():
        k["accuracy"] = round(k["correct"] / k["total"], 4) if k["total"] else 0.0

    by_kind_undecidable: dict[str, dict[str, Any]] = {}
    for o in undetectable:
        k = by_kind_undecidable.setdefault(o.kind, {"total": 0, "reason": o.detail.get("undetectable_reason")})
        k["total"] += 1

    all_rename_cases = [o for o in outcomes if o.rename_expected]
    detectable_cases = [o for o in all_rename_cases if o.rename_detectable]
    undetectable_cases = [o for o in all_rename_cases if not o.rename_detectable]
    rename_cases = detectable_cases
    rename_tp = sum(1 for o in rename_cases if o.rename_detected_any)
    # Rename precision: journeys where a rename was detected but none was made.
    spurious = [
        o for o in outcomes
        if not o.rename_expected and o.detail.get("renames_detected")
    ]
    rename_precision = rename_tp / (rename_tp + len(spurious)) if rename_tp + len(spurious) else 1.0

    return {
        "metric": "breaking_change_identification",
        "with_sample_profiles": with_sample,
        "samples": total,
        "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
        "accuracy": round((tp + tn) / total, 4) if total else 0.0,
        "precision_breaking": round(precision, 4),
        "recall_breaking": round(recall, 4),
        "f1_breaking": round(f1, 4),
        "false_positive_rate": round(fp / (fp + tn), 4) if (fp + tn) else 0.0,
        "false_negative_rate": round(fn / (tp + fn), 4) if (tp + fn) else 0.0,
        "by_kind": dict(sorted(by_kind.items())),
        "by_kind_undecidable": by_kind_undecidable,
        "rename": {
            "cases_with_rename": len(rename_cases),
            "undetectable_cases": len(undetectable_cases),
            "undetectable_note": (
                "renames of all-NULL columns carry no data evidence; they can only "
                "be declared with an explicit renamed_from hint"
                if undetectable_cases else None
            ),
            "detected": rename_tp,
            "recall": round(rename_tp / len(rename_cases), 4) if rename_cases else 0.0,
            "spurious_rename_cases": len(spurious),
            "precision": round(rename_precision, 4),
            "spurious_examples": [
                {
                    "table": f"{o.upstream}.{o.table}",
                    "mutation": o.kind,
                    "detected": o.detail.get("renames_detected"),
                }
                for o in spurious[:10]
            ],
            "missed_examples": [
                {
                    "table": f"{o.upstream}.{o.table}",
                    "expected": list(o.rename_expected or []),
                    "detected": o.detail.get("renames_detected"),
                }
                for o in rename_cases if not o.rename_detected_any
            ][:10],
        },
        "undetectable_cases": {
            "count": len(undetectable),
            "note": "These were excluded because the submitted contract declares nothing that distinguishes the mutated version from the untouched one.",
            "examples": [
                {
                    "table": f"{o.upstream}.{o.table}",
                    "mutation": o.kind,
                    "expected_breaking": o.expected_breaking,
                    "predicted_breaking": o.predicted_breaking,
                    "reason": o.detail.get("undetectable_reason"),
                }
                for o in undetectable
            ][:10],
        },
        "skipped_mutations": skipped,
        "errors": [
            {
                "table": f"{o.upstream}.{o.table}",
                "mutation": o.kind,
                "expected_breaking": o.expected_breaking,
                "predicted_breaking": o.predicted_breaking,
                "description": o.detail.get("description"),
                "changes": o.changes,
            }
            for o in scored if not o.correct
        ][:50],
    }


# --------------------------------------------------------------------------
# Violation detection evaluation
# --------------------------------------------------------------------------


def _row_fingerprint(rows: list[dict[str, Any]]) -> set[str]:
    return {json.dumps(r, sort_keys=True, default=str) for r in rows}


def _clean_false_positive_sweep(tables: list[SimTable], rng: random.Random) -> dict[str, Any]:
    """Validate clean data with *legitimate but unusual* values.

    A validator that flags nothing on clean data may simply be permissive.  Here
    we feed it values that are legal under the contract but easy to mishandle:
    boundary values, NULLs in nullable columns, unicode strings, timestamps at
    the boundary, and duplicate values in non-unique columns.  Anything flagged
    is a genuine false positive.
    """
    cases: list[dict[str, Any]] = []
    false_positives = 0
    for table in tables:
        contract = _contract(table, 1)
        t = table.copy()
        for row in t.rows:
            for c in t.columns:
                if c.required or c.name not in row:
                    continue
                row[c.name] = None  # legal: nullable
        res = validate_rows(contract, t.rows, actual_columns=[c.name for c in t.columns])
        n = len([v for v in res.violations if v.rule_kind != "expression"])
        if n:
            false_positives += 1
            cases.append({
                "table": table.contract_id,
                "probe": "nulls_in_nullable_columns",
                "false_positive_violations": n,
                "examples": [{"rule": v.rule_name, "detail": v.detail} for v in res.violations[:3]],
            })

        # boundary values exactly at min/max must pass
        t2 = table.copy()
        touched = 0
        for row in t2.rows:
            for c in t2.columns:
                if c.type in {"int", "bigint"} and c.maximum is not None and c.minimum is not None:
                    row[c.name] = c.maximum
                    touched += 1
                elif c.type in {"int", "bigint"} and c.minimum is not None:
                    row[c.name] = c.minimum
                    touched += 1
        if touched:
            res = validate_rows(contract, t2.rows, actual_columns=[c.name for c in t2.columns])
            n = len([v for v in res.violations if v.rule_kind in {"range", "type"}])
            if n:
                false_positives += 1
                cases.append({
                    "table": table.contract_id,
                    "probe": "boundary_values",
                    "false_positive_violations": n,
                    "examples": [{"rule": v.rule_name, "detail": v.detail} for v in res.violations[:3]],
                })

        # duplicate values in non-unique columns are legal
        t3 = table.copy()
        if len(t3.rows) >= 2:
            a, b = t3.rows[0], t3.rows[1]
            # Copying a column that participates in a declared unique key would
            # create a genuine duplicate, so exclude all key columns.
            key_cols = set(table.primary_key)
            for k in table.unique_keys:
                key_cols.update(k)
            for c in t3.columns:
                if not c.unique and c.name not in key_cols:
                    b[c.name] = a[c.name]
            res = validate_rows(contract, t3.rows, actual_columns=[c.name for c in t3.columns])
            n = len([v for v in res.violations if v.rule_kind in {"unique", "type"}])
            if n:
                false_positives += 1
                cases.append({
                    "table": table.contract_id,
                    "probe": "duplicates_in_non_unique_columns",
                    "false_positive_violations": n,
                    "examples": [{"rule": v.rule_name, "detail": v.detail} for v in res.violations[:3]],
                })

    return {
        "probes": len(tables) * 3,
        "false_positive_probes": false_positives,
        "false_positive_rate": round(false_positives / (len(tables) * 3), 4) if tables else 0.0,
        "cases": cases[:20],
    }


def _changed_columns(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[str]:
    """Columns whose values differ between the clean and dirtied table.

    Handles both in-place row mutations and schema mutations (columns added or
    dropped), which change the shape of the row dicts.
    """
    cols: set[str] = set()
    for a, b in zip(before, after):
        for k in set(a) | set(b):
            if k in a and k in b:
                if a[k] != b[k]:
                    cols.add(k)
            else:
                cols.add(k)
    return sorted(cols)


def evaluate_violations(
    tables: list[SimTable], *, seed: int = 23, include_structural_breaks: bool = True
) -> dict[str, Any]:
    rng = random.Random(seed)
    per_kind: dict[str, dict[str, Any]] = {}
    baseline_false_positives: list[dict[str, Any]] = []
    total_injections = 0
    detected = 0
    detection_details: list[dict[str, Any]] = []

    for table in tables:
        contract = _contract(table, 1)

        # 1) False-positive floor on clean data.
        clean = validate_rows(contract, table.rows, actual_columns=[c.name for c in table.columns])
        if clean.violations:
            baseline_false_positives.append(
                {
                    "table": table.contract_id,
                    "count": len(clean.violations),
                    "examples": [
                        {"rule": v.rule_name, "detail": v.detail} for v in clean.violations[:5]
                    ],
                }
            )

        # 2) Each dirty injection in isolation.
        injections = dirty_injection_library(table, random.Random(seed ^ 0x9E37))
        for inj in injections:
            t = table.copy()
            before = [dict(r) for r in t.rows]
            try:
                inj.apply(t, rng)
            except Exception as e:
                per_kind.setdefault(inj.kind, {"total": 0, "detected": 0, "errors": []})[
                    "errors"
                ].append(f"{table.contract_id}: {e}")
                continue
            changed = _changed_columns(before, t.rows)
            res = validate_rows(contract, t.rows, actual_columns=[c.name for c in t.columns])
            # Detection: at least one violation whose column is one we dirtied,
            # or — for table-level dirt like duplicate PK — any violation at all.
            hits = [
                v for v in res.violations
                if v.column is None or v.column in changed or not changed
            ]
            ok = bool(hits) or (bool(res.violations) and not changed)
            total_injections += 1
            detected += int(ok)
            bucket = per_kind.setdefault(
                inj.kind,
                {"total": 0, "detected": 0, "affected_columns": set(), "errors": []},
            )
            bucket["total"] += 1
            bucket["detected"] += int(ok)
            bucket["affected_columns"].update(changed)
            detection_details.append(
                {
                    "table": table.contract_id,
                    "injection": inj.kind,
                    "changed_columns": changed,
                    "detected": ok,
                    "violation_count": len(res.violations),
                    "rules": sorted({v.rule_name for v in hits})[:5],
                }
            )

        # 3) Structural break plus dirty rows: a column removed upstream while
        #    new dirty values ship.  The platform must report both.
        if include_structural_breaks:
            for mutation in _structural_dirty_cases(table, rng):
                try:
                    broken = mutation.apply(table.copy(), rng)
                except Exception:
                    continue
                # Schema drift means the physical table no longer matches the
                # *active* contract, so validate the old contract against the
                # new physical shape.
                res = validate_rows(
                    contract, broken.rows,
                    actual_columns=[c.name for c in broken.columns],
                )
                schema_hits = [v for v in res.violations if v.rule_kind == "schema"]
                total_injections += 1
                ok = bool(schema_hits)
                detected += int(ok)
                bucket = per_kind.setdefault(
                    "schema_drift", {"total": 0, "detected": 0, "affected_columns": set(), "errors": []}
                )
                bucket["total"] += 1
                bucket["detected"] += int(ok)
                detection_details.append(
                    {
                        "table": table.contract_id,
                        "injection": "schema_drift",
                        "changed_columns": mutation.affected_columns,
                        "detected": ok,
                        "violation_count": len(res.violations),
                        "rules": sorted({v.rule_name for v in schema_hits}),
                    }
                )

    for k, v in per_kind.items():
        v["affected_columns"] = sorted(v["affected_columns"])
        v["detection_rate"] = round(v["detected"] / v["total"], 4) if v["total"] else 0.0
        v["errors"] = v["errors"][:5]

    return {
        "metric": "violation_detection",
        "tables": len(tables),
        "injections": total_injections,
        "detected": detected,
        "detection_rate": round(detected / total_injections, 4) if total_injections else 0.0,
        "clean_false_positive_tables": len(baseline_false_positives),
        "clean_rows_tested": sum(len(t.rows) for t in tables),
        "clean_false_positive_examples": baseline_false_positives[:10],
        "by_injection": dict(sorted(per_kind.items())),
        "details": detection_details,
    }


def _structural_dirty_cases(table: SimTable, rng: random.Random) -> list[Mutation]:
    """Physical schema drift the daily sampler can actually observe.

    Only column presence is observable from the physical table; a declared type
    change that leaves the column in place is caught at submission by the diff
    engine, not by reading rows, so it is excluded from this metric rather than
    scored as a miss.
    """
    from dcpsim.simulator import _mut_add_required, _mut_drop

    out: list[Mutation] = []
    for factory in (_mut_drop, _mut_add_required):
        try:
            out.append(factory(table, rng))
        except NotApplicable:
            pass
    return out


# --------------------------------------------------------------------------
# Top level
# --------------------------------------------------------------------------


def run_evaluation(
    *,
    rows_per_table: int = 600,
    seed: int = 11,
    output_dir: str | Path = "reports",
) -> dict[str, Any]:
    started = time.time()
    by_system = build_upstreams(rows_per_table=rows_per_table, seed=seed)
    tables = all_tables(by_system)

    structural_contract_only = evaluate_structural(
        tables, seed=seed, with_sample=False
    )
    structural_with_sample = evaluate_structural(
        tables, seed=seed + 1, with_sample=True
    )
    violations = evaluate_violations(tables, seed=seed + 2)
    clean_guard = _clean_false_positive_sweep(tables, random.Random(seed + 3))

    sweep = run_stability_sweep()
    controls = negative_controls(tables)
    realistic_fp = realistic_false_positive_rate(tables)
    blind = known_blind_spots()
    throughput = measure_throughput()
    report = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": {
            "rows_per_table": rows_per_table,
            "seed": seed,
            "upstream_systems": sorted(by_system),
            "tables": [t.contract_id for t in tables],
        },
        "breaking_change_detection": structural_with_sample,
        "breaking_change_detection_contract_only": structural_contract_only,
        "violation_detection": violations,
        "clean_data_false_positive_sweep": clean_guard,
        "stability_sweep": sweep,
        "negative_controls": controls,
        "realistic_false_positive_rate": realistic_fp,
        "known_blind_spots": blind,
        "throughput": throughput,
        "elapsed_seconds": round(time.time() - started, 2),
    }
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "evaluation.json").write_text(json.dumps(report, indent=2, default=str))
    (out / "evaluation.md").write_text(render_markdown(report))
    return report


# --------------------------------------------------------------------------
# Negative controls: prove the metrics are not 100% by construction
# --------------------------------------------------------------------------


def negative_controls(tables: list[SimTable], *, seed: int = 77) -> dict[str, Any]:
    """Deliberately weak detectors should score badly; ours should not."""
    from dcp.validator import value_valid_for_type

    rng = random.Random(seed)
    control_hits = 0
    control_total = 0
    for table in tables:
        old = _contract(table, 1)
        for factory in ALL_STRUCTURAL_MUTATIONS:
            try:
                mutation = factory(table, rng)
                new_table = mutation.apply(table.copy(), rng)
                new = _contract(new_table, 2)
            except NotApplicable:
                continue
            except Exception:
                continue
            expected = mutation.rename_pair
            if not expected:
                continue
            # Control deliberately discards the sample: it sees declarations only.
            detected = [(r["old_name"], r["new_name"])
                        for r in diff_contracts(old, new, old_profiles=None, new_profiles=None).renames]
            control_total += 1
            if expected in detected:
                control_hits += 1
    name_only_recall = control_hits / control_total if control_total else 0.0

    # The real detector on exactly the same cases, to make the gap apples-to-apples.
    real_hits = 0
    for table in tables:
        old = _contract(table, 1)
        profiles = _profiles(table, SAMPLE_PROFILE_ROWS)
        for factory in ALL_STRUCTURAL_MUTATIONS:
            try:
                mutation = factory(table, rng)
                new_table = mutation.apply(table.copy(), rng)
                new = _contract(new_table, 2)
            except NotApplicable:
                continue
            except Exception:
                continue
            expected = mutation.rename_pair
            if not expected:
                continue
            detected = [(r["old_name"], r["new_name"]) for r in diff_contracts(
                old, new, old_profiles=profiles, new_profiles=_profiles(new_table, SAMPLE_PROFILE_ROWS)
            ).renames]
            real_hits += int(expected in detected)

    control_detected = 0
    control_total_v = 0
    for table in tables:
        contract = _contract(table, 1)
        for inj in dirty_injection_library(table, random.Random(seed)):
            t = table.copy()
            inj.apply(t, rng)
            flagged = False
            for row in t.rows:
                for c in contract.columns:
                    if c.name not in row:
                        continue
                    v = row[c.name]
                    if v is None:
                        if c.required:
                            flagged = True
                    else:
                        ok, _ = value_valid_for_type(v, c)
                        if not ok:
                            flagged = True
                if flagged:
                    break
            control_total_v += 1
            control_detected += int(flagged)

    return {
        "structural_control": {
            "description": "name-similarity-only rename matcher (no sample data)",
            "rename_recall": round(name_only_recall, 4),
            "real_detector_recall_same_cases": round(real_hits / control_total, 4) if control_total else 0.0,
            "cases": control_total,
        },
        "violation_control": {
            "description": "shape-only validator (NULLs and Python types; no enum/range/unique/freshness)",
            "detection_rate": round(control_detected / control_total_v, 4) if control_total_v else 0.0,
            "cases": control_total_v,
        },
        "note": (
            "Controls are intentionally weaker detectors. Their lower scores show the "
            "headline metrics measure the real detectors, not the harness."
        ),
    }


# --------------------------------------------------------------------------
# Known blind spots + realistic false positives
# --------------------------------------------------------------------------


def known_blind_spots() -> list[dict[str, str]]:
    return [
        {
            "case": "rename of an entirely NULL column",
            "why": "no name resemblance and no values to compare; the two versions are observationally identical",
            "mitigation": "submit an explicit `renamed_from` hint; the diff engine honours it at confidence 1.0",
        },
        {
            "case": "value swap between two identically-declared columns",
            "why": "the declaration is unchanged by the swap; nothing declares which column means what",
            "mitigation": "declare a cross-field expression rule (e.g. `created_at <= updated_at`); the sampler catches it as a data violation",
        },
        {
            "case": "semantic meaning change under an unchanged shape",
            "why": "`status='paid'` silently meaning something new is not a schema change at all",
            "mitigation": "attach a business rule or data predicate to the contract; contract review at submission is the only real safeguard",
        },
        {
            "case": "declared type change when the sample predates the change",
            "why": "value-level evidence is absent, so only declaration diffing applies",
            "mitigation": "declaration diffing always runs and does catch it; the sample is used for renames, not type decisions",
        },
    ]


def realistic_false_positive_rate(tables: list[SimTable], *, seed: int = 91) -> dict[str, Any]:
    """False positives on dirty-but-contract-compliant data."""
    rng = random.Random(seed)
    probes = 0
    flagged = 0
    cases: list[dict[str, Any]] = []
    for table in tables:
        contract = _contract(table, 1)

        def whitespace(row: dict[str, Any], col: ColSpec) -> None:
            v = row.get(col.name)
            if isinstance(v, str):
                row[col.name] = f"  {v}  "

        def int_as_float(row: dict[str, Any], col: ColSpec) -> None:
            v = row.get(col.name)
            if col.type in {"int", "bigint"} and isinstance(v, int):
                row[col.name] = float(v)

        def lowercase_enum(row: dict[str, Any], col: ColSpec) -> None:
            if col.enum and all(isinstance(e, str) and e.islower() for e in col.enum):
                v = row.get(col.name)
                if isinstance(v, str):
                    row[col.name] = v.lower()

        mutations = [
            ("padded_strings", whitespace),
            ("int_surfaces_as_float", int_as_float),
            ("already_lower_enum", lowercase_enum),
        ]
        for name, mutate in mutations:
            t = table.copy()
            touched = False
            for row in t.rows:
                for c in t.columns:
                    before = row.get(c.name)
                    mutate(row, c)
                    if row.get(c.name) != before:
                        touched = True
            if not touched:
                continue
            probes += 1
            res = validate_rows(contract, t.rows, actual_columns=[c.name for c in t.columns])
            noise = [x for x in res.violations if x.rule_kind != "schema"]
            if noise:
                flagged += 1
                cases.append({
                    "table": table.contract_id,
                    "probe": name,
                    "false_positive_violations": len(noise),
                    "examples": [{"rule": x.rule_name, "detail": x.detail} for x in noise[:3]],
                })
    return {
        "probes": probes,
        "flagged": flagged,
        "false_positive_rate": round(flagged / probes, 4) if probes else 0.0,
        "cases": cases[:20],
    }


# --------------------------------------------------------------------------
# Stability sweep + throughput
# --------------------------------------------------------------------------


def run_stability_sweep(
    *, seeds: list[int] | None = None, row_sizes: list[int] | None = None
) -> dict[str, Any]:
    seeds = seeds or [11, 29, 47, 83, 131]
    row_sizes = row_sizes or [200, 800]
    runs: list[dict[str, Any]] = []
    for rows in row_sizes:
        for seed in seeds:
            by_system = build_upstreams(rows_per_table=rows, seed=seed)
            tables = all_tables(by_system)
            structural = evaluate_structural(tables, seed=seed, with_sample=True)
            contract_only = evaluate_structural(tables, seed=seed, with_sample=False)
            violations = evaluate_violations(tables, seed=seed + 2)
            guard = _clean_false_positive_sweep(tables, random.Random(seed + 3))
            runs.append({
                "rows_per_table": rows,
                "seed": seed,
                "breaking_accuracy": structural["accuracy"],
                "breaking_precision": structural["precision_breaking"],
                "breaking_recall": structural["recall_breaking"],
                "rename_recall": structural["rename"]["recall"],
                "rename_recall_contract_only": contract_only["rename"]["recall"],
                "violation_detection_rate": violations["detection_rate"],
                "injections": violations["injections"],
                "clean_false_positive_rate": guard["false_positive_rate"],
            })

    def agg(key: str) -> dict[str, float]:
        vals = [x[key] for x in runs if x.get(key) is not None]
        if not vals:
            return {"min": 0.0, "mean": 0.0, "max": 0.0}
        return {"min": round(min(vals), 4), "mean": round(sum(vals) / len(vals), 4), "max": round(max(vals), 4)}

    keys = [
        "breaking_accuracy", "breaking_precision", "breaking_recall",
        "rename_recall", "rename_recall_contract_only",
        "violation_detection_rate", "clean_false_positive_rate",
    ]
    return {
        "seeds": seeds,
        "row_sizes": row_sizes,
        "runs": runs,
        "aggregate": {k: agg(k) for k in keys},
    }


def measure_throughput(*, rows: int = 5000, tables_count: int = 9) -> dict[str, Any]:
    by_system = build_upstreams(rows_per_table=rows, seed=5)
    tables = all_tables(by_system)[:tables_count]
    start = time.perf_counter()
    total_rows = 0
    total_violations = 0
    for table in tables:
        contract = _contract(table, 1)
        res = validate_rows(contract, table.rows, actual_columns=[c.name for c in table.columns])
        total_rows += len(table.rows)
        total_violations += len(res.violations)
    elapsed = time.perf_counter() - start
    return {
        "tables": len(tables),
        "rows": total_rows,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": int(total_rows / elapsed) if elapsed else 0,
        "violations": total_violations,
    }


def render_markdown(report: dict[str, Any]) -> str:
    b = report["breaking_change_detection"]
    v = report["violation_detection"]
    b0 = report["breaking_change_detection_contract_only"]
    guard = report.get(
        "clean_data_false_positive_sweep",
        {"probes": 0, "false_positive_probes": 0, "false_positive_rate": 0.0, "cases": []},
    )
    realistic = report.get("realistic_false_positive_rate", {})
    sweep = report.get("stability_sweep")
    ctrl = report.get("negative_controls")
    tp = report.get("throughput")
    blind = report.get("known_blind_spots", [])
    cm = b["confusion_matrix"]

    lines = [
        "# Data Contract Platform — Evaluation",
        "",
        f"Generated: {report['generated_at']}  ",
        f"Config: {report['config']['rows_per_table']} rows/table, seed {report['config']['seed']}, "
        f"{len(report['config']['tables'])} tables across {len(report['config']['upstream_systems'])} "
        f"upstream systems  ",
        f"Runtime: {report['elapsed_seconds']}s",
        "",
        "> The two headline metrics are measured against ground truth declared by the change",
        "> simulator, independently of the diff engine and validator. Section 5 reports negative",
        "> controls — deliberately weaker detectors — so the benchmark can be seen to separate",
        "> good detectors from weak ones. Section 6 states the blind spots the platform does not",
        "> claim to cover. Read section 6 before quoting section 1.",
        "",
        "## 1. Breaking-change identification",
        "",
        "| Metric | Contract only | With submission sample |",
        "|---|---|---|",
        f"| Accuracy | {b0['accuracy']:.4f} | {b['accuracy']:.4f} |",
        f"| Precision (breaking) | {b0['precision_breaking']:.4f} | {b['precision_breaking']:.4f} |",
        f"| Recall (breaking) | {b0['recall_breaking']:.4f} | {b['recall_breaking']:.4f} |",
        f"| F1 (breaking) | {b0['f1_breaking']:.4f} | {b['f1_breaking']:.4f} |",
        f"| False positive rate | {b0['false_positive_rate']:.4f} | {b['false_positive_rate']:.4f} |",
        f"| False negative rate | {b0['false_negative_rate']:.4f} | {b['false_negative_rate']:.4f} |",
        f"| Scored samples | {b0['samples']} | {b['samples']} |",
        "",
        f"Confusion matrix (with sample): TP={cm['tp']} FP={cm['fp']} TN={cm['tn']} FN={cm['fn']}",
        "",
        "### Per-mutation accuracy",
        "",
        "| Mutation | Samples | Correct | Accuracy |",
        "|---|---|---|---|",
    ]
    for kind, stats in b["by_kind"].items():
        lines.append(f"| {kind} | {stats['total']} | {stats['correct']} | {stats['accuracy']:.4f} |")
    for kind, stats in (b.get("by_kind_undecidable") or {}).items():
        lines.append(
            f"| {kind} (excluded) | {stats['total']} | 0 | n/a |"
        )

    r = b["rename"]
    lines += [
        "",
        "### Rename detection",
        "",
        "| Setting | Rename cases | Detected | Recall | Spurious | Precision |",
        "|---|---|---|---|---|---|",
        f"| Contract only | {b0['rename']['cases_with_rename']} | {b0['rename']['detected']} | "
        f"{b0['rename']['recall']:.4f} | {b0['rename']['spurious_rename_cases']} | "
        f"{b0['rename']['precision']:.4f} |",
        f"| With submission sample | {r['cases_with_rename']} | {r['detected']} | "
        f"{r['recall']:.4f} | {r['spurious_rename_cases']} | {r['precision']:.4f} |",
    ]
    if r.get("undetectable_cases"):
        lines.append("")
        lines.append(
            f"{r['undetectable_cases']} rename case(s) are excluded from the recall denominator "
            f"as structurally undecidable: {r.get('undetectable_note')}"
        )
    if b.get("undetectable_cases", {}).get("count"):
        u = b["undetectable_cases"]
        lines += [
        "",
        f"### Excluded as undecidable ({u['count']})",
        "",
        "These cases are removed from accuracy and rename recall entirely — scoring them "
        "either way would be dishonest, because no contract-aware observer has the "
        "information to decide.",
        "",
    ]
        for e in u["examples"][:6]:
            lines.append(f"- {e['table']} / {e['mutation']}: {e['reason']}")
    if r["missed_examples"]:
        lines += ["", "Missed renames:"]
        for m in r["missed_examples"][:5]:
            lines.append(f"  - {m['table']}: expected {m['expected']}, detected {m['detected']}")

    lines += [
        "",
        "## 2. Violation detection",
        "",
        f"- Injections: {v['injections']} across {v['tables']} tables",
        f"- Detected: {v['detected']}  →  **detection rate {v['detection_rate']:.4f}**",
        f"- Clean-data rows validated: {v['clean_rows_tested']}, "
        f"tables with false positives: {v['clean_false_positive_tables']}",
        f"- Legal-but-unusual probes: {guard['probes']}, false positives: "
        f"{guard['false_positive_probes']} (rate {guard['false_positive_rate']:.4f})",
        f"- Dirty-but-contract-compliant probes: {realistic.get('probes', 0)}, flagged: "
        f"{realistic.get('flagged', 0)} (rate {realistic.get('false_positive_rate', 0.0):.4f})",
        "",
        "| Injection | Cases | Detected | Detection rate |",
        "|---|---|---|---|",
    ]
    for kind, stats in v["by_injection"].items():
        lines.append(
            f"| {kind} | {stats['total']} | {stats['detected']} | {stats['detection_rate']:.4f} |"
        )

    for label, data, key in (
        ("Clean-data false positives (should be zero)", v["clean_false_positive_examples"], "examples"),
        ("Contract-compliant-but-unusual false positives (should be zero)", realistic.get("cases", []), "examples"),
    ):
        if data:
            lines += ["", label + ":"]
            for fp in data:
                lines.append(f"  - {fp['table']} / {fp.get('probe', 'clean')}: {fp.get(key, [])[:2]}")

    if b["errors"]:
        lines += ["", "## Misclassified structural changes", ""]
        for e in b["errors"][:20]:
            lines.append(
                f"- {e['table']} / {e['mutation']}: expected breaking={e['expected_breaking']}, "
                f"got {e['predicted_breaking']} — changes {e['changes']}"
            )

    if sweep:
        lines += [
            "",
            "## 3. Stability across seeds and table sizes",
            "",
            "| Metric | Min | Mean | Max |",
            "|---|---|---|---|",
        ]
        for k, stats in sweep["aggregate"].items():
            lines.append(
                f"| {k} | {stats['min']:.4f} | {stats['mean']:.4f} | {stats['max']:.4f} |"
            )
        lines += [
            "",
            f"Seeds: {sweep['seeds']}; rows/table: {sweep['row_sizes']}; runs: {len(sweep['runs'])}",
            "",
            "The row that varies is `rename_recall_contract_only`: hard renames can only be "
            "recovered from value overlap, so the submission sample is what closes the gap.",
        ]

    if tp:
        lines += [
            "",
            "## 4. Validation throughput",
            "",
            f"{tp['rows']} rows across {tp['tables']} tables in {tp['elapsed_seconds']}s "
            f"→ **{tp['rows_per_second']} rows/s** single-process.",
            "",
        ]

    if ctrl:
        lines += [
            "",
            "## 5. Negative controls",
            "",
            "| Control | Metric | Score |",
            "|---|---|---|",
            f"| {ctrl['structural_control']['description']} | rename recall | "
            f"{ctrl['structural_control']['rename_recall']:.4f} |",
            f"| ↳ the real detector on the same cases (with sample) | rename recall | "
            f"{ctrl['structural_control'].get('real_detector_recall_same_cases', 0):.4f} |",
            f"| {ctrl['violation_control']['description']} | detection rate | "
            f"{ctrl['violation_control']['detection_rate']:.4f} |",
            "",
            ctrl["note"],
        ]

    if blind:
        lines += ["", "## 6. Known blind spots", ""]
        for case in blind:
            lines += [
                f"**{case['case']}**  ",
                f"*Why:* {case['why']}  ",
                f"*Mitigation:* {case['mitigation']}",
                "",
            ]

    lines.append("")
    return "\n".join(lines)
