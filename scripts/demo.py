"""Curated end-to-end demo: one upstream change of every interesting kind.

Run after ``python -m dcp.cli seed``.  Prints a narrative so a human can follow
what each subsystem did, and leaves the platform in a state with a pending
sign-off, a parallel rollout, violations and deduplicated alerts.
"""

from __future__ import annotations

import json
import sys
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from dcp.contract import parse_contract
from dcp.platform import Platform, PlatformError, utcnow
from dcp.store import Store
from dcpsim.simulator import (
    NotApplicable,
    _mut_add_nullable,
    _mut_drop,
    _mut_enum_tighten,
    _mut_noop,
    _mut_range_tighten,
    _mut_rename_hard,
    _mut_type_widen,
    dirty_injection_library,
)
from dcpsim.upstreams import materialize, physical_schema, read_table


def _table_from_live(store: Store, system: str, table_name: str):
    from dcpsim.upstreams import ColSpec, SimTable

    ups = store.upstream_table(system, table_name)
    if not ups:
        raise SystemExit(f"{system}.{table_name} not registered")
    cols, rows = read_table(ups["db_path"], table_name)
    phys = physical_schema(ups["db_path"], table_name)
    latest = store.latest_version(f"{system}.{table_name}")
    doc = json.loads(latest["document"])
    sim_cols = [
        ColSpec(
            name=c["name"],
            type=c["type"],
            sql_type=phys.get(c["name"], c.get("sql_type", "TEXT")),
            required=c.get("required", True),
            enum=c.get("enum"),
            minimum=c.get("minimum"),
            maximum=c.get("maximum"),
            unique=c.get("unique", False),
            precision=c.get("precision"),
            scale=c.get("scale"),
            gen="text",
        )
        for c in doc["columns"]
    ]
    return SimTable(
        system=system,
        name=table_name,
        columns=sim_cols,
        primary_key=doc.get("primary_key", []),
        unique_keys=doc.get("unique_keys", []),
        freshness=doc.get("freshness"),
        rules=doc.get("rules", []),
        consumers=doc.get("consumers", []),
        owners=doc.get("owners", []),
        rows=rows,
    ), ups["db_path"]


def _banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def _target_column(old, new):
    """The numeric column whose maximum shrank."""
    for c in new.columns:
        o = old.column(c.name)
        if o is not None and o.maximum is not None and c.maximum is not None and c.maximum < o.maximum:
            return c.name
    return new.columns[0].name


def run_demo(db_path: str = "data/dcp.db") -> dict[str, Any]:
    store = Store(db_path)
    platform = Platform(store)
    rng = random.Random(7)
    out: dict[str, Any] = {"steps": []}

    # ---- 1. Compatible change: nullable column added, auto-released ----
    _banner("1. Compatible change — add a nullable column to order_core.orders")
    sim, db = _table_from_live(store, "order_core", "orders")
    mut = _mut_add_nullable(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=new_table.rows[:40], submitted_by="order_core-service")
    print(f"   {mut.description}\n   -> {res['compatibility']} / {res['status']} (no sign-off needed)")
    out["steps"].append({"step": "compatible_add", "result": res["status"], "compatibility": res["compatibility"]})

    # ---- 2. Type widening: compatible ----
    _banner("2. Compatible change — widen an int column to bigint on risk_engine.risk_events")
    sim, db = _table_from_live(store, "risk_engine", "risk_events")
    mut = _mut_type_widen(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=new_table.rows[:40], submitted_by="risk_engine-service")
    print(f"   {mut.description}\n   -> {res['compatibility']} / {res['status']}")
    out["steps"].append({"step": "type_widen", "result": res["status"]})

    # ---- 3. Submission sample that violates the new contract ----
    _banner("3. Submission rejected at the gate — sample data contradicts the contract")
    sim, db = _table_from_live(store, "payment_core", "payments")
    mut = _mut_enum_tighten(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    # Ship a sample containing the value the contract is about to disallow.
    # Find whichever enum column the mutation actually tightened.
    enum_col = next(
        c.name for c in new_table.columns
        if c.enum is not None and sim.column(c.name) is not None
        and c.enum != sim.column(c.name).enum
    )
    removed = [e for e in sim.column(enum_col).enum if e not in new_table.column(enum_col).enum][0]
    bad_sample = [dict(r) for r in new_table.rows[:20]]
    bad_sample[0][enum_col] = removed
    print(f"   mutation: {mut.description}; sample row 0 {enum_col}={removed!r} (removed by the new contract)")
    try:
        platform.submit(new_table.contract_document(), sample=bad_sample, submitted_by="payment_core-service")
        print("   !! UNEXPECTED: submission was accepted")
    except PlatformError as e:
        print(f"   -> REJECTED (HTTP {e.status_code}): {e.detail['sample_errors'][0]}")
        out["steps"].append({"step": "sample_gate", "result": "rejected", "error": str(e)})
    # Now submit without the bad row: it is a genuine breaking change -> sign-off.
    good_sample = [r for r in new_table.rows[:20] if r.get(enum_col) != removed]
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=good_sample, submitted_by="payment_core-service")
    print(f"   resubmitted with clean sample -> {res['compatibility']} / {res['status']}")
    print(f"   consumers to sign: {[s['consumer'] for s in res.get('signoffs', [])]}")
    out["steps"].append({"step": "breaking_enum", "result": res["status"], "signoffs": res.get("signoffs")})

    # ---- 4. Hard rename identified only from sample values ----
    _banner("4. Field rename — only detectable by comparing sample values")
    sim, db = _table_from_live(store, "user_center", "users")
    mut = _mut_rename_hard(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=new_table.rows[:40], submitted_by="user_center-service")
    renames = res["changes"]["renames"]
    print(f"   {mut.description}")
    for r in renames:
        print(f"   -> detected rename {r['old_name']} -> {r['new_name']} "
              f"(confidence {r['confidence']}, evidence {r['evidence']})")
    print(f"   classification: {res['compatibility']} / {res['status']}")
    out["steps"].append({"step": "rename_hard", "renames": renames, "result": res["status"]})

    # ---- 5. Destructive drop, sign-off partial, then rejection ----
    _banner("5. Destructive drop — sign-off gate with a rejecting consumer")
    sim, db = _table_from_live(store, "merchant", "merchants")
    mut = _mut_drop(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=new_table.rows[:40], submitted_by="merchant-service")
    cid, ver = res["contract_id"], res["version"]
    consumers = [s["consumer"] for s in res["signoffs"]]
    print(f"   {mut.description} -> {res['status']} awaiting {consumers}")
    platform.decide(cid, ver, consumers[0], "approved", comment="report query updated")
    print(f"   {consumers[0]} approved; still pending")
    platform.decide(cid, ver, consumers[1], "rejected", comment="feature pipeline still reads the column")
    print(f"   {consumers[1]} rejected -> version status = {store.get_version(cid, ver)['status']}")
    out["steps"].append({"step": "drop_rejected", "status": "rejected"})

    # ---- 6. Destructive range tightening, all consumers approve, parallel ----
    _banner("6. Range tightening — all consumers approve, parallel rollout begins")
    sim, db = _table_from_live(store, "order_core", "order_items")
    mut = _mut_range_tighten(sim, rng)
    new_table = mut.apply(sim.copy(), rng)
    # A real submitter would ship sample data that satisfies the tightened range.
    tightened = new_table.columns[[c.name for c in sim.columns].index(_target_column(sim, new_table))]
    for row in new_table.rows:
        v = row.get(tightened.name)
        if isinstance(v, (int, float)) and tightened.maximum is not None and v > tightened.maximum:
            row[tightened.name] = tightened.maximum
    materialize([new_table], db)
    res = platform.submit(new_table.contract_document(), sample=new_table.rows[:40], submitted_by="order_core-service")
    cid, ver = res["contract_id"], res["version"]
    for c in res["signoffs"]:
        platform.decide(cid, ver, c["consumer"], "approved", comment="ok after backfill")
    row = store.get_version(cid, ver)
    print(f"   {mut.description}")
    print(f"   -> status {row['status']}, transition ends {row['transition_ends_at']}")
    print(f"   old version now: {store.get_version(cid, ver - 1)['status']} (parallel window)")
    out["steps"].append({"step": "range_approved", "status": row["status"], "transition_ends_at": row["transition_ends_at"]})

    # ---- 7. Dirty data ships; daily sampler catches it, alerts dedupe ----
    _banner("7. Dirty data — validation, row-level drilldown, alert deduplication")
    sim, db = _table_from_live(store, "payment_core", "payments")
    clean_rows = [dict(r) for r in sim.rows]
    injections = dirty_injection_library(sim, random.Random(3))
    fired = []
    t = sim.copy()
    for inj in injections:
        if inj.kind in {"null_in_required", "invalid_enum", "value_out_of_range", "duplicate_primary_key"}:
            inj.apply(t, random.Random(11))
            fired.append(inj.kind)
    materialize([t], db)
    print(f"   injected cumulatively: {fired}")
    run1 = platform.validate_contract("payment_core.payments", trigger="scheduled")
    for r in run1:
        print(f"   run {r['run_id'][:8]} v{r['version']}: {r['rows_checked']} rows, "
              f"{r['violation_count']} violations, alerts delivered {len(r['alerts_delivered'])}")
        print(f"     by rule: {r['by_rule']}")
    # second run inside the quiet window -> repeats suppressed
    run2 = platform.validate_contract("payment_core.payments", trigger="scheduled")
    for r in run2:
        print(f"   second run: {r['violation_count']} violations, new deliveries {len(r['alerts_delivered'])} "
              f"(duplicates suppressed)")
    out["steps"].append({"step": "dirty_data", "run1": run1[0]["violation_count"],
                         "delivered_1": len(run1[0]["alerts_delivered"]),
                         "delivered_2": len(run2[0]["alerts_delivered"])})

    # ---- 8. Alert recovery ----
    _banner("8. Data repaired — open alerts resolve exactly once")
    sim, db = _table_from_live(store, "payment_core", "payments")
    sim.rows = clean_rows
    materialize([sim], db)
    run3 = platform.validate_contract("payment_core.payments", trigger="scheduled")
    for r in run3:
        print(f"   clean run: {r['violation_count']} violations; resolution notices {len(r['alerts_delivered'])}")
    out["steps"].append({"step": "recovery", "resolutions": len(run3[0]["alerts_delivered"])})

    # ---- 9. No-op change is rejected as a no-change submit ----
    _banner("9. No-op — identical contract is refused rather than version-bumped")
    latest = store.latest_version("user_center.user_devices")
    exact_doc = json.loads(latest["document"])
    sim, db = _table_from_live(store, "user_center", "user_devices")
    try:
        platform.submit(exact_doc, sample=sim.rows[:20], submitted_by="user_center-service")
        print("   !! UNEXPECTED")
    except PlatformError as e:
        print(f"   -> {e.status_code}: {e}")

    # ---- summary ----
    _banner("Platform state")
    d = platform.dashboard()
    print(json.dumps({
        "contracts": d["contracts"],
        "pending_signoffs": [p["contract_id"] + " v" + str(p["version"]) for p in d["pending_signoffs"]],
        "open_alerts": d["alerts"]["open"],
        "suppressed_duplicates": d["alerts"]["suppressed_recent"],
        "violations_recent": d["violations"]["total_recent"],
        "upstream_summary": d["upstream_summary"],
    }, indent=2, default=str))
    out["dashboard"] = d
    return out


if __name__ == "__main__":
    run_demo()
