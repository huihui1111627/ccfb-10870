"""Command-line interface and demo bootstrap.

``python -m dcp.cli seed``          build mock upstream DBs, register contracts
``python -m dcp.cli simulate``      run N random upstream changes through the gate
``python -m dcp.cli validate``      run the daily sampler over all active contracts
``python -m dcp.cli evaluate``      run the measurement harness
``python -m dcp.cli serve``         start the admin backend
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import settings
from .platform import Platform, PlatformError, iso, utcnow
from .store import Store


def _platform(db_path: str | None = None) -> tuple[Store, Platform]:
    path = Path(db_path) if db_path else settings.database
    store = Store(path)
    platform = Platform(
        store,
        sample_rows=int(store.get_setting("sample_rows", str(settings.sample_rows)) or settings.sample_rows),
        transition_days=int(
            store.get_setting("transition_days", str(settings.transition_days)) or settings.transition_days
        ),
    )
    return store, platform


def cmd_seed(args: argparse.Namespace) -> int:
    from dcpsim.upstreams import build_upstreams, materialize, all_tables

    store, platform = _platform(args.db)
    if args.reset:
        store.reset()

    by_system = build_upstreams(rows_per_table=args.rows, seed=args.seed)
    upstream_dir = Path(args.upstream_dir) if args.upstream_dir else settings.upstream_dir
    upstream_dir.mkdir(parents=True, exist_ok=True)

    submitted = 0
    for system, tables in by_system.items():
        db_file = upstream_dir / f"{system}.db"
        materialize(tables, db_file)
        for table in tables:
            store.register_upstream(system, table.name, str(db_file), table.description)
            doc = table.contract_document()
            sample = table.rows[: min(50, len(table.rows))]
            try:
                res = platform.submit(
                    doc, sample=sample, submitted_by=f"{system}-service", note="bootstrap baseline"
                )
                status = res["status"]
                print(f"  registered {table.contract_id:32s} -> v{res['version']} {status}")
                submitted += 1
            except PlatformError as e:
                print(f"  !! {table.contract_id}: {e}", file=sys.stderr)
    if args.validate:
        results = platform.validate_all(trigger="bootstrap")
        bad = sum(r.get("violation_count", 0) for r in results)
        print(f"baseline validation: {len(results)} runs, {bad} violations")
    print(f"seeded {submitted} contracts; upstream DBs in {upstream_dir}")
    return 0


def _mutate_upstream(
    store: Store,
    platform: Platform,
    upstream_dir: Path,
    *,
    system: str,
    table_name: str,
    mutation_kind: str | None,
    sample_rows: int,
    seed: int,
    submit: bool,
) -> dict[str, Any]:
    """Load a live upstream, apply one mutation, rewrite it, and submit through the gate."""
    from dcpsim.simulator import (
        ALL_STRUCTURAL_MUTATIONS,
        NotApplicable,
        dirty_injection_library,
    )
    from dcpsim.upstreams import materialize, physical_schema, read_table, SimTable, ColSpec, _gen_value

    ups = store.upstream_table(system, table_name)
    if not ups:
        raise SystemExit(f"upstream {system}.{table_name} is not registered")

    cols, rows = read_table(ups["db_path"], table_name)
    phys = physical_schema(ups["db_path"], table_name)
    # Rebuild a SimTable from the live database shape.
    current_contract_id = f"{system}.{table_name}"
    latest = store.latest_version(current_contract_id)
    if not latest:
        raise SystemExit("no contract registered")
    from .contract import parse_contract

    doc = json.loads(latest["document"])
    sim_cols = []
    for c in doc["columns"]:
        sim_cols.append(
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
        )
    sim = SimTable(
        system=system,
        name=table_name,
        columns=sim_cols,
        primary_key=doc.get("primary_key", []),
        unique_keys=doc.get("unique_keys", []),
        freshness=doc.get("freshness"),
        rules=doc.get("rules", []),
        consumers=doc.get("consumers", []),
        owners=doc.get("owners", []),
        description=doc.get("description", ""),
        rows=rows,
    )

    rng = random.Random(seed)
    if mutation_kind:
        factory = next(
            (f for f in ALL_STRUCTURAL_MUTATIONS if _kind_for(f) == mutation_kind), None
        )
        if factory is None:
            raise SystemExit(f"unknown mutation {mutation_kind}")
    else:
        factory = rng.choice(ALL_STRUCTURAL_MUTATIONS)
        for _ in range(20):
            try:
                probe = factory(sim, rng)
                break
            except NotApplicable:
                factory = rng.choice(ALL_STRUCTURAL_MUTATIONS)
        else:
            raise SystemExit("could not find an applicable mutation")

    mutation = factory(sim, rng)
    new_table = mutation.apply(sim.copy(), rng)
    # Persist the new physical shape.
    materialize([new_table], ups["db_path"])

    sample = new_table.rows[: min(sample_rows, len(new_table.rows))]
    result: dict[str, Any] = {
        "system": system,
        "table": table_name,
        "mutation": mutation.kind,
        "label": mutation.label,
        "description": mutation.description,
    }
    if submit:
        try:
            res = platform.submit(
                new_table.contract_document(),
                sample=sample,
                submitted_by=f"{system}-service",
                note=f"simulated {mutation.kind}",
            )
            result.update(
                {
                    "version": res["version"],
                    "status": res["status"],
                    "compatibility": res["compatibility"],
                    "breaking": res["compatibility"] == "breaking",
                    "correct": (res["compatibility"] == "breaking") == (mutation.label == "breaking"),
                    "changes": [c["summary"] for c in res["changes"]["changes"]],
                    "renames": res["changes"]["renames"],
                }
            )
            print(
                f"{system}.{table_name}: {mutation.kind} -> {res['compatibility']} "
                f"({res['status']}), expected {mutation.label}"
            )
        except PlatformError as e:
            result.update({"error": str(e), "status": "rejected_at_submission"})
            print(f"{system}.{table_name}: {mutation.kind} -> rejected: {e}")
    return result


def _kind_for(factory) -> str | None:
    """Map a factory function to its mutation kind by invoking it is impossible;
    use the docstring-free registry instead."""
    from dcpsim import simulator as sim

    mapping = {
        sim._mut_rename: "rename_column",
        sim._mut_rename_hard: "rename_hard",
        sim._mut_rename_with_tightening: "rename_with_tightening",
        sim._mut_drop: "drop_column",
        sim._mut_add_nullable: "add_nullable_column",
        sim._mut_add_required: "add_required_column",
        sim._mut_type_widen: "type_widen",
        sim._mut_type_narrow: "type_narrow",
        sim._mut_enum_tighten: "enum_remove_value",
        sim._mut_enum_widen: "enum_add_value",
        sim._mut_range_tighten: "range_tighten",
        sim._mut_range_widen: "range_widen",
        sim._mut_required_tighten: "required_tighten",
        sim._mut_required_relax: "required_relax",
        sim._mut_unique_tighten: "unique_tighten",
        sim._mut_unique_relax: "unique_relax",
        sim._mut_precision_narrow: "precision_narrow",
        sim._mut_pk_change: "primary_key_change",
        sim._mut_freshness_tighten: "freshness_tighten",
        sim._mut_freshness_relax: "freshness_relax",
        sim._mut_add_rule: "rule_added",
        sim._mut_description: "description_change",
        sim._mut_reorder: "reorder_columns",
        sim._mut_noop: "noop",
        sim._mut_drop_and_swap: "drop_and_add_unrelated",
    }
    return mapping.get(factory)


def cmd_simulate(args: argparse.Namespace) -> int:
    store, platform = _platform(args.db)
    upstream_dir = Path(args.upstream_dir) if args.upstream_dir else settings.upstream_dir
    tables = store.list_upstream_tables()
    if not tables:
        print("no upstreams registered; run `python -m dcp.cli seed` first", file=sys.stderr)
        return 2
    rng = random.Random(args.seed)
    results = []
    for i in range(args.count):
        # Prefer contracts with no open sign-off so each simulated change is
        # diffed against an unambiguous baseline.
        open_ids = {p["contract_id"] for p in store.pending_versions()}
        eligible = [t for t in tables if f"{t['upstream_system']}.{t['table_name']}" not in open_ids]
        t = rng.choice(eligible or tables)
        try:
            res = _mutate_upstream(
                store, platform, upstream_dir,
                system=t["upstream_system"], table_name=t["table_name"],
                mutation_kind=None if args.random else args.mutation,
                sample_rows=args.sample_rows, seed=args.seed + i, submit=True,
            )
            results.append(res)
        except (SystemExit, PlatformError) as e:
            print(f"skip: {e}", file=sys.stderr)
        if args.auto_sign:
            for p in store.pending_versions():
                contract = store.load_contract(p["contract_id"], p["version"])
                if not contract:
                    continue
                for c in contract.consumers:
                    try:
                        platform.decide(p["contract_id"], p["version"], c, "approved", comment="auto-signed (demo)")
                    except PlatformError:
                        pass
        if args.delay:
            time.sleep(args.delay)
    if args.validate:
        out = platform.validate_all(trigger="simulation")
        print(f"validated {len(out)} versions, "
              f"{sum(r.get('violation_count', 0) for r in out)} violations")
    if args.report:
        Path(args.report).write_text(json.dumps(results, indent=2, default=str))
        print(f"wrote {args.report}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    store, platform = _platform(args.db)
    if args.contract:
        out = platform.validate_contract(args.contract, use_sample=args.use_sample)
    else:
        out = platform.validate_all(trigger="scheduled")
    for r in out:
        if "error" in r:
            print(f"  {r['contract_id']}: {r['error']}")
            continue
        print(
            f"  {r['contract_id']} v{r['version']} [{r['role']}]: "
            f"{r['rows_checked']} rows, {r['violation_count']} violations, "
            f"clean {r['clean_ratio']:.2%}"
        )
    return 0


def cmd_evaluate(args: argparse.Namespace) -> int:
    from dcpeval.harness import run_evaluation

    report = run_evaluation(
        rows_per_table=args.rows, seed=args.seed, output_dir=args.output_dir
    )
    b = report["breaking_change_detection"]
    v = report["violation_detection"]
    print(f"breaking-change accuracy : {b['accuracy']:.4f}  (P {b['precision_breaking']:.4f} / R {b['recall_breaking']:.4f})")
    print(f"violation detection rate : {v['detection_rate']:.4f}  ({v['detected']}/{v['injections']})")
    print(f"report: {Path(args.output_dir) / 'evaluation.md'}")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .api import create_app

    app = create_app(db_path=args.db, sample_rows=args.sample_rows)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    store, platform = _platform(args.db)
    d = platform.dashboard()
    print(json.dumps(
        {
            "contracts": d["contracts"],
            "pending_signoffs": len(d["pending_signoffs"]),
            "open_alerts": d["alerts"]["open"],
            "latest_run": d["latest_run"],
        },
        indent=2, default=str,
    ))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dcp", description="Data Contract Platform")
    p.add_argument("--db", help="path to platform SQLite DB", default=None)
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("seed", help="build mock upstreams and register contracts")
    s.add_argument("--rows", type=int, default=800)
    s.add_argument("--seed", type=int, default=7)
    s.add_argument("--reset", action="store_true")
    s.add_argument("--validate", action="store_true")
    s.add_argument("--upstream-dir", default=None)
    s.set_defaults(func=cmd_seed)

    s = sub.add_parser("simulate", help="randomly change upstreams through the gate")
    s.add_argument("--count", type=int, default=5)
    s.add_argument("--seed", type=int, default=99)
    s.add_argument("--mutation", default=None)
    s.add_argument("--random", action="store_true", default=True)
    s.add_argument("--demo", action="store_true", help="use the curated demo sequence")
    s.add_argument("--sample-rows", type=int, default=40)
    s.add_argument("--delay", type=float, default=0)
    s.add_argument("--auto-sign", action="store_true")
    s.add_argument("--validate", action="store_true")
    s.add_argument("--report", default=None)
    s.add_argument("--upstream-dir", default=None)
    s.set_defaults(func=cmd_simulate)

    s = sub.add_parser("validate", help="run the daily sampler")
    s.add_argument("--contract", default=None)
    s.add_argument("--use-sample", action="store_true")
    s.set_defaults(func=cmd_validate)

    s = sub.add_parser("evaluate", help="measure detection accuracy")
    s.add_argument("--rows", type=int, default=600)
    s.add_argument("--seed", type=int, default=11)
    s.add_argument("--output-dir", default="reports")
    s.set_defaults(func=cmd_evaluate)

    s = sub.add_parser("serve", help="start the admin backend")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8765)
    s.add_argument("--sample-rows", type=int, default=None)
    s.add_argument("--log-level", default="info")
    s.set_defaults(func=cmd_serve)

    s = sub.add_parser("status", help="print platform state summary")
    s.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
