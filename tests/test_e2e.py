"""End-to-end acceptance test against the real mock upstream databases.

Unlike the unit tests, this exercises the whole path the demo does: materialize
SQLite databases, register contracts, mutate the physical schema, submit through
the API, sign off, validate real rows, and check that alerts are deduplicated.
"""

from __future__ import annotations

import random
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from dcp.alerts import InMemoryChannel
from dcp.api import create_app
from dcp.platform import Platform, PlatformError, utcnow
from dcp.store import Store
from dcpsim.simulator import (
    _mut_drop,
    _mut_enum_tighten,
    _mut_rename_hard,
    _mut_type_widen,
    dirty_injection_library,
)
from dcpsim.upstreams import SimTable, build_upstreams, materialize, physical_schema


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    root = tmp_path_factory.mktemp("e2e")
    by_system = build_upstreams(rows_per_table=120, seed=17)
    db_paths = {}
    for system, tables in by_system.items():
        path = root / f"{system}.db"
        materialize(tables, path)
        db_paths[system] = str(path)
    return {"root": root, "by_system": by_system, "db_paths": db_paths}


@pytest.fixture
def env(world, tmp_path):
    store = Store(tmp_path / "e2e.db")
    for system, tables in world["by_system"].items():
        for table in tables:
            store.register_upstream(system, table.name, world["db_paths"][system], table.description)
    channel = InMemoryChannel()
    from dcp.alerts import AlertManager

    manager = AlertManager(store, channels=[channel], quiet_hours=24)
    platform = Platform(store, alert_manager=manager, transition_days=7)
    return store, platform, channel


def live_table(world, system: str, name: str) -> SimTable:
    """Rebuild a SimTable from the physical DB plus the latest contract."""
    import json

    from dcpsim.upstreams import read_table

    table = next(t for t in world["by_system"][system] if t.name == name)
    return table


def test_full_lifecycle(world, env):
    store, platform, channel = env
    now = utcnow()

    # 1. register every table at v1 and validate the real database
    for system, tables in world["by_system"].items():
        for table in tables:
            res = platform.submit(
                table.contract_document(),
                sample=table.rows[:30],
                submitted_by=f"{system}-service",
                now=now,
            )
            assert res["status"] == "active"
    runs = platform.validate_all(trigger="bootstrap", now=now)
    assert len(runs) == 9
    assert all(r.get("violation_count", 0) == 0 for r in runs), "baseline must be clean"

    # 2. compatible physical change: widening an int column
    table = live_table(world, "risk_engine", "risk_events")
    sim, db = _load(world, store, "risk_engine", "risk_events")
    mut = _mut_type_widen(sim, random.Random(4))
    widened = mut.apply(sim.copy(), random.Random(4))
    materialize([widened], db)
    res = platform.submit(widened.contract_document(), sample=widened.rows[:30], now=now)
    assert res["compatibility"] == "backward_compatible"
    assert res["status"] == "active"

    # 3. breaking change: a hard rename, needs sign-off from all consumers
    sim, db = _load(world, store, "user_center", "users")
    mut = _mut_rename_hard(sim, random.Random(5))
    renamed = mut.apply(sim.copy(), random.Random(5))
    materialize([renamed], db)
    res = platform.submit(renamed.contract_document(), sample=renamed.rows[:30], now=now)
    assert res["compatibility"] == "breaking"
    assert res["status"] == "pending_signoff"
    assert res["changes"]["renames"], "the rename must be identified, not treated as drop+add"
    old_name, new_name = res["changes"]["renames"][0]["old_name"], res["changes"]["renames"][0]["new_name"]

    consumers = [s["consumer"] for s in res["signoffs"]]
    assert len(consumers) >= 2
    for c in consumers[:-1]:
        platform.decide(res["contract_id"], res["version"], c, "approved", now=now)
    assert store.get_version(res["contract_id"], res["version"])["status"] == "pending_signoff"
    platform.decide(res["contract_id"], res["version"], consumers[-1], "approved", now=now)
    assert store.get_version(res["contract_id"], res["version"])["status"] == "active"

    # both versions validate during the parallel window
    both = platform.validate_contract("user_center.users", now=now)
    assert len(both) == 2
    assert {b["role"] for b in both} == {"active", "previous"}

    # 4. submission gate: bad sample is refused before it becomes a version
    sim, db = _load(world, store, "payment_core", "payments")
    mut = _mut_enum_tighten(sim, random.Random(6))
    tightened = mut.apply(sim.copy(), random.Random(6))
    enum_col, allowed = _tightened_enum(sim, tightened)
    removed = next(r for r in sim.column(enum_col).enum if r not in allowed)
    bad = [dict(r) for r in tightened.rows[:20]]
    bad[0][enum_col] = removed
    before = store.latest_version("payment_core.payments")["version"]
    with pytest.raises(PlatformError) as exc:
        platform.submit(tightened.contract_document(), sample=bad, now=now)
    assert exc.value.status_code == 422
    assert store.latest_version("payment_core.payments")["version"] == before

    # 5. real dirty data is caught and alerts dedupe
    sim, db = _load(world, store, "payment_core", "payments")
    clean_rows = [dict(r) for r in sim.rows]
    t = sim.copy()
    injections = dirty_injection_library(sim, random.Random(7))
    fired = 0
    for inj in injections:
        if inj.kind in {"null_in_required", "invalid_enum", "value_out_of_range"}:
            inj.apply(t, random.Random(fired))
            fired += 1
    materialize([t], db)
    channel.sent.clear()
    first = platform.validate_contract("payment_core.payments", trigger="scheduled", now=now)
    assert first[0]["violation_count"] > 0
    assert first[0]["alerts_delivered"], "violations must produce alerts"

    # second run inside the quiet window: same problems, no new pushes
    channel.sent.clear()
    second = platform.validate_contract(
        "payment_core.payments", trigger="scheduled", now=now + timedelta(hours=2)
    )
    assert second[0]["violation_count"] == first[0]["violation_count"]
    assert second[0]["alerts_delivered"] == []
    suppressed = [a for a in store.list_alerts() if a["suppressed"] == 1]
    assert suppressed, "repeats must be recorded as suppressed, not dropped"

    # 6. repair: alerts resolve once
    sim.rows = clean_rows
    materialize([sim], db)
    channel.sent.clear()
    repaired = platform.validate_contract(
        "payment_core.payments", trigger="scheduled", now=now + timedelta(hours=3)
    )
    assert repaired[0]["violation_count"] == 0
    assert all(a.kind == "resolution" for a in channel.sent)

    # 7. trend reporting attributes violations by upstream system
    trend = platform.trend()
    assert any(row["upstream_system"] == "payment_core" for row in trend["by_upstream"])

    # 8. dropping a column upstream is caught by the sampler as schema drift
    sim, db = _load(world, store, "merchant", "merchants")
    mut = _mut_drop(sim, random.Random(8))
    dropped = mut.apply(sim.copy(), random.Random(8))
    materialize([dropped], db)
    drift = platform.validate_contract("merchant.merchants", trigger="scheduled", now=now)
    assert drift[0]["structural_errors"], "missing column must be reported"
    assert any(v["rule_kind"] == "schema" for v in platform.violations_list(contract_id="merchant.merchants"))


def _load(world, store, system: str, name: str):
    """Build a SimTable from the live DB and physical schema."""
    import json

    from dcpsim.upstreams import ColSpec, read_table

    db = world["db_paths"][system]
    phys = physical_schema(db, name)
    latest = store.latest_version(f"{system}.{name}")
    doc = json.loads(latest["document"])
    cols, rows = read_table(db, name)
    sim = SimTable(
        system=system,
        name=name,
        columns=[
            ColSpec(
                name=c["name"],
                type=c["type"],
                sql_type=phys.get(c["name"], "TEXT"),
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
        ],
        primary_key=doc.get("primary_key", []),
        unique_keys=doc.get("unique_keys", []),
        freshness=doc.get("freshness"),
        rules=doc.get("rules", []),
        consumers=doc.get("consumers", []),
        owners=doc.get("owners", []),
        rows=rows,
    )
    return sim, db


def _tightened_enum(old: SimTable, new: SimTable):
    for c in new.columns:
        before = old.column(c.name)
        if before is not None and c.enum != before.enum and c.enum is not None:
            return c.name, set(c.enum)
    raise AssertionError("no enum tightening found")


def test_console_serves_against_real_data(world, tmp_path):
    store = Store(tmp_path / "ui.db")
    for system, tables in world["by_system"].items():
        for table in tables:
            store.register_upstream(system, table.name, world["db_paths"][system], table.description)
    app = create_app(db_path=tmp_path / "ui.db", alert_channels=[InMemoryChannel()])
    with TestClient(app) as c:
        table = world["by_system"]["order_core"][0]
        r = c.post(
            "/api/contracts/order_core.orders/submit",
            json={"contract": table.contract_document(), "sample": table.rows[:20]},
        )
        assert r.status_code == 200
        assert c.get("/api/contracts/order_core.orders").status_code == 200
        assert c.get("/").status_code == 200
        dash = c.get("/api/dashboard").json()
        assert dash["contracts"]["total"] == 1
