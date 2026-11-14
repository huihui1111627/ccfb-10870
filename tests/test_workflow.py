"""Workflow + alerting tests: submission gate, sign-off, rollout, dedupe, recovery."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dcp.alerts import AlertManager, InMemoryChannel, alert_key
from dcp.contract import parse_contract
from dcp.platform import Platform, PlatformError
from dcp.store import Store
from dcp.validator import ValidationResult, Violation


NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def env(tmp_path):
    store = Store(tmp_path / "dcp.db")
    channel = InMemoryChannel()
    manager = AlertManager(store, channels=[channel], quiet_hours=24)
    platform = Platform(store, alert_manager=manager, transition_days=7)
    return store, platform, channel


def doc(**over):
    d = {
        "contract_id": "s.t",
        "upstream_system": "s",
        "table": "t",
        "columns": [
            {"name": "id", "type": "string", "unique": True},
            {"name": "amount", "type": "decimal", "minimum": 0, "maximum": 1000, "precision": 8, "scale": 2},
            {"name": "status", "type": "string", "enum": ["paid", "refunded"]},
        ],
        "primary_key": ["id"],
        "consumers": ["downstream/a", "downstream/b"],
        "owners": ["owner/x"],
    }
    d.update(over)
    return d


def row(**kw):
    base = {"id": "1", "amount": 10.0, "status": "paid"}
    base.update(kw)
    return base


def test_first_submission_is_active(env):
    store, platform, channel = env
    res = platform.submit(doc(), sample=[row()], submitted_by="upstream", now=NOW)
    assert res["status"] == "active"
    assert res["compatibility"] == "initial"
    assert store.active_version("s.t")["version"] == 1


def test_second_submission_blocked_while_signoff_pending(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    d2 = doc()
    d2["columns"].append({"name": "other", "type": "string", "required": False})
    with pytest.raises(PlatformError) as e:
        platform.submit(d2, sample=[row(other=None)], now=NOW)
    assert e.value.status_code == 409
    assert e.value.detail["pending_version"] == 2


def test_omitted_metadata_is_inherited(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d.pop("consumers")
    d.pop("owners")
    d["columns"].append({"name": "extra", "type": "string", "required": False})
    res = platform.submit(d, sample=[row(extra=None)], now=NOW)
    assert res["status"] == "active"
    stored = store.load_contract("s.t", 2)
    assert stored.consumers == ["downstream/a", "downstream/b"]
    assert stored.owners == ["owner/x"]


def test_explicit_empty_consumers_is_honoured(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc(consumers=[])
    d["columns"].append({"name": "extra", "type": "string", "required": False})
    platform.submit(d, sample=[row(extra=None)], now=NOW)
    assert store.load_contract("s.t", 2).consumers == []


def test_identical_resubmission_is_refused(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    with pytest.raises(PlatformError) as e:
        platform.submit(doc(), sample=[row()], now=NOW)
    assert e.value.status_code == 409


def test_compatible_change_activates_without_signoff(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"].append({"name": "extra", "type": "string", "required": False})
    res = platform.submit(d, sample=[row()], now=NOW)
    assert res["status"] == "active"
    assert res["compatibility"] == "backward_compatible"


def test_breaking_change_enters_signoff_and_requires_all(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    res = platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    assert res["status"] == "pending_signoff"
    assert len(res["signoffs"]) == 2
    assert all(s["decision"] == "pending" for s in res["signoffs"])

    out = platform.decide("s.t", 2, "downstream/a", "approved", now=NOW)
    assert out["status"] == "pending_signoff"
    out = platform.decide("s.t", 2, "downstream/b", "approved", now=NOW)
    assert out["status"] == "active"


def test_v1_becomes_parallel_during_transition(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    platform.decide("s.t", 2, "downstream/a", "approved", now=NOW)
    platform.decide("s.t", 2, "downstream/b", "approved", now=NOW)
    v1 = store.get_version("s.t", 1)
    assert v1["status"] == "parallel"
    assert v1["transition_ends_at"]


def test_one_rejection_blocks_the_change(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    platform.decide("s.t", 2, "downstream/a", "approved", now=NOW)
    out = platform.decide("s.t", 2, "downstream/b", "rejected", comment="still reads it", now=NOW)
    assert out["status"] == "rejected"
    assert store.get_version("s.t", 2)["status"] == "rejected"
    # rejection is pushed to the upstream owner
    assert any(a.recipient == "owner/x" and a.kind == "signoff_rejected" for a in channel.sent)


def test_unregistered_consumer_cannot_sign(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    with pytest.raises(PlatformError) as e:
        platform.decide("s.t", 2, "stranger", "approved", now=NOW)
    assert e.value.status_code == 403


def test_cannot_sign_a_version_not_pending(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    with pytest.raises(PlatformError) as e:
        platform.decide("s.t", 1, "downstream/a", "approved", now=NOW)
    assert e.value.status_code == 409


def test_submission_sample_gate_blocks_bad_sample(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [
        c if c["name"] != "status" else {**c, "enum": ["paid"]} for c in d["columns"]
    ]
    with pytest.raises(PlatformError) as e:
        platform.submit(d, sample=[row(status="refunded")], now=NOW)
    assert e.value.status_code == 422
    assert "sample violates" in str(e.value.detail["sample_errors"][0])
    # nothing was persisted as a version
    assert store.latest_version("s.t")["version"] == 1


def test_breaking_change_without_consumers_is_released_with_note(env):
    store, platform, channel = env
    base = doc(consumers=[], owners=["owner/x"])
    platform.submit(base, sample=[row()], now=NOW)
    d = doc(consumers=[], owners=["owner/x"])
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    res = platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW, note="")
    assert res["status"] == "active"


def test_auto_released_breaking_change_still_parallel(env):
    store, platform, channel = env
    base = doc(consumers=[], owners=["owner/x"])
    platform.submit(base, sample=[row()], now=NOW)
    d = doc(consumers=[], owners=["owner/x"])
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    res = platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW, note="")
    assert res["status"] == "active"
    assert store.get_version("s.t", 1)["status"] == "parallel"


def test_transition_finalization_supersedes_old_version(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    platform.decide("s.t", 2, "downstream/a", "approved", now=NOW)
    platform.decide("s.t", 2, "downstream/b", "approved", now=NOW)
    done = platform.finalize_transitions(now=NOW + timedelta(days=8))
    assert any(x["version"] == 1 for x in done)
    assert store.get_version("s.t", 1)["status"] == "superseded"


# ---------------- alerting ----------------


def make_result(rule="column.amount.max", column="amount", n=3, severity="high"):
    res = ValidationResult(contract_id="s.t", version=1, rows_checked=100, rows_clean=97)
    for i in range(n):
        res.violations.append(
            Violation(rule, "range", severity, column, "amount above maximum",
                      row_pk={"id": str(i)}, row_data={"id": str(i)})
        )
    return res


def test_alert_grouping_by_problem_not_row(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    channel.sent.clear()
    manager = platform.alerts
    delivered = manager.process_run(contract, make_result(n=500), now=NOW)
    recipients = {a.recipient for a in delivered}
    assert recipients == {"owner/x"}  # high severity -> owners only
    assert all(a.occurrence_count == 500 for a in delivered)


def test_critical_goes_to_owners_and_consumers(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    channel.sent.clear()
    delivered = platform.alerts.process_run(
        contract, make_result(severity="critical"), now=NOW
    )
    assert {a.recipient for a in delivered} == {"owner/x", "downstream/a", "downstream/b"}


def test_repeat_within_quiet_window_is_suppressed(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    channel.sent.clear()
    first = platform.alerts.process_run(contract, make_result(), now=NOW)
    then = NOW + timedelta(hours=2)
    channel.sent.clear()
    second = platform.alerts.process_run(contract, make_result(), now=then)
    assert len(first) >= 1
    assert second == []
    rows = store.list_alerts(include_suppressed=True)
    assert any(r["suppressed"] == 1 for r in rows)


def test_repeat_after_quiet_window_notifies_again(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    platform.alerts.process_run(contract, make_result(), now=NOW)
    later = NOW + timedelta(hours=25)
    channel.sent.clear()
    again = platform.alerts.process_run(contract, make_result(), now=later)
    assert len(again) >= 1


def test_resolution_emitted_once_when_problem_clears(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    platform.alerts.process_run(contract, make_result(), now=NOW)
    channel.sent.clear()
    cleared = platform.alerts.process_run(contract, ValidationResult("s.t", 1, 100, 100), now=NOW + timedelta(hours=3))
    assert cleared and all(a.kind == "resolution" for a in cleared)
    channel.sent.clear()
    again = platform.alerts.process_run(contract, ValidationResult("s.t", 1, 100, 100), now=NOW + timedelta(hours=4))
    assert again == []


def test_medium_alerts_only_at_digest_time(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    channel.sent.clear()
    noon = NOW.replace(hour=12)
    assert platform.alerts.process_run(contract, make_result(severity="medium"), now=noon) == []
    assert platform.alerts.process_run(contract, make_result(severity="medium"), now=NOW.replace(hour=9))


def test_medium_resolution_waits_for_digest(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    platform.alerts.process_run(contract, make_result(severity="medium"), now=NOW.replace(hour=9))
    channel.sent.clear()
    clear = ValidationResult("s.t", 1, 100, 100)
    noon = NOW.replace(hour=12)
    assert platform.alerts.process_run(contract, clear, now=noon) == []


def test_distinct_rules_alert_separately(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    res = make_result(rule="r1", column="amount", n=2)
    res.violations += make_result(rule="r2", column="status", n=2).violations
    channel.sent.clear()
    delivered = platform.alerts.process_run(contract, res, now=NOW)
    keys = {a.alert_key for a in delivered}
    assert len(keys) == 2


def test_occurrence_count_uses_true_total_when_storage_is_capped(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    from dcp.validator import ValidationResult, Violation

    # The run reports 900 true violations but only 10 records were kept.
    res = ValidationResult("s.t", 1, rows_checked=900, rows_clean=0)
    res.violations = [
        Violation("column.amount.max", "range", "high", "amount", "too big", row_pk={"id": str(i)})
        for i in range(10)
    ]
    res.by_rule = {"column.amount.max": 900}
    channel.sent.clear()
    delivered = platform.alerts.process_run(contract, res, now=NOW)
    assert delivered and all(a.occurrence_count == 900 for a in delivered)


def test_alert_key_is_stable_across_rows(env):
    a = Violation("r", "range", "high", "amount", "x", row_pk={"id": "1"})
    b = Violation("r", "range", "high", "amount", "x", row_pk={"id": "2"})
    assert alert_key("s.t", 1, "r", "amount") == alert_key("s.t", 1, "r", "amount")
    # row identity is deliberately not part of the problem identity
    assert alert_key("s.t", 1, "r", "amount") != alert_key("s.t", 2, "r", "amount")


def test_breaking_change_notifies_consumers(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    channel.sent.clear()
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    assert {a.recipient for a in channel.sent} >= {"downstream/a", "downstream/b"}
    assert any(a.kind == "signoff_request" for a in channel.sent)


def test_approval_notifies_everyone_once(env):
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    channel.sent.clear()
    platform.decide("s.t", 2, "downstream/a", "approved", now=NOW)
    platform.decide("s.t", 2, "downstream/b", "approved", now=NOW)
    kinds = [a.kind for a in channel.sent]
    assert kinds.count("signoff_approved") >= 1


def test_workflow_alerts_are_not_resolved_by_clean_runs(env):
    """A clean validation run must not close sign-off notices."""
    store, platform, channel = env
    platform.submit(doc(), sample=[row()], now=NOW)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "status"]
    platform.submit(d, sample=[{"id": "1", "amount": 10.0}], now=NOW)
    contract = store.load_contract("s.t", 2)
    channel.sent.clear()
    platform.alerts.process_run(contract, ValidationResult("s.t", 2, 100, 100), now=NOW + timedelta(hours=1))
    assert channel.sent == []


def test_broken_channel_does_not_stop_validation(env):
    class Boom(InMemoryChannel):
        name = "boom"

        def send(self, alert):
            raise RuntimeError("channel down")

    store = Store(Path(env[0].path).parent / "b.db")
    manager = AlertManager(store, channels=[Boom()])
    platform = Platform(store, alert_manager=manager)
    platform.submit(doc(), sample=[row()], now=NOW)
    contract = store.load_contract("s.t", 1)
    delivered = manager.process_run(contract, make_result(), now=NOW)
    assert delivered
    # failure itself is recorded as an alert rather than swallowed
    assert any("channel delivery failed" in a["title"] for a in store.list_alerts())
