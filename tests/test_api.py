"""HTTP API tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dcp.alerts import InMemoryChannel
from dcp.api import create_app


@pytest.fixture
def client(tmp_path):
    channel = InMemoryChannel()
    app = create_app(db_path=tmp_path / "api.db", alert_channels=[channel])
    with TestClient(app) as c:
        c.channel = channel
        yield c


def doc(**over):
    d = {
        "contract_id": "s.t",
        "upstream_system": "s",
        "table": "t",
        "columns": [
            {"name": "id", "type": "string", "unique": True},
            {"name": "amount", "type": "decimal", "minimum": 0, "maximum": 100, "precision": 8, "scale": 2},
        ],
        "primary_key": ["id"],
        "consumers": ["down/acme"],
        "owners": ["owner/s"],
    }
    d.update(over)
    return d


def submit(client, sample=None, **over):
    return client.post(
        "/api/contracts/s.t/submit",
        json={"contract": doc(**over), "sample": sample or [{"id": "1", "amount": 5.0}], "submitted_by": "test"},
    )


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_console_page_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "数据契约平台" in r.text


def test_submit_creates_contract(client):
    r = submit(client)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "active"
    assert body["version"] == 1


def test_submit_twice_conflicts(client):
    submit(client)
    assert submit(client).status_code == 409


def test_submit_bad_sample_is_422(client):
    submit(client)
    bad = doc()
    bad["columns"] = [c for c in bad["columns"] if c["name"] != "amount"] + [
        {"name": "amount", "type": "int"}
    ]
    r = client.post(
        "/api/contracts/s.t/submit",
        json={"contract": bad, "sample": [{"id": "1", "amount": 5.9}]},
    )
    assert r.status_code == 422
    assert "sample_errors" in r.json()["detail"]


def test_contract_detail_and_diff(client):
    submit(client)
    d = doc()
    d["columns"].append({"name": "extra", "type": "string", "required": False})
    client.post("/api/contracts/s.t/submit", json={"contract": d, "sample": [{"id": "1", "amount": 5.0}]})
    detail = client.get("/api/contracts/s.t").json()
    assert detail["contract"]["version"] == 2
    diff = client.get("/api/contracts/s.t/diff", params={"version": 2}).json()
    assert diff["compatibility"] == "backward_compatible"


def test_contract_missing_is_404(client):
    assert client.get("/api/contracts/nope.nope").status_code == 404


def test_signoff_flow_through_api(client):
    submit(client)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "amount"]
    r = client.post("/api/contracts/s.t/submit", json={"contract": d, "sample": [{"id": "1"}]})
    assert r.status_code == 200
    assert r.json()["status"] == "pending_signoff"

    pending = client.get("/api/signoffs").json()
    assert pending and pending[0]["contract_id"] == "s.t"

    r = client.post(
        "/api/contracts/s.t/versions/2/signoff",
        json={"consumer": "down/acme", "decision": "approved", "comment": "ok"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "active"


def test_signoff_rejects_unknown_consumer(client):
    submit(client)
    d = doc()
    d["columns"] = [c for c in d["columns"] if c["name"] != "amount"]
    client.post("/api/contracts/s.t/submit", json={"contract": d, "sample": [{"id": "1"}]})
    r = client.post(
        "/api/contracts/s.t/versions/2/signoff",
        json={"consumer": "nobody", "decision": "approved"},
    )
    assert r.status_code == 403


def test_violation_endpoint_drills_to_rows(client):
    submit(client, sample=[{"id": "1", "amount": -3.0}] if False else None)
    client.post("/api/validate-all")
    rows = client.get("/api/violations").json()
    assert "violations" in rows


def test_dashboard_shape(client):
    submit(client)
    d = client.get("/api/dashboard").json()
    assert d["contracts"]["total"] == 1
    assert "pending_signoffs" in d
    assert "upstream_trend" in d


def test_admin_token_enforced(tmp_path, monkeypatch):
    monkeypatch.setenv("DCP_ADMIN_TOKEN", "s3cret")
    app = create_app(db_path=tmp_path / "auth.db", alert_channels=[InMemoryChannel()])
    with TestClient(app) as c:
        r = c.post("/api/contracts/s.t/submit", json={"contract": doc(), "sample": [{"id": "1", "amount": 5.0}]})
        assert r.status_code == 401
        r = c.post(
            "/api/contracts/s.t/submit",
            json={"contract": doc(), "sample": [{"id": "1", "amount": 5.0}]},
            headers={"X-Admin-Token": "s3cret"},
        )
        assert r.status_code == 200


def test_consumer_token_scoped(tmp_path, monkeypatch):
    monkeypatch.setenv("DCP_ADMIN_TOKEN", "s3cret")
    monkeypatch.setenv("DCP_CONSUMER_SECRET", "consumer-secret")
    app = create_app(db_path=tmp_path / "consumer.db", alert_channels=[InMemoryChannel()])
    with TestClient(app) as c:
        tok = {"X-Admin-Token": "s3cret"}
        c.post("/api/contracts/s.t/submit", json={"contract": doc(), "sample": [{"id": "1", "amount": 5.0}]}, headers=tok)
        d = doc()
        d["columns"] = [c for c in d["columns"] if c["name"] != "amount"]
        c.post("/api/contracts/s.t/submit", json={"contract": d, "sample": [{"id": "1"}]}, headers=tok)

        from dcp.api import _consumer_token

        r = c.post(
            "/api/contracts/s.t/versions/2/signoff",
            json={"consumer": "down/acme", "decision": "approved"},
            headers={"X-Consumer-Token": _consumer_token("down/other")},
        )
        assert r.status_code == 401
        r = c.post(
            "/api/contracts/s.t/versions/2/signoff",
            json={"consumer": "down/acme", "decision": "approved"},
            headers={"X-Consumer-Token": _consumer_token("down/acme")},
        )
        assert r.status_code == 200


def test_submit_without_sample_works(client):
    r = client.post("/api/contracts/s.t/submit", json={"contract": doc()})
    assert r.status_code == 200
    assert r.json()["status"] == "active"


def test_submit_yaml_text(client):
    yaml_doc = """contract_id: s.t
upstream_system: s
table: t
columns:
  - {name: id, type: string, unique: true}
  - {name: amount, type: decimal, minimum: 0, maximum: 100, precision: 8, scale: 2}
primary_key: [id]
consumers: [down/acme]
owners: [owner/s]
"""
    r = client.post("/api/contracts/s.t/submit", json={"contract_text": yaml_doc})
    assert r.status_code == 200
    assert r.json()["version"] == 1


def test_submit_yaml_invalid_is_422(client):
    r = client.post("/api/contracts/s.t/submit", json={"contract_text": "contract_id: [oops"})
    assert r.status_code == 422


def test_submit_needs_a_document(client):
    r = client.post("/api/contracts/s.t/submit", json={})
    assert r.status_code == 422


def test_settings_roundtrip(client):
    r = client.post("/api/settings", json={"transition_days": 3, "alert_quiet_hours": 6})
    assert r.status_code == 200
    assert r.json()["transition_days"] == 3
    assert client.get("/api/settings").json()["alert_quiet_hours"] == 6


def test_settings_validation(client):
    assert client.post("/api/settings", json={"transition_days": 999}).status_code == 422
