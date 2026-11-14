"""FastAPI application: the admin backend.

Endpoints are grouped by the four sections of the console:

* contracts      — catalog, versions, diffs, submission
* signoff        — pending approvals and decisions
* validation     — runs, violations, row drilldown
* alerts         — alert feed, trends, acknowledgements

Auth is a shared admin token, optional in development.  Write endpoints require
it when ``DCP_ADMIN_TOKEN`` is set; consumer sign-off uses a per-consumer token
derived from the same secret so that one consumer cannot approve for another.
"""

from __future__ import annotations

import hashlib
import json
import hmac
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .alerts import AlertManager, OutboxChannel
from .config import settings
from .contract import ContractError, dump_contract_flow, load_contract_text
from .platform import Platform, PlatformError
from .store import Store

STATIC_DIR = Path(__file__).resolve().parent / "static"


class SubmitPayload(BaseModel):
    contract: dict[str, Any] | None = Field(
        default=None, description="Contract document as JSON (same shape as YAML)"
    )
    contract_text: str | None = Field(
        default=None, description="Contract document as YAML text; alternative to `contract`"
    )
    sample: list[dict[str, Any]] | None = Field(
        default=None, description="Example rows shipped with the submission"
    )
    submitted_by: str = "upstream"
    note: str | None = None


class DecisionPayload(BaseModel):
    consumer: str
    decision: str
    comment: str | None = None


class ValidatePayload(BaseModel):
    version: int | None = None
    use_sample: bool = False
    trigger: str = "manual"


def create_app(
    *,
    db_path: str | Path | None = None,
    alert_channels: list | None = None,
    sample_rows: int | None = None,
) -> FastAPI:
    db = Path(db_path) if db_path else settings.database
    store = Store(db)
    channels = alert_channels if alert_channels is not None else [
        OutboxChannel(db.parent / "alerts.jsonl")
    ]
    manager = AlertManager(
        store,
        channels=channels,
        quiet_hours=int(store.get_setting("alert_quiet_hours", str(settings.alert_quiet_hours)) or 24),
    )
    platform = Platform(
        store,
        alert_manager=manager,
        sample_rows=sample_rows or int(store.get_setting("sample_rows", str(settings.sample_rows)) or 2000),
        transition_days=int(store.get_setting("transition_days", str(settings.transition_days)) or 7),
    )

    app = FastAPI(title="Data Contract Platform", version="0.1.0")
    app.state.store = store
    app.state.platform = platform

    def require_admin(token: str | None) -> None:
        expected = os.environ.get("DCP_ADMIN_TOKEN", "")
        if not expected:
            return
        if not token or not hmac.compare_digest(token, expected):
            raise HTTPException(status_code=401, detail="admin token required")

    def guard(fn):
        try:
            return fn()
        except PlatformError as e:
            raise HTTPException(status_code=e.status_code, detail={"message": str(e), **(e.detail or {})})
        except ContractError as e:
            raise HTTPException(status_code=422, detail={"message": str(e)})

    # ---------------- health / meta ----------------

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "contracts": len(platform.contracts()),
            "time": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

    @app.get("/api/settings")
    def get_settings() -> dict[str, Any]:
        return {
            "transition_days": platform.transition_days,
            "sample_rows": platform.sample_rows,
            "alert_quiet_hours": manager.quiet_hours,
            "auth_required": bool(os.environ.get("DCP_ADMIN_TOKEN")),
        }

    @app.post("/api/settings")
    def update_settings(payload: dict[str, Any], x_admin_token: str | None = Header(default=None)):
        require_admin(x_admin_token)
        if "transition_days" in payload:
            v = int(payload["transition_days"])
            if not 0 <= v <= 90:
                raise HTTPException(422, "transition_days must be 0..90")
            platform.transition_days = v
            store.set_setting("transition_days", str(v))
        if "sample_rows" in payload:
            v = int(payload["sample_rows"])
            if not 10 <= v <= 100000:
                raise HTTPException(422, "sample_rows must be 10..100000")
            platform.sample_rows = v
            store.set_setting("sample_rows", str(v))
        if "alert_quiet_hours" in payload:
            v = int(payload["alert_quiet_hours"])
            if not 0 <= v <= 720:
                raise HTTPException(422, "alert_quiet_hours must be 0..720")
            manager.quiet_hours = v
            store.set_setting("alert_quiet_hours", str(v))
        return get_settings()

    # ---------------- contracts ----------------

    @app.get("/api/contracts")
    def list_contracts() -> list[dict[str, Any]]:
        return platform.contracts()

    @app.get("/api/contracts/{contract_id}")
    def get_contract(contract_id: str, version: int | None = None) -> dict[str, Any]:
        return guard(lambda: platform.contract_detail(contract_id, version))

    @app.get("/api/contracts/{contract_id}/diff")
    def get_diff(contract_id: str, version: int) -> dict[str, Any]:
        return guard(lambda: platform.diff(contract_id, version))

    @app.get("/api/contracts/{contract_id}/versions")
    def get_versions(contract_id: str) -> list[dict[str, Any]]:
        return [
            {k: v[k] for k in ("version", "status", "compatibility", "submitted_at", "submitted_by", "schema_hash")}
            for v in store.list_versions(contract_id)
        ]

    @app.post("/api/contracts/{contract_id}/submit")
    def submit(
        contract_id: str,
        payload: SubmitPayload,
        x_admin_token: str | None = Header(default=None),
    ) -> dict[str, Any]:
        require_admin(x_admin_token)
        if payload.contract is not None:
            raw = dict(payload.contract)
        elif payload.contract_text:
            try:
                raw = load_contract_text(payload.contract_text)  # type: ignore[assignment]
            except ContractError as e:
                raise HTTPException(422, {"message": f"invalid contract document: {e}"})
            raw = dump_contract_flow(raw)
        else:
            raise HTTPException(422, "provide `contract` (JSON) or `contract_text` (YAML)")
        if raw.get("contract_id") not in (None, contract_id):
            raise HTTPException(422, "contract_id in body does not match path")
        raw["contract_id"] = contract_id
        return guard(
            lambda: platform.submit(
                raw,
                sample=payload.sample,
                submitted_by=payload.submitted_by,
                note=payload.note,
            )
        )

    @app.post("/api/contracts/{contract_id}/validate")
    def validate_contract(
        contract_id: str,
        payload: ValidatePayload | None = None,
        x_admin_token: str | None = Header(default=None),
    ) -> list[dict[str, Any]]:
        require_admin(x_admin_token)
        p = payload or ValidatePayload()
        return guard(
            lambda: platform.validate_contract(
                contract_id, version=p.version, use_sample=p.use_sample, trigger=p.trigger
            )
        )

    @app.post("/api/validate-all")
    def validate_all(x_admin_token: str | None = Header(default=None)) -> list[dict[str, Any]]:
        require_admin(x_admin_token)
        return guard(lambda: platform.validate_all(trigger="scheduled"))

    # ---------------- sign-off ----------------

    @app.get("/api/signoffs")
    def list_pending_signoffs() -> list[dict[str, Any]]:
        out = []
        for row in store.pending_versions():
            out.append(
                {
                    "contract_id": row["contract_id"],
                    "version": row["version"],
                    "compatibility": row["compatibility"],
                    "submitted_by": row["submitted_by"],
                    "submitted_at": row["submitted_at"],
                    "signoffs": store.list_signoffs(row["contract_id"], row["version"]),
                    "changes": store.get_changes(row["contract_id"], row["version"]),
                }
            )
        return out

    @app.post("/api/contracts/{contract_id}/versions/{version}/signoff")
    def signoff(
        contract_id: str,
        version: int,
        payload: DecisionPayload,
        x_admin_token: str | None = Header(default=None),
        x_consumer_token: str | None = Header(default=None),
    ) -> dict[str, Any]:
        # A consumer token may decide only for itself.
        if x_consumer_token and not x_admin_token:
            expected = _consumer_token(payload.consumer)
            if not hmac.compare_digest(x_consumer_token, expected):
                raise HTTPException(401, "invalid consumer token")
        else:
            require_admin(x_admin_token)
        return guard(
            lambda: platform.decide(
                contract_id, version, payload.consumer, payload.decision, comment=payload.comment
            )
        )

    @app.get("/api/contracts/{contract_id}/versions/{version}/signoffs")
    def get_signoffs(contract_id: str, version: int) -> list[dict[str, Any]]:
        return store.list_signoffs(contract_id, version)

    # ---------------- validation & violations ----------------

    @app.get("/api/runs")
    def list_runs(contract_id: str | None = None, limit: int = Query(50, le=500)) -> list[dict[str, Any]]:
        return platform.runs_list(contract_id=contract_id, limit=limit)

    @app.get("/api/violations")
    def list_violations(
        contract_id: str | None = None,
        run_id: str | None = None,
        rule_name: str | None = None,
        severity: str | None = None,
        limit: int = Query(100, le=1000),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        rows = platform.violations_list(
            contract_id=contract_id,
            run_id=run_id,
            rule_name=rule_name,
            severity=severity,
            limit=limit,
            offset=offset,
        )
        for r in rows:
            r["row_pk"] = json.loads(r["row_pk"]) if r["row_pk"] else None
            r["row_data"] = json.loads(r["row_data"]) if r["row_data"] else None
        return {"violations": rows, "count": len(rows), "limit": limit, "offset": offset}

    @app.get("/api/contracts/{contract_id}/violation-summary")
    def violation_summary(contract_id: str) -> list[dict[str, Any]]:
        return store.violation_counts_by_rule(contract_id)

    # ---------------- alerts ----------------

    @app.get("/api/alerts")
    def list_alerts(limit: int = Query(100, le=500), include_suppressed: bool = True) -> list[dict[str, Any]]:
        return platform.alerts_list(limit=limit, include_suppressed=include_suppressed)

    @app.get("/api/dashboard")
    def dashboard() -> dict[str, Any]:
        return platform.dashboard()

    @app.get("/api/trends")
    def trends() -> dict[str, Any]:
        return platform.trend()

    @app.post("/api/transitions/finalize")
    def finalize(x_admin_token: str | None = Header(default=None)) -> list[dict[str, Any]]:
        require_admin(x_admin_token)
        return platform.finalize_transitions()

    # ---------------- console ----------------

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))

    return app


def _consumer_token(consumer: str) -> str:
    secret = os.environ.get("DCP_CONSUMER_SECRET") or os.environ.get("DCP_ADMIN_TOKEN") or "dev-secret"
    return hmac.new(secret.encode(), consumer.encode(), hashlib.sha256).hexdigest()[:32]


app = create_app()
