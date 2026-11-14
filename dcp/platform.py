"""Service layer: submission gate, sign-off workflow, rollout, validation, reporting.

This is the single place where workflow state transitions happen, so the HTTP API
and the CLI/simulator share identical semantics.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .alerts import AlertManager, alert_key
from .contract import Contract, ContractError, parse_contract
from .diff import ChangeSet, diff_contracts
from .store import Store
from .validator import (
    UpstreamData,
    ValidationResult,
    fetch_upstream,
    run_validation,
    validate_submission_sample,
    validate_rows,
)

ACTIVE_FOR_VALIDATION = ("active", "parallel")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


class PlatformError(Exception):
    def __init__(self, message: str, status_code: int = 400, detail: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


def _normalize_sample_rows(contract: Contract, sample: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Fill omitted nullable columns with NULL; leave missing required columns alone.

    Submitters often omit optional columns from examples.  Treating that as
    "missing" would reject valid submissions, so only required columns remain
    a hard error.
    """
    if not sample:
        return []
    out = []
    optional = {c.name for c in contract.columns if not c.required}
    for row in sample:
        if not isinstance(row, dict):
            out.append(row)
            continue
        filled = dict(row)
        for name in optional:
            filled.setdefault(name, None)
        out.append(filled)
    return out


def profile_rows(rows: list[dict[str, Any]], columns: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Small data profile used as evidence for rename detection."""
    out: dict[str, dict[str, Any]] = {}
    for col in columns:
        values = [r.get(col) for r in rows if col in r]
        non_null = [v for v in values if v is not None]
        distinct = []
        seen = set()
        for v in non_null:
            k = repr(v)
            if k in seen:
                continue
            seen.add(k)
            distinct.append(v)
            if len(distinct) >= 20:
                break
        out[col] = {
            "null_ratio": round((len(values) - len(non_null)) / len(values), 4) if values else 0.0,
            "sample_values": distinct,
        }
    return out


class Platform:
    def __init__(
        self,
        store: Store,
        *,
        alert_manager: AlertManager | None = None,
        sample_rows: int = 2000,
        transition_days: int = 7,
        upstream_dir: str | None = None,
    ):
        self.store = store
        self.alerts = alert_manager or AlertManager(store)
        self.sample_rows = sample_rows
        self.transition_days = transition_days

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit(
        self,
        document: dict[str, Any],
        *,
        sample: list[dict[str, Any]] | None = None,
        submitted_by: str = "upstream",
        note: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or utcnow()
        now_s = iso(now)

        contract_id = document.get("contract_id") if isinstance(document, dict) else None
        if not contract_id:
            raise PlatformError("contract_id is required", 422)

        latest = self.store.latest_version(contract_id)
        next_version = (latest["version"] + 1) if latest else 1

        # One open version at a time.  If a change is awaiting sign-off, a second
        # submission would be diffed against the still-active version and conflate
        # two unrelated change sets; make the upstream finish or withdraw first.
        if latest and latest["status"] == "pending_signoff":
            raise PlatformError(
                f"v{latest['version']} is awaiting sign-off; resolve it before submitting v{next_version}",
                409,
                {"pending_version": latest["version"]},
            )

        # Metadata the submitter omitted is inherited from the baseline rather
        # than silently erased.  An explicit empty list still means "remove".
        if isinstance(document, dict):
            baseline_row = self.store.active_version(contract_id) or latest
            if baseline_row:
                previous = json.loads(baseline_row["document"])
                document = dict(document)
                for key in ("description", "consumers", "owners"):
                    if key not in document and key in previous:
                        document[key] = previous[key]

        try:
            contract = parse_contract(document, version=next_version)
        except ContractError as e:
            raise PlatformError(f"contract rejected: {e}", 422, {"path": e.path}) from None

        if contract.contract_id != contract_id:
            raise PlatformError("contract_id mismatch", 422)

        # Baseline: the version consumers currently rely on.  Falling back to the
        # latest submission keeps pending work from being diffed against stale schemas.
        baseline_row = self.store.active_version(contract_id) or latest
        baseline = None
        old_profiles: dict[str, dict] | None = None
        if baseline_row:
            baseline = self.store.load_contract(contract_id, baseline_row["version"])
            if baseline:
                for role in ("active", "previous", "candidate"):
                    stored = self.store.get_sample(contract_id, baseline.version, role)
                    if stored:
                        old_profiles = profile_rows(stored[:500], baseline.column_names)
                        break

        # Submission-time sample gate (requirement 6).
        sample_errors: list[str] = []
        normalized: list[dict[str, Any]] = []
        if sample is not None:
            # Normalize rows to the contract's declared shape: a sample may omit
            # columns whose absence simply maps to NULL, provided they are
            # nullable, so fill them before validating.
            normalized = _normalize_sample_rows(contract, sample)
            sample_errors = validate_submission_sample(contract, normalized)
            if sample_errors:
                self.store.save_changes(
                    contract_id,
                    next_version,
                    {
                        "rejected_at_submission": True,
                        "errors": sample_errors,
                        "contract": contract.to_dict(),
                    },
                    now_s,
                )
                raise PlatformError(
                    "submission sample does not satisfy the submitted contract",
                    422,
                    {"sample_errors": sample_errors, "version": next_version},
                )
        elif baseline is not None:
            # A breaking schema change submitted without example data is a
            # warning, not a blocker; rename inference falls back to names+constraints.
            pass

        new_profiles = profile_rows(normalized[:500], contract.column_names) if sample else None
        change_set: ChangeSet = diff_contracts(
            baseline, contract, old_profiles=old_profiles, new_profiles=new_profiles
        )

        if change_set.compatibility == "no_change":
            raise PlatformError(
                f"contract v{next_version} is identical to v{baseline.version if baseline else 0}",
                409,
                {"schema_hash": contract.schema_hash},
            )

        if sample:
            self.store.save_sample(contract_id, next_version, "candidate", normalized, now_s)

        breaking = change_set.breaking
        signoffs: list[dict[str, Any]] = []

        if baseline is None:
            compatibility = "initial"
            status = "active"
        elif change_set.requires_signoff and breaking:
            compatibility = "breaking"
            if contract.consumers:
                status = "pending_signoff"
            else:
                # Nobody registered to approve; the change is released but the
                # absence of consumers is recorded as a warning.
                status = "active"
                note = (note + " " if note else "") + "[no registered consumers: auto-released]"
        else:
            compatibility = "backward_compatible"
            status = "active"

        self.store.insert_version(
            contract,
            parent_version=baseline.version if baseline else None,
            compatibility=compatibility,
            submitted_by=submitted_by,
            submitted_at=now_s,
            status=status,
            note=note,
        )
        self.store.save_changes(contract_id, contract.version, change_set.to_dict(), now_s)

        activated_at = None
        transition_ends_at = None
        if status == "active":
            activated_at = now_s
            if baseline is not None:
                if breaking:
                    # Auto-released (no registered consumers) still gets the
                    # parallel window: the old version keeps validating so a
                    # downstream failure is detectable, not silent.
                    transition_ends_at = iso(now + timedelta(days=self.transition_days))
                    self._parallel_rollout(contract_id, baseline.version, transition_ends_at)
                else:
                    self.store.supersede_older(contract_id, contract.version)
            self._notify_compatible(contract, change_set) if not breaking else None

        if status == "pending_signoff":
            self.store.ensure_signoffs(contract_id, contract.version, contract.consumers)
            signoffs = self.store.list_signoffs(contract_id, contract.version)
            if breaking:
                self.alerts.notify_breaking_change(contract, change_set, now=now)

        return {
            "contract_id": contract_id,
            "version": contract.version,
            "status": status,
            "compatibility": compatibility,
            "schema_hash": contract.schema_hash,
            "changes": change_set.to_dict(),
            "signoffs": signoffs,
            "sample_rows": len(normalized or []),
            "submitted_by": submitted_by,
            "activated_at": activated_at,
            "transition_ends_at": transition_ends_at,
        }

    def _parallel_rollout(self, contract_id: str, old_version: int, ends_at: str) -> None:
        self.store.set_version_status(contract_id, old_version, "parallel", transition_ends_at=ends_at)
        self.store.supersede_older(contract_id, old_version)

    def _notify_compatible(self, contract: Contract, change_set: ChangeSet) -> None:
        if not change_set.compatible:
            return
        # low-noise: compatible releases are recorded, not paged.

    # ------------------------------------------------------------------
    # Sign-off
    # ------------------------------------------------------------------

    def decide(
        self,
        contract_id: str,
        version: int,
        consumer: str,
        decision: str,
        *,
        comment: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or utcnow()
        row = self.store.get_version(contract_id, version)
        if not row:
            raise PlatformError("version not found", 404)
        if row["status"] != "pending_signoff":
            raise PlatformError(f"version {version} is not awaiting sign-off (status={row['status']})", 409)
        contract = self.store.load_contract(contract_id, version)
        assert contract is not None
        if consumer not in contract.consumers:
            raise PlatformError(f"{consumer} is not a registered consumer of {contract_id}", 403)

        ok = self.store.decide_signoff(
            contract_id, version, consumer, decision, iso(now), comment
        )
        if not ok:
            raise PlatformError("sign-off record not found", 404)

        signoffs = self.store.list_signoffs(contract_id, version)
        rejected = [s for s in signoffs if s["decision"] == "rejected"]
        approved = [s for s in signoffs if s["decision"] == "approved"]

        result: dict[str, Any] = {
            "contract_id": contract_id,
            "version": version,
            "decision": decision,
            "consumer": consumer,
            "signoffs": signoffs,
            "status": "pending_signoff",
        }

        if rejected:
            self.store.set_version_status(contract_id, version, "rejected")
            existing = self.store.get_changes(contract_id, version) or {}
            existing["rejected_by"] = [s["consumer"] for s in rejected]
            existing["rejected_at"] = iso(now)
            self.store.save_changes(contract_id, version, existing, iso(now))
            result["status"] = "rejected"
            self._notify_decision(contract, approved, rejected, now)
            return result

        if len(approved) == len(signoffs) and signoffs:
            ends_at = iso(now + timedelta(days=self.transition_days))
            self.store.set_version_status(
                contract_id, version, "active", activated_at=iso(now), transition_ends_at=ends_at
            )
            prev = self.store.previous_active_version(contract_id)
            if not prev:
                # fall back to the parent recorded on the submission
                parent = row.get("parent_version")
                if parent:
                    self.store.set_version_status(
                        contract_id, parent, "parallel", transition_ends_at=ends_at
                    )
                    self.store.supersede_older(contract_id, parent)
            else:
                self.store.set_version_status(
                    contract_id, prev["version"], "parallel", transition_ends_at=ends_at
                )
                self.store.supersede_older(contract_id, prev["version"])
            # supersede any other pending/parallel tail
            result["status"] = "active"
            result["activated_at"] = iso(now)
            result["transition_ends_at"] = ends_at
            self.alerts.notify_signoff_complete(contract, now=now)
        return result

    def _notify_decision(self, contract, approved, rejected, now) -> None:
        # Rejection is recorded and pushed to owners via the normal alert path.
        from .alerts import Alert

        for s in rejected:
            for r in (contract.owners or ["data-platform-oncall"]):
                alert = Alert(
                    alert_key=f"reject-{contract.contract_id}-{contract.version}",
                    contract_id=contract.contract_id,
                    version=contract.version,
                    rule_name=None,
                    severity="high",
                    audience="owner",
                    recipient=r,
                    channel=self.alerts.channels[0].name if self.alerts.channels else "outbox",
                    title=f"{contract.contract_id} v{contract.version} rejected by {s['consumer']}",
                    body=s.get("comment") or "Consumer rejected the breaking change.",
                    created_at=iso(now),
                    kind="signoff_rejected",
                )
                self.alerts._deliver(alert)

    def finalize_transitions(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        now = now or utcnow()
        now_s = iso(now)
        finalized = []
        rows = self.store.connect().execute(
            "SELECT contract_id, version, transition_ends_at FROM contract_versions "
            "WHERE status='parallel' AND transition_ends_at IS NOT NULL"
        ).fetchall()
        for r in rows:
            end = r["transition_ends_at"]
            if end and end <= now_s:
                self.store.set_version_status(r["contract_id"], r["version"], "superseded")
                finalized.append({"contract_id": r["contract_id"], "version": r["version"], "ended_at": now_s})
        return finalized

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def versions_to_validate(self, contract_id: str) -> list[dict[str, Any]]:
        rows = self.store.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? AND status IN ('active','parallel') "
            "ORDER BY version DESC",
            (contract_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def validate_contract(
        self,
        contract_id: str,
        *,
        version: int | None = None,
        use_sample: bool = False,
        trigger: str = "manual",
        now: datetime | None = None,
        alert: bool = True,
    ) -> list[dict[str, Any]]:
        now = now or utcnow()
        self.finalize_transitions(now=now)

        if version is not None:
            row = self.store.get_version(contract_id, version)
            if not row:
                raise PlatformError("version not found", 404)
            versions = [row]
        else:
            versions = self.versions_to_validate(contract_id)
        if not versions:
            raise PlatformError("no active contract version to validate", 409)

        results = []
        for row in versions:
            results.append(
                self._validate_version(
                    contract_id, row, use_sample=use_sample, trigger=trigger, now=now, alert=alert
                )
            )
        return results

    def validate_all(self, *, trigger: str = "scheduled", now: datetime | None = None) -> list[dict[str, Any]]:
        now = now or utcnow()
        self.finalize_transitions(now=now)
        out = []
        seen = set()
        for row in self.store.connect().execute(
            "SELECT contract_id, version FROM contract_versions "
            "WHERE status IN ('active','parallel') ORDER BY contract_id, version DESC"
        ).fetchall():
            cid = row["contract_id"]
            if cid in seen:
                continue
            seen.add(cid)
            try:
                out.extend(self.validate_contract(cid, trigger=trigger, now=now))
            except PlatformError as e:
                out.append({"contract_id": cid, "error": str(e)})
        return out

    def _validate_version(
        self,
        contract_id: str,
        row: dict[str, Any],
        *,
        use_sample: bool,
        trigger: str,
        now: datetime,
        alert: bool,
    ) -> dict[str, Any]:
        contract = self.store.load_contract(contract_id, row["version"])
        assert contract is not None
        run_id = uuid.uuid4().hex
        started = iso(now)
        wall_start = time.perf_counter()
        role = "active" if row["status"] == "active" else "previous"

        self.store.start_run(run_id, contract_id, row["version"], started, trigger, role)

        upstream_row = self.store.upstream_table(contract.upstream_system, contract.table)
        if use_sample:
            stored = (
                self.store.get_sample(contract_id, row["version"], "candidate")
                if row["status"] != "parallel"
                else None
            ) or self.store.get_sample(contract_id, row["version"], role)
            if stored is None:
                self.store.finish_run(
                    run_id, finished_at=started, rows_checked=0, rows_clean=0,
                    violation_count=0, status="failed", error="no stored sample for this version",
                )
                raise PlatformError("no stored sample to validate against", 409)
            data = UpstreamData(
                columns=contract.column_names,
                rows=stored[: self.sample_rows],
                sampled=len(stored) > self.sample_rows,
                total=len(stored),
            )
        else:
            if not upstream_row:
                self.store.finish_run(
                    run_id, finished_at=started, rows_checked=0, rows_clean=0,
                    violation_count=0, status="failed",
                    error=f"upstream table {contract.upstream_system}.{contract.table} not registered",
                )
                raise PlatformError("upstream table not registered", 409)
            try:
                data = fetch_upstream(upstream_row["db_path"], contract.table, self.sample_rows)
            except Exception as e:
                self.store.finish_run(
                    run_id, finished_at=started, rows_checked=0, rows_clean=0,
                    violation_count=0, status="failed", error=f"upstream read failed: {e}",
                )
                raise PlatformError(f"upstream read failed: {e}", 502) from None

        result: ValidationResult = run_validation(contract, data, now=now)

        self.store.save_sample(
            contract_id, row["version"], role, data.rows, iso(now)
        )

        violations_rows = []
        for v in result.violations:
            violations_rows.append(
                {
                    "violation_key": v.key(contract_id, row["version"]),
                    "run_id": run_id,
                    "contract_id": contract_id,
                    "version": row["version"],
                    "rule_name": v.rule_name,
                    "rule_kind": v.rule_kind,
                    "column_name": v.column,
                    "severity": v.severity,
                    "row_pk": json.dumps(v.row_pk, sort_keys=True, default=str) if v.row_pk else None,
                    "row_data": json.dumps(v.row_data, default=str) if v.row_data else None,
                    "detail": v.detail,
                    "observed_at": iso(now),
                }
            )
        self.store.add_violations(violations_rows)
        elapsed_ms = int((time.perf_counter() - wall_start) * 1000)
        self.store.finish_run(
            run_id,
            finished_at=iso(now),
            elapsed_ms=elapsed_ms,
            rows_checked=result.rows_checked,
            rows_clean=result.rows_clean,
            violation_count=result.violation_count,
            status="completed",
        )

        delivered = self.alerts.process_run(contract, result, now=now) if alert else []

        out = {
            "run_id": run_id,
            "contract_id": contract_id,
            "version": row["version"],
            "role": role,
            "rows_checked": result.rows_checked,
            "rows_total": result.rows_total,
            "rows_clean": result.rows_clean,
            "violation_count": result.violation_count,
            "clean_ratio": round(result.clean_ratio, 4),
            "sampled": result.sampled,
            "by_rule": result.by_rule,
            "structural_errors": result.structural_errors,
            "started_at": started,
            "elapsed_ms": elapsed_ms,
            "alerts_delivered": [
                {"recipient": a.recipient, "severity": a.severity, "title": a.title, "kind": a.kind}
                for a in delivered
            ],
        }
        return out

    # ------------------------------------------------------------------
    # Candidate dry-run (submission sample against proposed contract)
    # ------------------------------------------------------------------

    def dry_run_sample(self, contract_id: str, version: int) -> dict[str, Any]:
        row = self.store.get_version(contract_id, version)
        if not row:
            raise PlatformError("version not found", 404)
        contract = self.store.load_contract(contract_id, version)
        sample = self.store.get_sample(contract_id, version, "candidate")
        if not sample:
            raise PlatformError("no submission sample stored for this version", 409)
        assert contract is not None
        result = validate_rows(contract, sample, actual_columns=contract.column_names)
        return {
            "contract_id": contract_id,
            "version": version,
            "rows": len(sample),
            "violations": [
                {"rule": v.rule_name, "severity": v.severity, "detail": v.detail, "row": v.row_pk}
                for v in result.violations
            ],
            "violation_count": len(result.violations),
        }

    # ------------------------------------------------------------------
    # Read models / reporting
    # ------------------------------------------------------------------

    def contracts(self) -> list[dict[str, Any]]:
        return self.store.list_contracts()

    def contract_detail(self, contract_id: str, version: int | None = None) -> dict[str, Any]:
        row = self.store.get_version(contract_id, version) if version else self.store.latest_version(contract_id)
        if not row:
            raise PlatformError("contract not found", 404)
        contract = self.store.load_contract(contract_id, row["version"])
        assert contract is not None
        versions = self.store.list_versions(contract_id)
        active = self.store.active_version(contract_id)
        parallel = [
            v for v in versions if v["status"] == "parallel"
        ]
        out = {
            "contract": contract.to_dict(),
            "version_row": row,
            "versions": [
                {
                    k: v[k]
                    for k in (
                        "version", "status", "compatibility", "submitted_by",
                        "submitted_at", "activated_at", "transition_ends_at", "schema_hash",
                    )
                }
                for v in versions
            ],
            "changes": self.store.get_changes(contract_id, row["version"]),
            "signoffs": self.store.list_signoffs(contract_id, row["version"]),
            "violation_summary": self.store.violation_counts_by_rule(contract_id),
            "active_version": active["version"] if active else None,
            "parallel_versions": [v["version"] for v in parallel],
            "upstream": self.store.upstream_table(contract.upstream_system, contract.table),
        }
        return out

    def diff(self, contract_id: str, version: int) -> dict[str, Any]:
        row = self.store.get_version(contract_id, version)
        if not row:
            raise PlatformError("version not found", 404)
        changes = self.store.get_changes(contract_id, version)
        if changes is None:
            raise PlatformError("no recorded diff for this version", 404)
        return changes

    def dashboard(self) -> dict[str, Any]:
        contracts = self.contracts()
        runs = self.store.list_runs(limit=100)
        violations = self.store.list_violations(limit=200)
        alerts = self.store.list_alerts(limit=200)
        pending = self.store.pending_versions()
        open_alerts = [
            a for a in alerts
            if a["resolved_at"] is None and not a["suppressed"] and a.get("kind") == "violation"
        ]
        return {
            "contracts": {
                "total": len(contracts),
                "active": sum(1 for c in contracts if c["status"] == "active"),
                "pending_signoff": sum(1 for c in contracts if c["status"] == "pending_signoff"),
                "upstreams": sorted({c["upstream_system"] for c in contracts}),
            },
            "pending_signoffs": [
                {
                    "contract_id": p["contract_id"],
                    "version": p["version"],
                    "submitted_by": p["submitted_by"],
                    "submitted_at": p["submitted_at"],
                    "signoffs": self.store.list_signoffs(p["contract_id"], p["version"]),
                }
                for p in pending
            ],
            "runs": runs[:20],
            "latest_run": runs[0] if runs else None,
            "violations": {
                "recent": violations[:20],
                "by_severity": _count_by(violations, "severity"),
                "by_rule": _count_by(violations, "rule_name")[:10],
                "total_recent": len(violations),
            },
            "alerts": {
                "open": len(open_alerts),
                "recent": alerts[:20],
                "suppressed_recent": sum(1 for a in alerts if a["suppressed"]),
            },
            "upstream_trend": self.store.violation_trend(),
            "upstream_summary": self.store.upstream_summary(),
            "contracts_detail": contracts,
        }

    def alerts_list(self, limit: int = 200, include_suppressed: bool = True) -> list[dict[str, Any]]:
        return self.store.list_alerts(limit=limit, include_suppressed=include_suppressed)

    def runs_list(self, contract_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.store.list_runs(contract_id=contract_id, limit=limit)

    def violations_list(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.store.list_violations(**kwargs)

    def trend(self) -> dict[str, Any]:
        return {
            "by_day_upstream": self.store.violation_trend(),
            "by_upstream": self.store.upstream_summary(),
        }


def _count_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counts: dict[Any, int] = {}
    for r in rows:
        counts[r.get(key)] = counts.get(r.get(key), 0) + 1
    return sorted(({key: k, "count": v} for k, v in counts.items()), key=lambda x: -x["count"])
