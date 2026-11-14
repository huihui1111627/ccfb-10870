"""Alert routing, severity escalation and deduplication.

Principles:

* An alert's identity is the *problem*, not the observation: ``alert_key`` is
  derived from contract + version + rule + column, never from an individual row.
  A thousand bad rows in one column are one alert with ``occurrence_count`` 1000.
* Severity decides the audience.  ``critical`` reaches upstream owners and every
  registered consumer; ``high`` reaches owners; ``medium``/``low`` are rolled into
  a per-owner daily digest.
* Re-notification is rate limited by a quiet window (default 24h).  Repeated
  observations inside the window are recorded as suppressed occurrences so the
  history stays auditable without paging anyone again.
* When a problem stops being observed in a subsequent run, its state is closed
  and a resolution notice is emitted once.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from .contract import Contract
from .store import Store
from .validator import ValidationResult

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def alert_key(contract_id: str, version: int, rule_name: str, column: str | None) -> str:
    raw = f"{contract_id}|{version}|{rule_name}|{column or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


@dataclass
class Alert:
    alert_key: str
    contract_id: str
    version: int
    rule_name: str | None
    severity: str
    audience: str
    recipient: str
    channel: str
    title: str
    body: str
    created_at: str
    resolved_at: str | None = None
    suppressed: int = 0
    occurrence_count: int = 1
    kind: str = "violation"  # violation | resolution | digest

    def to_row(self) -> dict[str, Any]:
        return {
            "alert_key": self.alert_key,
            "contract_id": self.contract_id,
            "version": self.version,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "audience": self.audience,
            "recipient": self.recipient,
            "channel": self.channel,
            "title": self.title,
            "body": self.body,
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
            "suppressed": self.suppressed,
            "occurrence_count": self.occurrence_count,
            "kind": self.kind,
        }


class Channel(Protocol):
    name: str

    def send(self, alert: Alert) -> None: ...


class OutboxChannel:
    """Append-only JSONL outbox; what a real webhook/IM integration would ship."""

    name = "outbox"

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, alert: Alert) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"kind": alert.kind, **alert.to_row()}, default=str) + "\n")


class ConsoleChannel:
    name = "console"

    def __init__(self, enabled: bool = False):
        self.enabled = enabled

    def send(self, alert: Alert) -> None:
        if self.enabled:
            print(f"[{alert.severity.upper()}] -> {alert.recipient}: {alert.title}")


class InMemoryChannel:
    """Used by tests and the evaluation harness to capture deliveries."""

    name = "memory"

    def __init__(self) -> None:
        self.sent: list[Alert] = []

    def send(self, alert: Alert) -> None:
        self.sent.append(alert)


def audience_for(severity: str) -> str:
    if severity == "critical":
        return "both"
    if severity == "high":
        return "owner"
    return "digest"


class AlertManager:
    def __init__(
        self,
        store: Store,
        *,
        channels: list[Channel] | None = None,
        quiet_hours: int = 24,
        digest: bool = True,
    ):
        self.store = store
        self.channels = channels if channels is not None else [OutboxChannel(Path(store.path).parent / "alerts.jsonl")]
        self.quiet_hours = quiet_hours
        self.digest = digest

    # -- recipients --------------------------------------------------------

    def recipients_for(self, contract: Contract, audience: str) -> list[tuple[str, str]]:
        owners = list(contract.owners)
        consumers = list(contract.consumers)
        out: list[tuple[str, str]] = []
        if audience in {"owner", "both", "digest"}:
            out += [(o, "owner") for o in owners]
        if audience in {"consumer", "both"}:
            out += [(c, "consumer") for c in consumers]
        return out or [("data-platform-oncall", "owner")]

    # -- core --------------------------------------------------------------

    def process_run(self, contract: Contract, result: ValidationResult, *, now: datetime | None = None) -> list[Alert]:
        """Emit/refresh/resolve alerts for one validation run. Returns delivered alerts."""
        now = now or datetime.now(timezone.utc)
        now_iso = now.isoformat()
        delivered: list[Alert] = []

        grouped: dict[str, list] = {}
        for v in result.violations:
            k = alert_key(contract.contract_id, contract.version, v.rule_name, v.column)
            grouped.setdefault(k, []).append(v)

        # Violation storage is capped per rule, so the number of stored records
        # understates the problem. `by_rule` carries the true count from the run;
        # the alert must report that, not the number of rows we kept.
        def true_occurrences(rule_name: str, stored: int) -> int:
            return max(stored, result.by_rule.get(rule_name, stored))

        active_keys = set(grouped)

        for key, vs in grouped.items():
            v = vs[0]
            state = self.store.get_alert_state(key)
            occurrence = true_occurrences(v.rule_name, len(vs))
            last_notified = _parse_ts(state["last_notified_at"]) if state else None
            within_quiet = last_notified is not None and (now - last_notified) < timedelta(hours=self.quiet_hours)
            already_open = state is not None and state["status"] == "open"

            if within_quiet and already_open:
                # Record a suppressed occurrence so the console shows repeats
                # without re-notifying anyone.
                self.store.upsert_alert_state(
                    key, "open", now_iso, state["last_notified_at"], state["notified_count"]
                )
                self.store.add_alert(
                    self._build(
                        contract, v, key, now_iso, suppressed=1, occurrence=occurrence
                    ).to_row()
                )
                continue

            audience = audience_for(v.severity)
            if audience == "digest" and self.digest and not _is_digest_time(now):
                # medium/low outside digest time: keep state open, no push.
                self.store.upsert_alert_state(key, "open", now_iso, None, 0)
                continue

            for recipient, role in self.recipients_for(contract, audience):
                alert = self._build(
                    contract, v, key, now_iso, suppressed=0, occurrence=occurrence,
                    recipient=recipient, audience=audience, role=role,
                )
                self._deliver(alert)
                delivered.append(alert)

            self.store.upsert_alert_state(
                key, "open", now_iso, now_iso,
                (state["notified_count"] if state else 0) + 1,
            )

        # Resolve problems that were open but no longer observed.
        #
        # Only *violation* alerts are resolvable by a clean validation run.  Sign-off
        # requests, approvals and other workflow notices have their own lifecycle
        # and must not be closed just because the sampler saw clean rows.
        resolved = self.store.connect().execute(
            "SELECT alert_key FROM alert_state WHERE status='open'"
        ).fetchall()
        for row in resolved:
            key = row["alert_key"]
            if key in active_keys:
                continue
            prior = self.store.connect().execute(
                "SELECT kind, severity FROM alerts WHERE alert_key=? ORDER BY id DESC LIMIT 1", (key,)
            ).fetchone()
            if prior is not None and prior["kind"] != "violation":
                continue
            # A resolution inherits the severity of the problem it closes: a
            # medium/low problem must not generate a page at 2am just because it
            # stopped happening.
            prev_sev = prior["severity"] if prior is not None else "high"
            if audience_for(prev_sev) == "digest" and self.digest and not _is_digest_time(now):
                continue
            self.store.upsert_alert_state(key, "resolved", now_iso, now_iso, 0)
            self.store.mark_alerts_resolved(key, now_iso)
            prev = self.store.alerts_for_key(key, limit=1)
            prev_alert = prev[0] if prev else None
            if prev_alert:
                recips = {a["recipient"] for a in self.store.alerts_for_key(key, limit=50) if not a["suppressed"]}
                for recipient in recips or {"data-platform-oncall"}:
                    alert = Alert(
                        alert_key=key,
                        contract_id=prev_alert["contract_id"],
                        version=prev_alert["version"],
                        rule_name=prev_alert["rule_name"],
                        severity=prev_alert["severity"],
                        audience="owner",
                        recipient=recipient,
                        channel=self.channels[0].name if self.channels else "outbox",
                        title=f"RESOLVED: {prev_alert['title']}",
                        body=f"Problem no longer observed in run at {now_iso}.",
                        created_at=now_iso,
                        kind="resolution",
                    )
                    self._deliver(alert)
                    delivered.append(alert)

        return delivered

    def _build(
        self,
        contract: Contract,
        v,
        key: str,
        now_iso: str,
        *,
        suppressed: int,
        occurrence: int,
        recipient: str = "data-platform-oncall",
        audience: str | None = None,
        role: str = "owner",
    ) -> Alert:
        audience = audience or audience_for(v.severity)
        sample_pk = json.dumps(v.row_pk, sort_keys=True, default=str) if v.row_pk else "table-level"
        title = f"[{v.severity}] {contract.contract_id} v{contract.version}: {v.rule_name}"
        body = (
            f"Rule: {v.rule_name} ({v.rule_kind})\n"
            f"Column: {v.column or '-'}\n"
            f"Detail: {v.detail}\n"
            f"Sample row: {sample_pk}\n"
            f"Occurrences in this run: {occurrence}\n"
            f"Contract owner: {', '.join(contract.owners) or 'unassigned'}\n"
            f"Receiving as: {role}"
        )
        return Alert(
            alert_key=key,
            contract_id=contract.contract_id,
            version=contract.version,
            rule_name=v.rule_name,
            severity=v.severity,
            audience=audience,
            recipient=recipient,
            channel=self.channels[0].name if self.channels else "outbox",
            title=title,
            body=body,
            created_at=now_iso,
            suppressed=suppressed,
            occurrence_count=occurrence,
        )

    def _deliver(self, alert: Alert) -> None:
        self.store.add_alert(alert.to_row())
        for ch in self.channels:
            try:
                ch.send(alert)
            except Exception as e:  # a broken channel must not stop validation
                self.store.add_alert(
                    Alert(
                        alert_key=alert.alert_key,
                        contract_id=alert.contract_id,
                        version=alert.version,
                        rule_name=alert.rule_name,
                        severity="low",
                        audience="owner",
                        recipient="data-platform-oncall",
                        channel=ch.name,
                        title=f"channel delivery failed: {ch.name}",
                        body=f"{e}",
                        created_at=alert.created_at,
                        suppressed=0,
                        occurrence_count=1,
                    ).to_row()
                )

    # -- breaking-change notifications ------------------------------------

    def notify_breaking_change(
        self,
        contract: Contract,
        change_set: Any,
        *,
        now: datetime | None = None,
    ) -> list[Alert]:
        now = now or datetime.now(timezone.utc)
        now_iso = now.isoformat()
        key = hashlib.sha256(
            f"breaking|{contract.contract_id}|{contract.version}".encode()
        ).hexdigest()[:20]
        breaking = change_set.breaking
        lines = [f"- [{c.severity}] {c.summary}" for c in breaking[:20]]
        title = f"[high] {contract.contract_id} v{contract.version} needs sign-off ({len(breaking)} breaking)"
        body = (
            f"Breaking changes submitted by upstream {contract.upstream_system}:\n"
            + "\n".join(lines)
            + f"\n\nSign-off required from: {', '.join(contract.consumers) or '(no consumers registered)'}"
        )
        delivered = []
        for recipient in sorted(set(contract.consumers) | set(contract.owners)):
            alert = Alert(
                alert_key=key,
                contract_id=contract.contract_id,
                version=contract.version,
                rule_name=None,
                severity="high",
                audience="consumer",
                recipient=recipient,
                channel=self.channels[0].name if self.channels else "outbox",
                title=title,
                body=body,
                created_at=now_iso,
                kind="signoff_request",
            )
            self._deliver(alert)
            delivered.append(alert)
        self.store.upsert_alert_state(key, "open", now_iso, now_iso, 1)
        return delivered

    def notify_signoff_complete(self, contract: Contract, *, now: datetime | None = None) -> list[Alert]:
        now = now or datetime.now(timezone.utc)
        now_iso = now.isoformat()
        key = hashlib.sha256(f"approved|{contract.contract_id}|{contract.version}".encode()).hexdigest()[:20]
        delivered = []
        for recipient in sorted(set(contract.consumers) | set(contract.owners)):
            alert = Alert(
                alert_key=key,
                contract_id=contract.contract_id,
                version=contract.version,
                rule_name=None,
                severity="info",
                audience="consumer",
                recipient=recipient,
                channel=self.channels[0].name if self.channels else "outbox",
                title=f"{contract.contract_id} v{contract.version} approved, parallel rollout started",
                body="All registered consumers approved. Old and new versions validate in parallel during the transition window.",
                created_at=now_iso,
                kind="signoff_approved",
            )
            self._deliver(alert)
            delivered.append(alert)
        return delivered


def _is_digest_time(now: datetime) -> bool:
    """Digest is cut once a day; used for medium/low severity only."""
    return now.hour == 9
