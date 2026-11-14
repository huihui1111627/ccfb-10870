"""SQLite persistence layer.

One file, WAL mode, no ORM.  The schema is small enough that explicit SQL is the
clearest documentation of what the platform remembers.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .contract import Contract, parse_contract

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS contract_versions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    upstream_system TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    document      TEXT NOT NULL,          -- canonical contract JSON
    schema_hash   TEXT NOT NULL,
    status        TEXT NOT NULL,          -- draft|active|pending_signoff|superseded|rejected
    parent_version INTEGER,
    compatibility TEXT,                   -- compatible|breaking|no_change|initial
    submitted_by  TEXT,
    submitted_at  TEXT NOT NULL,
    activated_at  TEXT,
    transition_ends_at TEXT,
    note          TEXT,
    UNIQUE(contract_id, version)
);
CREATE INDEX IF NOT EXISTS idx_cv_contract ON contract_versions(contract_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_cv_status ON contract_versions(status);

CREATE TABLE IF NOT EXISTS changes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id  TEXT NOT NULL,
    version      INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_changes_contract ON changes(contract_id, version DESC);

CREATE TABLE IF NOT EXISTS run_samples (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id  TEXT NOT NULL,
    version      INTEGER NOT NULL,
    role         TEXT NOT NULL,             -- candidate|active|previous
    payload      TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT NOT NULL UNIQUE,
    contract_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    rows_checked  INTEGER DEFAULT 0,
    rows_clean    INTEGER DEFAULT 0,
    violation_count INTEGER DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'running',
    trigger       TEXT NOT NULL DEFAULT 'manual',
    role          TEXT,
    elapsed_ms    INTEGER,
    error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_contract ON validation_runs(contract_id, started_at DESC);

CREATE TABLE IF NOT EXISTS violations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    violation_key TEXT NOT NULL,            -- stable dedupe key
    run_id        TEXT NOT NULL,
    contract_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    rule_name     TEXT NOT NULL,
    rule_kind     TEXT NOT NULL,
    column_name   TEXT,
    severity      TEXT NOT NULL,
    row_pk        TEXT,                     -- JSON primary key of offending row
    row_data      TEXT,                     -- JSON row snapshot
    detail        TEXT,
    observed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_viol_contract ON violations(contract_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_viol_key ON violations(violation_key);
CREATE INDEX IF NOT EXISTS idx_viol_run ON violations(run_id);

CREATE TABLE IF NOT EXISTS signoffs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    consumer      TEXT NOT NULL,
    decision      TEXT NOT NULL,            -- pending|approved|rejected
    decided_at    TEXT,
    comment       TEXT,
    UNIQUE(contract_id, version, consumer)
);

CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_key     TEXT NOT NULL,
    contract_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    rule_name     TEXT,
    severity      TEXT NOT NULL,
    audience      TEXT NOT NULL,            -- owner|consumer|both
    recipient     TEXT NOT NULL,
    channel       TEXT NOT NULL,
    title         TEXT NOT NULL,
    body          TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    resolved_at   TEXT,
    suppressed    INTEGER NOT NULL DEFAULT 0,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    kind          TEXT NOT NULL DEFAULT 'violation'
);
CREATE INDEX IF NOT EXISTS idx_alerts_key ON alerts(alert_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_contract ON alerts(contract_id, created_at DESC);

CREATE TABLE IF NOT EXISTS alert_state (
    alert_key     TEXT PRIMARY KEY,
    status        TEXT NOT NULL,            -- open|resolved
    last_seen_at  TEXT NOT NULL,
    last_notified_at TEXT,
    notified_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_tables (
    upstream_system TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    db_path         TEXT NOT NULL,
    description     TEXT,
    PRIMARY KEY (upstream_system, table_name)
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def connect(self) -> "Conn":
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- contracts ---------------------------------------------------------

    def insert_version(
        self,
        contract: Contract,
        *,
        parent_version: int | None,
        compatibility: str,
        submitted_by: str,
        submitted_at: str,
        status: str,
        note: str | None = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO contract_versions
                   (contract_id, version, upstream_system, table_name, document,
                    schema_hash, status, parent_version, compatibility, submitted_by,
                    submitted_at, note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    contract.contract_id,
                    contract.version,
                    contract.upstream_system,
                    contract.table,
                    json.dumps(contract.to_dict(), sort_keys=True, default=str),
                    contract.schema_hash,
                    status,
                    parent_version,
                    compatibility,
                    submitted_by,
                    submitted_at,
                    note,
                ),
            )

    def get_version(self, contract_id: str, version: int) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? AND version=?",
            (contract_id, version),
        ).fetchone()
        return dict(row) if row else None

    def load_contract(self, contract_id: str, version: int) -> Contract | None:
        row = self.get_version(contract_id, version)
        if not row:
            return None
        c = parse_contract(json.loads(row["document"]), version=row["version"])
        c.status = row["status"]
        c.schema_hash = row["schema_hash"]
        return c

    def latest_version(self, contract_id: str) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? ORDER BY version DESC LIMIT 1",
            (contract_id,),
        ).fetchone()
        return dict(row) if row else None

    def active_version(self, contract_id: str) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? AND status='active' ORDER BY version DESC LIMIT 1",
            (contract_id,),
        ).fetchone()
        return dict(row) if row else None

    def previous_active_version(self, contract_id: str) -> dict[str, Any] | None:
        rows = self.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? AND status='active' ORDER BY version DESC LIMIT 2",
            (contract_id,),
        ).fetchall()
        return dict(rows[1]) if len(rows) > 1 else None

    def list_contracts(self) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """SELECT cv.contract_id, cv.version, cv.upstream_system, cv.table_name,
                      cv.status, cv.compatibility, cv.submitted_at, cv.activated_at,
                      cv.transition_ends_at, cv.schema_hash, cv.document
               FROM contract_versions cv
               JOIN (SELECT contract_id, MAX(version) AS v FROM contract_versions
                     GROUP BY contract_id) m
                 ON cv.contract_id=m.contract_id AND cv.version=m.v"""
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            doc = json.loads(d.pop("document"))
            d["description"] = doc.get("description", "")
            d["column_count"] = len(doc.get("columns", []))
            d["consumers"] = doc.get("consumers", [])
            d["owners"] = doc.get("owners", [])
            act = self.active_version(d["contract_id"])
            d["active_version"] = act["version"] if act else None
            out.append(d)
        return sorted(out, key=lambda x: (x["upstream_system"], x["contract_id"]))

    def list_versions(self, contract_id: str) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT * FROM contract_versions WHERE contract_id=? ORDER BY version",
            (contract_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def set_version_status(
        self,
        contract_id: str,
        version: int,
        status: str,
        *,
        activated_at: str | None = None,
        transition_ends_at: str | None = None,
    ) -> None:
        with self.tx() as conn:
            if activated_at is not None or transition_ends_at is not None:
                conn.execute(
                    "UPDATE contract_versions SET status=?, activated_at=COALESCE(?, activated_at), "
                    "transition_ends_at=COALESCE(?, transition_ends_at) WHERE contract_id=? AND version=?",
                    (status, activated_at, transition_ends_at, contract_id, version),
                )
            else:
                conn.execute(
                    "UPDATE contract_versions SET status=? WHERE contract_id=? AND version=?",
                    (status, contract_id, version),
                )

    def supersede_older(self, contract_id: str, keep_version: int) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE contract_versions SET status='superseded' "
                "WHERE contract_id=? AND version<? AND status='active'",
                (contract_id, keep_version),
            )

    # -- changes -----------------------------------------------------------

    def save_changes(self, contract_id: str, version: int, payload: dict[str, Any], created_at: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO changes (contract_id, version, payload, created_at) VALUES (?,?,?,?)",
                (contract_id, version, json.dumps(payload, sort_keys=True, default=str), created_at),
            )

    def get_changes(self, contract_id: str, version: int) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT payload FROM changes WHERE contract_id=? AND version=? ORDER BY id DESC LIMIT 1",
            (contract_id, version),
        ).fetchone()
        return json.loads(row["payload"]) if row else None

    # -- samples -----------------------------------------------------------

    def save_sample(
        self, contract_id: str, version: int, role: str, rows: list[dict[str, Any]], created_at: str
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO run_samples (contract_id, version, role, payload, created_at) VALUES (?,?,?,?,?)",
                (contract_id, version, role, json.dumps(rows, default=str), created_at),
            )

    def get_sample(self, contract_id: str, version: int, role: str) -> list[dict[str, Any]] | None:
        row = self.connect().execute(
            "SELECT payload FROM run_samples WHERE contract_id=? AND version=? AND role=? "
            "ORDER BY id DESC LIMIT 1",
            (contract_id, version, role),
        ).fetchone()
        return json.loads(row["payload"]) if row else None

    # -- validation --------------------------------------------------------

    def start_run(
        self, run_id: str, contract_id: str, version: int, started_at: str, trigger: str, role: str | None = None
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO validation_runs (run_id, contract_id, version, started_at, status, trigger, role) "
                "VALUES (?,?,?,?, 'running', ?, ?)",
                (run_id, contract_id, version, started_at, trigger, role),
            )

    def finish_run(
        self,
        run_id: str,
        *,
        finished_at: str,
        rows_checked: int,
        rows_clean: int,
        violation_count: int,
        status: str = "completed",
        error: str | None = None,
        elapsed_ms: int | None = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE validation_runs SET finished_at=?, rows_checked=?, rows_clean=?, "
                "violation_count=?, status=?, error=?, elapsed_ms=COALESCE(?, elapsed_ms) WHERE run_id=?",
                (finished_at, rows_checked, rows_clean, violation_count, status, error, elapsed_ms, run_id),
            )

    def add_violations(self, violations: list[dict[str, Any]]) -> None:
        if not violations:
            return
        with self.tx() as conn:
            conn.executemany(
                """INSERT INTO violations
                   (violation_key, run_id, contract_id, version, rule_name, rule_kind,
                    column_name, severity, row_pk, row_data, detail, observed_at)
                   VALUES (:violation_key,:run_id,:contract_id,:version,:rule_name,:rule_kind,
                           :column_name,:severity,:row_pk,:row_data,:detail,:observed_at)""",
                violations,
            )

    def list_runs(
        self, contract_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        conn = self.connect()
        if contract_id:
            rows = conn.execute(
                "SELECT * FROM validation_runs WHERE contract_id=? ORDER BY started_at DESC LIMIT ?",
                (contract_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM validation_runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT * FROM validation_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_violations(
        self,
        *,
        contract_id: str | None = None,
        run_id: str | None = None,
        rule_name: str | None = None,
        severity: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        q = "SELECT * FROM violations WHERE 1=1"
        args: list[Any] = []
        if contract_id:
            q += " AND contract_id=?"
            args.append(contract_id)
        if run_id:
            q += " AND run_id=?"
            args.append(run_id)
        if rule_name:
            q += " AND rule_name=?"
            args.append(rule_name)
        if severity:
            q += " AND severity=?"
            args.append(severity)
        q += " ORDER BY observed_at DESC, id DESC LIMIT ? OFFSET ?"
        args.extend([limit, offset])
        return [dict(r) for r in self.connect().execute(q, args).fetchall()]

    def violation_counts_by_rule(self, contract_id: str) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """SELECT rule_name, rule_kind, severity, COUNT(*) AS n,
                      MAX(observed_at) AS last_seen
               FROM violations WHERE contract_id=?
               GROUP BY rule_name ORDER BY n DESC""",
            (contract_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def violation_trend(self, days: int = 30) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """SELECT substr(v.observed_at,1,10) AS day, cv.upstream_system, COUNT(*) AS n
               FROM violations v
               JOIN contract_versions cv ON cv.contract_id=v.contract_id AND cv.version=v.version
               GROUP BY day, cv.upstream_system ORDER BY day""",
        ).fetchall()
        return [dict(r) for r in rows]

    def upstream_summary(self, days: int = 30) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            """SELECT cv.upstream_system,
                      COUNT(*) AS violations,
                      COUNT(DISTINCT v.contract_id) AS contracts_affected,
                      SUM(CASE WHEN v.severity='critical' THEN 1 ELSE 0 END) AS critical_count,
                      MAX(v.observed_at) AS last_seen
               FROM violations v
               JOIN contract_versions cv ON cv.contract_id=v.contract_id AND cv.version=v.version
               GROUP BY cv.upstream_system ORDER BY violations DESC"""
        ).fetchall()
        return [dict(r) for r in rows]

    # -- signoffs ----------------------------------------------------------

    def ensure_signoffs(self, contract_id: str, version: int, consumers: list[str]) -> None:
        with self.tx() as conn:
            for c in consumers:
                conn.execute(
                    "INSERT OR IGNORE INTO signoffs (contract_id, version, consumer, decision) "
                    "VALUES (?,?,?,'pending')",
                    (contract_id, version, c),
                )

    def list_signoffs(self, contract_id: str, version: int) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT * FROM signoffs WHERE contract_id=? AND version=? ORDER BY consumer",
            (contract_id, version),
        ).fetchall()
        return [dict(r) for r in rows]

    def decide_signoff(
        self, contract_id: str, version: int, consumer: str, decision: str, decided_at: str, comment: str | None
    ) -> bool:
        if decision not in {"approved", "rejected", "pending"}:
            raise ValueError("decision must be approved, rejected or pending")
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE signoffs SET decision=?, decided_at=?, comment=? "
                "WHERE contract_id=? AND version=? AND consumer=?",
                (decision, decided_at if decision != "pending" else None, comment, contract_id, version, consumer),
            )
            return cur.rowcount > 0

    def pending_versions(self) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT * FROM contract_versions WHERE status='pending_signoff' ORDER BY submitted_at"
        ).fetchall()
        return [dict(r) for r in rows]

    # -- alerts ------------------------------------------------------------

    def get_alert_state(self, key: str) -> dict[str, Any] | None:
        row = self.connect().execute("SELECT * FROM alert_state WHERE alert_key=?", (key,)).fetchone()
        return dict(row) if row else None

    def upsert_alert_state(
        self, key: str, status: str, seen_at: str, notified_at: str | None, notified_count: int
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO alert_state (alert_key, status, last_seen_at, last_notified_at, notified_count)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(alert_key) DO UPDATE SET status=excluded.status,
                     last_seen_at=excluded.last_seen_at,
                     last_notified_at=excluded.last_notified_at,
                     notified_count=excluded.notified_count""",
                (key, status, seen_at, notified_at, notified_count),
            )

    def add_alert(self, alert: dict[str, Any]) -> int:
        with self.tx() as conn:
            cur = conn.execute(
                """INSERT INTO alerts
                   (alert_key, contract_id, version, rule_name, severity, audience, recipient,
                    channel, title, body, created_at, resolved_at, suppressed, occurrence_count, kind)
                   VALUES (:alert_key,:contract_id,:version,:rule_name,:severity,:audience,:recipient,
                           :channel,:title,:body,:created_at,:resolved_at,:suppressed,:occurrence_count,:kind)""",
                alert,
            )
            return cur.lastrowid or 0

    def list_alerts(self, limit: int = 100, include_suppressed: bool = True) -> list[dict[str, Any]]:
        q = "SELECT * FROM alerts"
        if not include_suppressed:
            q += " WHERE suppressed=0"
        q += " ORDER BY created_at DESC, id DESC LIMIT ?"
        return [dict(r) for r in self.connect().execute(q, (limit,)).fetchall()]

    def mark_alerts_resolved(self, key: str, resolved_at: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE alerts SET resolved_at=? WHERE alert_key=? AND resolved_at IS NULL AND suppressed=0",
                (resolved_at, key),
            )

    def alerts_for_key(self, key: str, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT * FROM alerts WHERE alert_key=? ORDER BY created_at DESC LIMIT ?", (key, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def all_recipients(self) -> set[str]:
        out: set[str] = set()
        for table, col in (("signoffs", "consumer"),):
            for r in self.connect().execute(f"SELECT DISTINCT {col} AS x FROM {table}"):
                out.add(r["x"])
        return out

    # -- misc --------------------------------------------------------------

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.connect().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO settings (key,value) VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def register_upstream(self, system: str, table: str, db_path: str, description: str = "") -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO upstream_tables (upstream_system, table_name, db_path, description) "
                "VALUES (?,?,?,?) ON CONFLICT(upstream_system, table_name) DO UPDATE SET "
                "db_path=excluded.db_path, description=excluded.description",
                (system, table, db_path, description),
            )

    def upstream_table(self, system: str, table: str) -> dict[str, Any] | None:
        row = self.connect().execute(
            "SELECT * FROM upstream_tables WHERE upstream_system=? AND table_name=?", (system, table)
        ).fetchone()
        return dict(row) if row else None

    def list_upstream_tables(self) -> list[dict[str, Any]]:
        rows = self.connect().execute(
            "SELECT * FROM upstream_tables ORDER BY upstream_system, table_name"
        ).fetchall()
        return [dict(r) for r in rows]

    def reset(self) -> None:
        with self.tx() as conn:
            for t in (
                "alerts", "alert_state", "violations", "validation_runs", "run_samples",
                "changes", "signoffs", "contract_versions", "upstream_tables", "settings",
            ):
                conn.execute(f"DELETE FROM {t}")
