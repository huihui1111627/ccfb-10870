"""Mock upstream business systems: schemas, valid data generation, materialization.

Each upstream is a real SQLite database that the platform reads.  The physical
schemas deliberately do not enforce the contract's constraints — that is the
point of the exercise: upstreams change shape and ship dirty rows, and the
platform is what notices.
"""

from __future__ import annotations

import hashlib
import random
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc


# --------------------------------------------------------------------------
# Column spec
# --------------------------------------------------------------------------


@dataclass
class ColSpec:
    name: str
    type: str  # logical type accepted by the contract DSL
    sql_type: str = "TEXT"
    required: bool = True
    enum: list[Any] | None = None
    minimum: Any = None
    maximum: Any = None
    unique: bool = False
    precision: int | None = None
    scale: int | None = None
    description: str | None = None
    #: generator kind
    gen: str = "text"
    gen_args: dict[str, Any] = field(default_factory=dict)

    def to_contract_column(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "sql_type": self.sql_type,
            "required": self.required,
        }
        if self.enum is not None:
            d["enum"] = list(self.enum)
        if self.minimum is not None:
            d["minimum"] = self.minimum
        if self.maximum is not None:
            d["maximum"] = self.maximum
        if self.unique:
            d["unique"] = True
        if self.precision is not None:
            d["precision"] = self.precision
        if self.scale is not None:
            d["scale"] = self.scale
        if self.description:
            d["description"] = self.description
        return d


@dataclass
class SimTable:
    system: str
    name: str
    columns: list[ColSpec]
    primary_key: list[str] = field(default_factory=list)
    unique_keys: list[list[str]] = field(default_factory=list)
    freshness: dict[str, Any] | None = None
    rules: list[dict[str, Any]] = field(default_factory=list)
    consumers: list[str] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    description: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def contract_id(self) -> str:
        return f"{self.system}.{self.name}"

    def column(self, name: str) -> ColSpec | None:
        return next((c for c in self.columns if c.name == name), None)

    def copy(self) -> "SimTable":
        return replace(
            self,
            columns=[replace(c) for c in self.columns],
            primary_key=list(self.primary_key),
            unique_keys=[list(k) for k in self.unique_keys],
            freshness=dict(self.freshness) if self.freshness else None,
            rules=[dict(r) for r in self.rules],
            consumers=list(self.consumers),
            owners=list(self.owners),
            rows=[dict(r) for r in self.rows],
        )

    def contract_document(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "contract_id": self.contract_id,
            "upstream_system": self.system,
            "table": self.name,
            "description": self.description,
            "columns": [c.to_contract_column() for c in self.columns],
            "primary_key": list(self.primary_key),
            "unique_keys": [list(k) for k in self.unique_keys],
            "consumers": list(self.consumers),
            "owners": list(self.owners),
        }
        if self.freshness:
            doc["freshness"] = dict(self.freshness)
        if self.rules:
            doc["rules"] = [dict(r) for r in self.rules]
        return doc


# --------------------------------------------------------------------------
# Value generation
# --------------------------------------------------------------------------

_FIRST = ["张", "王", "李", "赵", "陈", "刘", "杨", "黄", "周", "吴"]
_TEXT = ["alpha", "beta", "gamma", "delta", "omega", "nova", "atlas", "vega"]


def _rng_table(system: str, table: str, seed: int, salt: str = "v1") -> random.Random:
    h = int(hashlib.sha256(f"{system}.{table}:{seed}:{salt}".encode()).hexdigest()[:16], 16)
    return random.Random(h)


def _gen_value(col: ColSpec, rng: random.Random, idx: int, row_ctx: dict[str, Any]) -> Any:
    g = col.gen
    args = col.gen_args
    if g == "seq":
        prefix = args.get("prefix", "")
        return f"{prefix}{idx + args.get('start', 1):08d}"
    if g == "enum":
        pool = col.enum or ["unknown"]
        weights = args.get("weights")
        return rng.choices(pool, weights=weights, k=1)[0] if weights else rng.choice(pool)
    if g == "hash_text":
        return hashlib.sha256(f"hash:{idx}:{args.get('salt', 'x')}".encode()).hexdigest()[:24]
    if g == "fee":
        # Most payments charge a small percentage fee, never above the amount.
        return round(rng.uniform(0, 6), 2)
    if g == "int_range":
        lo = int(col.minimum if col.minimum is not None else args.get("lo", 0))
        hi = int(col.maximum if col.maximum is not None else args.get("hi", 100))
        return rng.randint(lo, hi)
    if g == "decimal_range":
        lo = float(col.minimum if col.minimum is not None else args.get("lo", 0))
        hi = float(col.maximum if col.maximum is not None else args.get("hi", 1000))
        scale = col.scale if col.scale is not None else 2
        return round(rng.uniform(lo, hi), scale)
    if g == "bool":
        return rng.random() < args.get("p", 0.7)
    if g == "timestamp":
        # within the freshness window by default
        minutes_ago = rng.randint(args.get("min_minutes_ago", 1), args.get("max_minutes_ago", 600))
        return (datetime.now(UTC) - timedelta(minutes=minutes_ago)).replace(microsecond=0).isoformat()
    if g == "date":
        days_ago = rng.randint(0, args.get("max_days_ago", 90))
        return (datetime.now(UTC).date() - timedelta(days=days_ago)).isoformat()
    if g == "uuid":
        return hashlib.md5(f"{row_ctx.get('_seed', 0)}:{idx}:{col.name}".encode()).hexdigest()[:16]
    if g == "phone":
        return f"1{rng.choice('3456789')}{rng.randint(100000000, 999999999)}"
    if g == "name":
        return _FIRST[rng.randrange(len(_FIRST))] + rng.choice(_TEXT)
    if g == "ref":
        pool = args.get("pool") or []
        return rng.choice(pool) if pool else None
    if g == "json":
        return '{"v": 1, "source": "app"}'
    return f"{args.get('prefix', 'v')}{rng.randrange(10000):04d}"


def generate_rows(table: SimTable, count: int, *, seed: int = 7) -> list[dict[str, Any]]:
    rng = _rng_table(table.system, table.name, seed)
    rows: list[dict[str, Any]] = []
    for i in range(count):
        row: dict[str, Any] = {"_seed": i}
        for col in table.columns:
            if col.gen == "ref":
                pool = col.gen_args.get("pool")
                if pool is None:
                    pool = [r[col.gen_args.get("from", "")] for r in rows] or [f"x{i:06d}"]
                row[col.name] = rng.choice(pool)
            else:
                row[col.name] = _gen_value(col, rng, i, row)
            if col.gen == "timestamp" and col.name.endswith("_at"):
                row[col.name] = row[col.name]
        # logical consistency for orders: paid_at only when paid
        if "paid_at" in row and "status" in row:
            if row["status"] in ("paid", "shipped", "completed"):
                created = datetime.fromisoformat(row["created_at"])
                row["paid_at"] = (created + timedelta(minutes=rng.randint(1, 120))).isoformat()
            else:
                row["paid_at"] = None
        row.pop("_seed", None)
        rows.append(row)
    return _dedupe_uniques(table, rows, rng)


def _dedupe_uniques(table: SimTable, rows: list[dict[str, Any]], rng: random.Random) -> list[dict[str, Any]]:
    """Ensure declared unique columns / keys are actually unique in valid data."""
    unique_cols = [c.name for c in table.columns if c.unique]
    for col in unique_cols:
        seen = set()
        for i, row in enumerate(rows):
            v = row.get(col)
            if v in seen:
                row[col] = f"{v}-{i}" if isinstance(v, str) else (v or 0) + i
            seen.add(row.get(col))
    for key in table.unique_keys + ([table.primary_key] if table.primary_key else []):
        seen = set()
        # Prefer perturbing a column that is neither enum-constrained nor the
        # primary key, so dedupe never manufactures out-of-enum data.
        enum_cols = {c.name for c in table.columns if c.enum}
        pk_cols = set(table.primary_key)
        perturb = [k for k in key if k not in enum_cols and k not in pk_cols]
        target = perturb[-1] if perturb else key[-1]
        for i, row in enumerate(rows):
            kv = tuple(row.get(k) for k in key)
            if kv in seen:
                v = row.get(target)
                row[target] = f"{v}-{i}" if isinstance(v, str) else (v + i)
                kv = tuple(row.get(k) for k in key)
            seen.add(kv)
    return rows


# --------------------------------------------------------------------------
# Materialization
# --------------------------------------------------------------------------


def _q(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier {name!r}")
    return f'"{name}"'


def materialize(tables: list[SimTable], db_path: str | Path) -> Path:
    """Write the given tables into a SQLite database, replacing existing ones."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {_q(table.name)}")
            cols_sql = ", ".join(f"{_q(c.name)} {c.sql_type}" for c in table.columns)
            conn.execute(f"CREATE TABLE {_q(table.name)} ({cols_sql})")
            if table.rows:
                names = [c.name for c in table.columns]
                placeholders = ",".join("?" for _ in names)
                conn.executemany(
                    f"INSERT INTO {_q(table.name)} ({','.join(_q(n) for n in names)}) VALUES ({placeholders})",
                    [tuple(_coerce_for_sqlite(r.get(n), c) for n, c in zip(names, table.columns)) for r in table.rows],
                )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _coerce_for_sqlite(value: Any, col: ColSpec) -> Any:
    if value is None:
        return None
    if col.type == "bool":
        return 1 if value else 0
    if col.type in {"int", "bigint"} and isinstance(value, bool):
        return int(value)
    return value


def physical_schema(db_path: str | Path, table: str) -> dict[str, str]:
    """Column name -> declared SQL type, straight from the database."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(f"PRAGMA table_info({_q(table)})").fetchall()
        return {r[1]: (r[2] or "TEXT") for r in rows}
    finally:
        conn.close()


def read_table(db_path: str | Path, table: str) -> tuple[list[str], list[dict[str, Any]]]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(f"SELECT * FROM {_q(table)}")
        cols = [d[0] for d in cur.description]
        return cols, [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# The five upstream systems
# --------------------------------------------------------------------------


def _c(**kw) -> ColSpec:
    return ColSpec(**kw)


def build_upstreams(*, rows_per_table: int = 800, seed: int = 7) -> dict[str, list[SimTable]]:
    """Build all mock upstream tables with valid data.

    Returns {upstream_system: [SimTable, ...]}.
    """
    tables: list[SimTable] = []

    # ---------------- order_core.orders ----------------
    orders = SimTable(
        system="order_core",
        name="orders",
        description="Order header written by the order service.",
        columns=[
            _c(name="order_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "ORD"}),
            _c(name="user_id", type="string", sql_type="TEXT", gen="seq", gen_args={"prefix": "U", "start": 900000}),
            _c(name="status", type="string", sql_type="TEXT", gen="enum",
               enum=["created", "paid", "shipped", "completed", "cancelled"]),
            _c(name="total_amount", type="decimal", sql_type="REAL", minimum=0, maximum=100000,
               precision=12, scale=2, gen="decimal_range", gen_args={"lo": 5, "hi": 5000}),
            _c(name="currency", type="string", sql_type="TEXT", gen="enum", enum=["CNY", "USD", "EUR", "HKD"]),
            _c(name="channel", type="string", sql_type="TEXT", gen="enum",
               enum=["app", "web", "mini_program", "offline"]),
            _c(name="created_at", type="timestamp", sql_type="TEXT", gen="timestamp",
               gen_args={"min_minutes_ago": 5, "max_minutes_ago": 20 * 60}),
            _c(name="paid_at", type="timestamp", sql_type="TEXT", required=False, gen="timestamp"),
            _c(name="updated_at", type="timestamp", sql_type="TEXT", gen="timestamp",
               gen_args={"min_minutes_ago": 1, "max_minutes_ago": 120}),
        ],
        primary_key=["order_id"],
        freshness={"max_age": "PT24H", "column": "updated_at", "severity": "high"},
        rules=[
            {"name": "paid_after_created", "kind": "expression",
             "expression": "paid_at is null or paid_at >= created_at", "severity": "high"},
            {"name": "order_id_not_null", "kind": "not_null", "column": "order_id", "severity": "critical"},
        ],
        consumers=["reporting.dw_orders", "risk.feature_order_velocity", "finance.recon_daily"],
        owners=["order_core.team"],
    )
    tables.append(orders)

    # ---------------- order_core.order_items ----------------
    tables.append(
        SimTable(
            system="order_core",
            name="order_items",
            description="Order line items.",
            columns=[
                _c(name="item_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "ITM"}),
                _c(name="order_id", type="string", sql_type="TEXT", gen="ref", gen_args={"from": "order_id"}),
                _c(name="sku_id", type="string", sql_type="TEXT", gen="seq", gen_args={"prefix": "SKU", "start": 3000}),
                _c(name="qty", type="int", sql_type="INTEGER", minimum=1, maximum=999, gen="int_range",
                   gen_args={"lo": 1, "hi": 8}),
                _c(name="unit_price", type="decimal", sql_type="REAL", minimum=0, maximum=100000,
                   precision=12, scale=2, gen="decimal_range", gen_args={"lo": 5, "hi": 3000}),
                _c(name="discount", type="decimal", sql_type="REAL", minimum=0, maximum=1,
                   precision=5, scale=4, gen="decimal_range", gen_args={"lo": 0, "hi": 0.5}),
                _c(name="created_at", type="timestamp", sql_type="TEXT", gen="timestamp",
                   gen_args={"min_minutes_ago": 5, "max_minutes_ago": 20 * 60}),
            ],
            primary_key=["item_id"],
            unique_keys=[["order_id", "sku_id"]],
            consumers=["reporting.dw_order_items", "supply.demand_forecast"],
            owners=["order_core.team"],
        )
    )

    # ---------------- payment_core.payments ----------------
    tables.append(
        SimTable(
            system="payment_core",
            name="payments",
            description="Payment transactions.",
            columns=[
                _c(name="payment_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "PAY"}),
                _c(name="order_id", type="string", sql_type="TEXT", gen="ref", gen_args={"from": "order_id"}),
                _c(name="method", type="string", sql_type="TEXT", gen="enum",
                   enum=["card", "wallet", "bank_transfer", "balance"],
                   gen_args={"weights": [6, 3, 1, 2]}),
                _c(name="amount", type="decimal", sql_type="REAL", minimum=0, maximum=100000,
                   precision=12, scale=2, gen="decimal_range", gen_args={"lo": 5, "hi": 5000}),
                _c(name="fee", type="decimal", sql_type="REAL", minimum=0, maximum=500,
                   precision=8, scale=2, gen="fee"),
                _c(name="status", type="string", sql_type="TEXT", gen="enum",
                   enum=["pending", "succeeded", "failed", "refunded"],
                   gen_args={"weights": [1, 8, 1, 1]}),
                _c(name="paid_at", type="timestamp", sql_type="TEXT", required=False, gen="timestamp"),
                _c(name="created_at", type="timestamp", sql_type="TEXT", gen="timestamp",
                   gen_args={"min_minutes_ago": 5, "max_minutes_ago": 20 * 60}),
            ],
            primary_key=["payment_id"],
            freshness={"max_age": "PT12H", "column": "created_at", "severity": "critical"},
            rules=[
                {"name": "fee_not_exceed_amount", "kind": "expression",
                 "expression": "fee <= amount", "severity": "medium"},
            ],
            consumers=["finance.recon_daily", "risk.feature_payment_fail_rate", "reporting.dw_payments"],
            owners=["payment_core.team"],
        )
    )

    # ---------------- payment_core.refunds ----------------
    tables.append(
        SimTable(
            system="payment_core",
            name="refunds",
            description="Refund requests.",
            columns=[
                _c(name="refund_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "RFD"}),
                _c(name="payment_id", type="string", sql_type="TEXT", gen="seq", gen_args={"prefix": "PAY"}),
                _c(name="amount", type="decimal", sql_type="REAL", minimum=0, maximum=100000,
                   precision=12, scale=2, gen="decimal_range", gen_args={"lo": 1, "hi": 2000}),
                _c(name="reason", type="string", sql_type="TEXT", gen="enum",
                   enum=["not_received", "damaged", "wrong_item", "buyer_remorse", "other"]),
                _c(name="status", type="string", sql_type="TEXT", gen="enum", enum=["requested", "approved", "rejected"]),
                _c(name="created_at", type="timestamp", sql_type="TEXT", gen="timestamp"),
            ],
            primary_key=["refund_id"],
            consumers=["finance.recon_daily"],
            owners=["payment_core.team"],
        )
    )

    # ---------------- user_center.users ----------------
    tables.append(
        SimTable(
            system="user_center",
            name="users",
            description="Registered user profiles.",
            columns=[
                _c(name="user_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "U", "start": 100000}),
                _c(name="phone", type="string", sql_type="TEXT", unique=True, gen="phone"),
                _c(name="nickname", type="string", sql_type="TEXT", gen="name"),
                _c(name="register_channel", type="string", sql_type="TEXT", gen="enum",
                   enum=["organic", "ads", "referral", "partner"]),
                _c(name="kyc_level", type="int", sql_type="INTEGER", minimum=0, maximum=3, gen="int_range",
                   gen_args={"lo": 0, "hi": 3}),
                _c(name="risk_score", type="decimal", sql_type="REAL", minimum=0, maximum=100,
                   precision=5, scale=2, gen="decimal_range", gen_args={"lo": 0, "hi": 100}),
                _c(name="is_active", type="bool", sql_type="INTEGER", gen="bool"),
                _c(name="created_at", type="timestamp", sql_type="TEXT", gen="timestamp",
                   gen_args={"min_minutes_ago": 60, "max_minutes_ago": 72 * 60}),
                _c(name="updated_at", type="timestamp", sql_type="TEXT", gen="timestamp",
                   gen_args={"min_minutes_ago": 1, "max_minutes_ago": 180}),
            ],
            primary_key=["user_id"],
            freshness={"max_age": "PT24H", "column": "updated_at", "severity": "high"},
            consumers=["risk.feature_user_profile", "reporting.dw_users", "marketing.audience_builder"],
            owners=["user_center.team"],
        )
    )

    # ---------------- user_center.user_devices ----------------
    tables.append(
        SimTable(
            system="user_center",
            name="user_devices",
            description="User device bindings.",
            columns=[
                _c(name="device_id", type="string", sql_type="TEXT", unique=True, gen="uuid"),
                _c(name="user_id", type="string", sql_type="TEXT", gen="ref", gen_args={"from": "user_id"}),
                _c(name="device_type", type="string", sql_type="TEXT", gen="enum", enum=["ios", "android", "web", "tablet"]),
                _c(name="os_version", type="string", sql_type="TEXT", gen="text"),
                _c(name="push_token", type="string", sql_type="TEXT", required=False, gen="uuid"),
                _c(name="last_seen_at", type="timestamp", sql_type="TEXT", gen="timestamp"),
            ],
            primary_key=["device_id"],
            unique_keys=[["user_id", "device_type"]],
            consumers=["marketing.audience_builder", "risk.feature_device_graph"],
            owners=["user_center.team"],
        )
    )

    # ---------------- risk_engine.risk_events ----------------
    tables.append(
        SimTable(
            system="risk_engine",
            name="risk_events",
            description="Realtime risk decisions.",
            columns=[
                _c(name="event_id", type="string", sql_type="TEXT", unique=True, gen="uuid"),
                _c(name="user_id", type="string", sql_type="TEXT", gen="ref", gen_args={"from": "user_id"}),
                _c(name="event_type", type="string", sql_type="TEXT", gen="enum",
                   enum=["login", "payment", "withdraw", "register", "device_change"]),
                _c(name="score", type="int", sql_type="INTEGER", minimum=0, maximum=1000, gen="int_range",
                   gen_args={"lo": 0, "hi": 1000}),
                _c(name="decision", type="string", sql_type="TEXT", gen="enum",
                   enum=["allow", "review", "reject"], gen_args={"weights": [7, 2, 1]}),
                _c(name="occurred_at", type="timestamp", sql_type="TEXT", gen="timestamp"),
                _c(name="merchant_id", type="string", sql_type="TEXT", required=False, gen="seq",
                   gen_args={"prefix": "M", "start": 5000}),
            ],
            primary_key=["event_id"],
            freshness={"max_age": "PT6H", "column": "occurred_at", "severity": "critical"},
            consumers=["risk.feature_event_history", "risk.model_training"],
            owners=["risk_engine.team"],
        )
    )

    # ---------------- merchant.merchants ----------------
    tables.append(
        SimTable(
            system="merchant",
            name="merchants",
            description="Merchant master data.",
            columns=[
                _c(name="merchant_id", type="string", sql_type="TEXT", unique=True, gen="seq", gen_args={"prefix": "M", "start": 5000}),
                _c(name="merchant_name", type="string", sql_type="TEXT", gen="name"),
                _c(name="category", type="string", sql_type="TEXT", gen="enum",
                   enum=["retail", "food", "travel", "digital", "services"]),
                _c(name="country", type="string", sql_type="TEXT", gen="enum", enum=["CN", "HK", "SG", "US"]),
                _c(name="mcc", type="string", sql_type="TEXT", gen="text"),
                _c(name="onboarded_at", type="date", sql_type="TEXT", gen="date", gen_args={"max_days_ago": 700}),
                _c(name="status", type="string", sql_type="TEXT", gen="enum", enum=["active", "suspended", "closed"]),
            ],
            primary_key=["merchant_id"],
            consumers=["reporting.dw_merchants", "risk.feature_merchant_risk"],
            owners=["merchant.team"],
        )
    )

    # ---------------- merchant.merchant_daily_stats ----------------
    tables.append(
        SimTable(
            system="merchant",
            name="merchant_daily_stats",
            description="Daily aggregate per merchant.",
            columns=[
                _c(name="merchant_id", type="string", sql_type="TEXT", gen="seq", gen_args={"prefix": "M", "start": 5000}),
                _c(name="stat_date", type="date", sql_type="TEXT", gen="date", gen_args={"max_days_ago": 30}),
                _c(name="txn_count", type="int", sql_type="INTEGER", minimum=0, maximum=1000000, gen="int_range",
                   gen_args={"lo": 0, "hi": 5000}),
                _c(name="gmv", type="decimal", sql_type="REAL", minimum=0, maximum=100000000,
                   precision=16, scale=2, gen="decimal_range", gen_args={"lo": 0, "hi": 500000}),
                _c(name="refund_count", type="int", sql_type="INTEGER", minimum=0, maximum=1000000, gen="int_range",
                   gen_args={"lo": 0, "hi": 200}),
                _c(name="updated_at", type="timestamp", sql_type="TEXT", gen="timestamp",
                   gen_args={"min_minutes_ago": 1, "max_minutes_ago": 300}),
            ],
            primary_key=["merchant_id", "stat_date"],
            consumers=["reporting.dw_merchant_daily", "finance.recon_daily"],
            owners=["merchant.team"],
        )
    )

    by_system: dict[str, list[SimTable]] = {}
    if rows_per_table > 0:
        for t in tables:
            t.rows = generate_rows(t, rows_per_table, seed=seed)
    for t in tables:
        by_system.setdefault(t.system, []).append(t)
    return by_system


def all_tables(by_system: dict[str, list[SimTable]]) -> list[SimTable]:
    return [t for tables in by_system.values() for t in tables]
