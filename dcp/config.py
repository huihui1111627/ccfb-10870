"""Runtime configuration, resolved from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser().resolve() if raw else default


@dataclass
class Settings:
    home: Path = _env_path("DCP_HOME", ROOT / "data")
    database: Path | None = None
    upstream_dir: Path | None = None
    admin_token: str = os.environ.get("DCP_ADMIN_TOKEN", "")
    transition_days: int = int(os.environ.get("DCP_TRANSITION_DAYS", "7"))
    alert_quiet_hours: int = int(os.environ.get("DCP_ALERT_QUIET_HOURS", "24"))
    sample_rows: int = int(os.environ.get("DCP_SAMPLE_ROWS", "2000"))

    def __post_init__(self) -> None:
        if self.database is None:
            self.database = self.home / "dcp.db"
        if self.upstream_dir is None:
            self.upstream_dir = self.home / "upstreams"
        self.home.mkdir(parents=True, exist_ok=True)
        self.upstream_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
