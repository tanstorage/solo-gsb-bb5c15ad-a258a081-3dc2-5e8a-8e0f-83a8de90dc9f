"""Runtime configuration loaded from environment variables.

Everything can be overridden through the process environment so that the
Docker Compose file can configure the service without rebuilding images.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    storage_dir: Path
    db_path: Path
    staging_dir: Path
    files_dir: Path
    session_ttl_seconds: float
    cleanup_interval_seconds: float
    max_file_size: int
    max_chunk_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        storage_dir = Path(os.getenv("STORAGE_DIR", "/data")).resolve()
        staging_dir = storage_dir / "staging"
        files_dir = storage_dir / "files"
        db_path = Path(
            os.getenv("DATABASE_PATH", str(storage_dir / "app.db"))
        ).resolve()
        return cls(
            storage_dir=storage_dir,
            db_path=db_path,
            staging_dir=staging_dir,
            files_dir=files_dir,
            session_ttl_seconds=_env_float("SESSION_TTL_SECONDS", 86400),
            cleanup_interval_seconds=_env_float("CLEANUP_INTERVAL_SECONDS", 300),
            # 0 means unlimited for the size limits.
            max_file_size=_env_int("MAX_FILE_SIZE", 0),
            max_chunk_size=_env_int("MAX_CHUNK_SIZE", 0),
        )
