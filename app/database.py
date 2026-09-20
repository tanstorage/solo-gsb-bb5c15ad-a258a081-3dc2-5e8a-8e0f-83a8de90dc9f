"""SQLite persistence layer.

A single connection is used together with WAL mode and a process-wide
re-entrant lock. The service runs with one Uvicorn worker (see the
Dockerfile / Compose file); all mutating transactions therefore serialise
through :attr:`Database.tx_lock`, which keeps interval accounting simple
and race free.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    total_size      INTEGER NOT NULL CHECK (total_size >= 0),
    sha256          TEXT NOT NULL,
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL DEFAULT 'application/octet-stream',
    status          TEXT NOT NULL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    expires_at      REAL NOT NULL,
    completed_at    REAL
);

CREATE INDEX IF NOT EXISTS idx_sessions_status_expires
    ON sessions(status, expires_at);

CREATE TABLE IF NOT EXISTS chunks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    start_offset    INTEGER NOT NULL CHECK (start_offset >= 0),
    end_offset      INTEGER NOT NULL CHECK (end_offset > start_offset),
    sha256          TEXT,
    received_at     REAL NOT NULL,
    UNIQUE(session_id, start_offset, end_offset)
);

CREATE INDEX IF NOT EXISTS idx_chunks_session_offsets
    ON chunks(session_id, start_offset, end_offset);
"""


class Database:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self.tx_lock = threading.RLock()
        self.init_schema()

    def init_schema(self) -> None:
        with self.tx_lock:
            self._conn.executescript(_SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialise writers and commit/rollback as a unit."""
        with self.tx_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self.tx_lock:
            return self._conn.execute(sql, params)

    def get_session(self, session_id: str) -> Optional[sqlite3.Row]:
        with self.tx_lock:
            cur = self._conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            )
            return cur.fetchone()

    def list_chunks(self, session_id: str) -> list[sqlite3.Row]:
        with self.tx_lock:
            cur = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? "
                "ORDER BY start_offset, end_offset",
                (session_id,),
            )
            return cur.fetchall()

    def close(self) -> None:
        with self.tx_lock:
            self._conn.close()
