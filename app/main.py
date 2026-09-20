"""FastAPI application: resumable, chunked file uploads over weak networks.

Flow
----
1. ``POST /sessions`` creates an upload session declared with the final
   file size and its whole-file SHA-256.
2. ``PUT  /sessions/{id}/chunks?start=&end=`` submits binary chunks in any
   order. Chunks outside the declared byte range, zero/negative length
   chunks, or chunks overlapping already accepted ranges are rejected.
3. ``GET  /sessions/{id}`` reports merged received ranges and the missing
   ranges the client still has to send.
4. ``PUT  /sessions/{id}/complete`` atomically assembles the file and
   verifies the whole-file SHA-256. A failed verification never leaves a
   downloadable artifact.
5. ``GET  /sessions/{id}/download`` serves only completed files.

Offsets are half-open: ``[start, end)`` contains ``end - start`` bytes.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .database import Database
from .intervals import merge_intervals, missing_intervals

logger = logging.getLogger("resumable_upload")

STATUS_UPLOADING = "uploading"
STATUS_COMPLETING = "completing"
STATUS_COMPLETED = "completed"

_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_FILENAME_INVALID = re.compile(r"[\x00-\x1f\x7f]")
_COPY_BLOCK = 1024 * 1024


# --------------------------------------------------------------------------- #
# Path / formatting helpers
# --------------------------------------------------------------------------- #


def _chunk_dir(settings: Settings, session_id: str) -> Path:
    return settings.staging_dir / session_id


def _chunk_path(settings: Settings, session_id: str, chunk_id: int) -> Path:
    return _chunk_dir(settings, session_id) / f"ch-{chunk_id:08d}.part"


def _final_path(settings: Settings, session_id: str) -> Path:
    return settings.files_dir / session_id


def _temp_path(settings: Settings, session_id: str) -> Path:
    return settings.files_dir / f".{session_id}.assemble.tmp"


def _iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _now() -> float:
    return time.time()


class CreateSessionRequest(BaseModel):
    total_size: int = Field(..., ge=0)
    sha256: str = Field(..., min_length=64, max_length=64)
    filename: str = Field(..., min_length=1, max_length=255)
    content_type: str = Field(default="application/octet-stream", max_length=255)


# --------------------------------------------------------------------------- #
# Application state / lifespan
# --------------------------------------------------------------------------- #


class AppState:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self._stop = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None

    def start_cleanup(self) -> None:
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, name="session-cleanup", daemon=True
        )
        self._cleanup_thread.start()

    def stop_cleanup(self) -> None:
        self._stop.set()
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=5)

    def _cleanup_loop(self) -> None:
        interval = self.settings.cleanup_interval_seconds
        while not self._stop.wait(interval):
            try:
                removed = purge_expired(self.settings, self.db)
                if removed:
                    logger.info("purged %d expired session(s)", removed)
            except Exception:  # pragma: no cover - defensive
                logger.exception("scheduled session cleanup failed")


def purge_expired(settings: Settings, db: Database) -> int:
    """Delete open sessions past their expiry, including staged bytes.

    Completed sessions are retained. Sessions in the transient
    ``completing`` state are skipped because a finalize is in flight.
    """
    now = _now()
    with db.transaction() as conn:
        rows = conn.execute(
            "SELECT id FROM sessions WHERE status = ? AND expires_at <= ?",
            (STATUS_UPLOADING, now),
        ).fetchall()
        ids = [row["id"] for row in rows]
        if not ids:
            return 0
        # Cascades remove the chunk rows; blob storage is removed below.
        conn.executemany("DELETE FROM sessions WHERE id = ?", [(i,) for i in ids])

    for session_id in ids:
        shutil.rmtree(_chunk_dir(settings, session_id), ignore_errors=True)
    return len(ids)


def _recover_stale_state(settings: Settings, db: Database) -> None:
    """Crash recovery: a session left in 'completing' never finished."""
    with db.transaction() as conn:
        conn.execute(
            "UPDATE sessions SET status = ?, updated_at = ? "
            "WHERE status = ?",
            (STATUS_UPLOADING, _now(), STATUS_COMPLETING),
        )
    if settings.files_dir.exists():
        for tmp in settings.files_dir.glob(".*.assemble.tmp"):
            try:
                tmp.unlink()
            except OSError:
                logger.exception("failed to remove stale temp file %s", tmp)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    settings.staging_dir.mkdir(parents=True, exist_ok=True)
    settings.files_dir.mkdir(parents=True, exist_ok=True)

    db = Database(settings.db_path)
    state = AppState(settings, db)
    _recover_stale_state(settings, db)
    purge_expired(settings, db)
    state.start_cleanup()

    app.state.settings = settings
    app.state.db = db
    try:
        yield
    finally:
        state.stop_cleanup()
        db.close()


app = FastAPI(
    title="Resumable File Upload API",
    version="1.0.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Status rendering
# --------------------------------------------------------------------------- #


def _status_payload(row: Any, chunk_rows: list[Any]) -> dict[str, Any]:
    received = merge_intervals(
        [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
    )
    total = row["total_size"]
    missing = missing_intervals(received, total)
    completed = row["status"] == STATUS_COMPLETED
    return {
        "id": row["id"],
        "filename": row["filename"],
        "content_type": row["content_type"],
        "total_size": total,
        "sha256": row["sha256"],
        "status": row["status"],
        "received_bytes": sum(end - start for start, end in received),
        "received_ranges": [[start, end] for start, end in received],
        "missing_ranges": [[start, end] for start, end in missing],
        "complete": not missing,
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
        "expires_at": _iso(row["expires_at"]),
        "completed_at": _iso(row["completed_at"]),
        "download_url": f"/sessions/{row['id']}/download" if completed else None,
    }


def _render_session(db: Database, row: Any) -> dict[str, Any]:
    return _status_payload(row, db.list_chunks(row["id"]))


def _require_active_upload(row: Any) -> None:
    """Map session row state to HTTP errors for upload/finalize calls."""
    if row["status"] == STATUS_COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="session is already completed",
        )
    if row["status"] == STATUS_COMPLETING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="session is currently being finalized",
        )
    if row["expires_at"] <= _now():
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="session has expired",
        )


# --------------------------------------------------------------------------- #
# Body streaming
# --------------------------------------------------------------------------- #


async def _stream_to_file(
    request: Request,
    dest: Path,
    expected_size: int,
    max_size: int,
) -> tuple[str, int]:
    """Stream the request body to ``dest``; return (sha256 hex, byte count)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    written = 0
    fh = open(dest, "wb")
    try:
        async for piece in request.stream():
            if not piece:
                continue
            written += len(piece)
            if written > expected_size or (max_size and written > max_size):
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"chunk body exceeds declared length {expected_size}",
                )
            digest.update(piece)
            fh.write(piece)
        fh.flush()
        os.fsync(fh.fileno())
    except HTTPException:
        fh.close()
        dest.unlink(missing_ok=True)
        raise
    except Exception:
        fh.close()
        dest.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="failed to read request body",
        )
    else:
        fh.close()
    return digest.hexdigest(), written


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sessions", status_code=status.HTTP_201_CREATED)
def create_session(
    payload: CreateSessionRequest,
    request: Request,
) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    db: Database = request.app.state.db

    if not _HEX64.match(payload.sha256):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="sha256 must be 64 hexadecimal characters",
        )
    filename = _FILENAME_INVALID.sub("", payload.filename).strip()
    if not filename:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="filename must not be empty",
        )
    if settings.max_file_size and payload.total_size > settings.max_file_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"file size {payload.total_size} exceeds limit "
                f"{settings.max_file_size}"
            ),
        )

    now = _now()
    session_id = uuid.uuid4().hex
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO sessions ("
            "id, total_size, sha256, filename, content_type, status, "
            "created_at, updated_at, expires_at, completed_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                session_id,
                payload.total_size,
                payload.sha256.lower(),
                filename,
                payload.content_type,
                STATUS_UPLOADING,
                now,
                now,
                now + settings.session_ttl_seconds,
            ),
        )
        row = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()

    logger.info(
        "created session %s (%d bytes, ttl=%ds)",
        session_id,
        payload.total_size,
        int(settings.session_ttl_seconds),
    )
    return _status_payload(row, [])


@app.put("/sessions/{session_id}/chunks")
async def upload_chunk(
    session_id: str,
    request: Request,
    start: int = Query(..., ge=0, description="first byte offset (inclusive)"),
    end: int = Query(..., ge=1, description="last byte offset (exclusive)"),
    x_chunk_sha256: Optional[str] = Header(default=None, alias="X-Chunk-SHA256"),
) -> JSONResponse:
    settings: Settings = request.app.state.settings
    db: Database = request.app.state.db

    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="session not found"
        )
    _require_active_upload(row)

    total = row["total_size"]
    if end <= start:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end must be greater than start (ranges are [start, end))",
        )
    if start >= total or end > total:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail=f"range [{start}, {end}) is outside the file [0, {total})",
        )
    expected_len = end - start
    if settings.max_chunk_size and expected_len > settings.max_chunk_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"chunk length {expected_len} exceeds limit "
                f"{settings.max_chunk_size}"
            ),
        )

    declared_length = request.headers.get("content-length")
    if declared_length is not None:
        try:
            if int(declared_length) != expected_len:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Content-Length {declared_length} does not match "
                        f"range length {expected_len}"
                    ),
                )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid Content-Length header",
            )

    if x_chunk_sha256 is not None and not _HEX64.match(x_chunk_sha256):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Chunk-SHA256 must be 64 hexadecimal characters",
        )

    # Stream the bytes to a unique temporary path BEFORE inserting the row,
    # so invalid bodies never touch the interval bookkeeping.
    staging = _chunk_dir(settings, session_id)
    incoming = staging / f".incoming-{uuid.uuid4().hex}.tmp"
    body_hash, body_len = await _stream_to_file(
        request, incoming, expected_len, settings.max_chunk_size
    )
    try:
        if body_len != expected_len:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"received {body_len} bytes but range declares "
                    f"{expected_len}"
                ),
            )
        if x_chunk_sha256 is not None and body_hash != x_chunk_sha256.lower():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="chunk sha256 does not match X-Chunk-SHA256",
            )

        # Serialise the overlap check + insert in one write transaction.
        with db.transaction() as conn:
            current = conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if current is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="session not found",
                )
            _require_active_upload(current)

            existing = conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? "
                "ORDER BY start_offset, end_offset",
                (session_id,),
            ).fetchall()

            exact = next(
                (
                    r
                    for r in existing
                    if r["start_offset"] == start and r["end_offset"] == end
                ),
                None,
            )
            if exact is not None:
                # Idempotent retry of the exact same bytes; different bytes
                # for the same offsets are a conflict.
                if exact["sha256"] != body_hash:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            f"range [{start}, {end}) was already uploaded "
                            "with different content"
                        ),
                    )
                response_status = status.HTTP_200_OK
                duplicate = True
            else:
                hit = next(
                    (
                        r
                        for r in existing
                        if start < r["end_offset"] and r["start_offset"] < end
                    ),
                    None,
                )
                if hit is not None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            f"range [{start}, {end}) overlaps accepted "
                            f"range [{hit['start_offset']}, "
                            f"{hit['end_offset']})"
                        ),
                    )
                cur = conn.execute(
                    "INSERT INTO chunks ("
                    "session_id, start_offset, end_offset, sha256, received_at"
                    ") VALUES (?, ?, ?, ?, ?)",
                    (session_id, start, end, body_hash, _now()),
                )
                chunk_id = cur.lastrowid
                conn.execute(
                    "UPDATE sessions SET updated_at = ? WHERE id = ?",
                    (_now(), session_id),
                )
                response_status = status.HTTP_201_CREATED
                duplicate = False

                target = _chunk_path(settings, session_id, chunk_id)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(incoming, target)

            chunks = conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? "
                "ORDER BY start_offset, end_offset",
                (session_id,),
            ).fetchall()
            refreshed = conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()

        payload = _status_payload(refreshed, chunks)
        payload["duplicate"] = duplicate
        return JSONResponse(content=payload, status_code=response_status)
    finally:
        incoming.unlink(missing_ok=True)


@app.get("/sessions/{session_id}")
def get_session(session_id: str, request: Request) -> dict[str, Any]:
    db: Database = request.app.state.db
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="session not found"
        )
    if row["status"] == STATUS_UPLOADING and row["expires_at"] <= _now():
        raise HTTPException(
            status_code=status.HTTP_410_GONE, detail="session has expired"
        )
    return _render_session(db, row)


@app.put("/sessions/{session_id}/complete")
def complete_session(session_id: str, request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    db: Database = request.app.state.db

    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="session not found",
            )
        _require_active_upload(row)
        chunk_rows = conn.execute(
            "SELECT * FROM chunks WHERE session_id = ? "
            "ORDER BY start_offset, end_offset",
            (session_id,),
        ).fetchall()
        ranges = merge_intervals(
            [(r["start_offset"], r["end_offset"]) for r in chunk_rows]
        )
        gaps = missing_intervals(ranges, row["total_size"])
        if gaps:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": "upload is incomplete", "missing": gaps},
            )
        # Lock the session against chunk uploads and concurrent finalizes.
        conn.execute(
            "UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?",
            (STATUS_COMPLETING, _now(), session_id),
        )
        ordered_ids = [r["id"] for r in chunk_rows]

    # Assemble to a temp file outside the database lock. Cleanup skips
    # 'completing' rows, so the session cannot be purged mid-assembly.
    final = _final_path(settings, session_id)
    tmp = _temp_path(settings, session_id)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    assembled = 0
    try:
        with open(tmp, "wb") as fh:
            for chunk_id in ordered_ids:
                part = _chunk_path(settings, session_id, chunk_id)
                if not part.exists():
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"stored chunk blob missing: {part.name}",
                    )
                with open(part, "rb") as src:
                    while True:
                        block = src.read(_COPY_BLOCK)
                        if not block:
                            break
                        digest.update(block)
                        assembled += len(block)
                        fh.write(block)
            fh.flush()
            os.fsync(fh.fileno())

        with db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="session not found",
                )

            failure_reason: Optional[str] = None
            if assembled != row["total_size"]:
                failure_reason = (
                    f"assembled size {assembled} != declared "
                    f"{row['total_size']}"
                )
            elif digest.hexdigest() != row["sha256"]:
                failure_reason = (
                    f"sha256 mismatch: got {digest.hexdigest()}, "
                    f"expected {row['sha256']}"
                )

            if failure_reason is not None:
                # Reopen for uploads; never publish the temp file.
                conn.execute(
                    "UPDATE sessions SET status = ?, updated_at = ? "
                    "WHERE id = ?",
                    (STATUS_UPLOADING, _now(), session_id),
                )
            else:
                # Atomic publish of the verified artifact.
                os.replace(tmp, final)
                conn.execute(
                    "UPDATE sessions SET status = ?, updated_at = ?, "
                    "completed_at = ? WHERE id = ?",
                    (STATUS_COMPLETED, _now(), _now(), session_id),
                )

            chunks = conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? "
                "ORDER BY start_offset, end_offset",
                (session_id,),
            ).fetchall()
            refreshed = conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
    except HTTPException:
        _reset_to_uploading(db, session_id)
        raise
    except Exception:
        logger.exception("assembly failed for session %s", session_id)
        _reset_to_uploading(db, session_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="file assembly failed",
        )
    finally:
        tmp.unlink(missing_ok=True)

    if refreshed["status"] != STATUS_COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": (
                    "file verification failed; no downloadable artifact "
                    "was created"
                ),
                "reason": failure_reason,
            },
        )

    return _status_payload(refreshed, chunks)


def _reset_to_uploading(db: Database, session_id: str) -> None:
    try:
        with db.transaction() as conn:
            conn.execute(
                "UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?",
                (STATUS_UPLOADING, _now(), session_id),
            )
    except Exception:  # pragma: no cover - defensive
        logger.exception("could not reset session %s to uploading", session_id)


@app.get("/sessions/{session_id}/download")
def download_session(session_id: str, request: Request) -> FileResponse:
    settings: Settings = request.app.state.settings
    db: Database = request.app.state.db
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="session not found"
        )
    if row["status"] != STATUS_COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="file is not available for download yet",
        )
    final = _final_path(settings, session_id)
    if not final.exists():
        # Defense in depth: metadata says complete but blob is missing.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="file artifact is unavailable",
        )
    return FileResponse(
        final,
        media_type=row["content_type"],
        filename=row["filename"],
        headers={
            "Content-Length": str(row["total_size"]),
            "X-Content-SHA256": row["sha256"],
        },
    )


@app.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_session(session_id: str, request: Request):
    settings: Settings = request.app.state.settings
    db: Database = request.app.state.db
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="session not found",
            )
        if row["status"] == STATUS_COMPLETING:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="cannot delete while finalization is in progress",
            )
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    shutil.rmtree(_chunk_dir(settings, session_id), ignore_errors=True)
    _final_path(settings, session_id).unlink(missing_ok=True)
    _temp_path(settings, session_id).unlink(missing_ok=True)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
