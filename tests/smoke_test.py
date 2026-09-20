"""End-to-end smoke tests runnable without pytest:

    python tests/smoke_test.py

Exercises the API through FastAPI's TestClient against an isolated
STORAGE_DIR. Exits non-zero on the first failed assertion.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile

# Point the service at an isolated data directory before importing the app.
DATA_DIR = tempfile.mkdtemp(prefix="resumable-smoke-")
os.environ["STORAGE_DIR"] = DATA_DIR
os.environ["SESSION_TTL_SECONDS"] = "2"
os.environ["CLEANUP_INTERVAL_SECONDS"] = "1"
os.environ["MAX_FILE_SIZE"] = "1048576"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def create_session(client: TestClient, data: bytes, filename: str = "file.bin"):
    digest = hashlib.sha256(data).hexdigest()
    r = client.post(
        "/sessions",
        json={
            "total_size": len(data),
            "sha256": digest,
            "filename": filename,
            "content_type": "application/octet-stream",
        },
    )
    check("create session -> 201", r.status_code == 201, r.text)
    body = r.json()
    check(
        "new session reports full missing range",
        body["missing_ranges"] == ([[0, len(data)]] if data else []),
        str(body["missing_ranges"]),
    )
    check("new session status uploading", body["status"] == "uploading")
    return body["id"], digest


def put_chunk(client, sid, start, end, content, expect, headers=None):
    r = client.put(
        f"/sessions/{sid}/chunks",
        params={"start": start, "end": end},
        content=content,
        headers=headers,
    )
    check(
        f"chunk [{start},{end}) -> {expect}",
        r.status_code == expect,
        f"{r.status_code}: {r.text}",
    )
    return r


with TestClient(app) as client:
    r = client.get("/health")
    check("health ok", r.status_code == 200 and r.json() == {"status": "ok"})

    data = bytes(range(256)) * 8  # 2048 bytes

    # --- Happy path: out-of-order upload -----------------------------------
    sid, digest = create_session(client, data)

    put_chunk(client, sid, 1024, 2048, data[1024:2048], 201)
    r = put_chunk(client, sid, 0, 512, data[0:512], 201)
    check(
        "missing ranges reported between uploads",
        r.json()["missing_ranges"] == [[512, 1024]],
        str(r.json()["missing_ranges"]),
    )

    # --- Boundary / overlap rejection --------------------------------------
    put_chunk(client, sid, 512, 2100, data[512:2100 - 512], 416)  # past EOF
    put_chunk(client, sid, 2048, 2049, b"x", 416)                 # starts at EOF
    put_chunk(client, sid, 400, 600, data[400:600], 409)          # overlaps 0-512
    put_chunk(client, sid, 1024, 1100, data[1024:1100], 409)      # inside 1024-2048
    put_chunk(client, sid, 500, 500, b"", 400)                    # empty range

    # wrong body length for declared range
    put_chunk(client, sid, 512, 1024, data[512:1000], 400)

    # complete before coverage -> 409
    r = client.put(f"/sessions/{sid}/complete")
    check("incomplete finalize -> 409", r.status_code == 409, r.text)

    # download unavailable
    r = client.get(f"/sessions/{sid}/download")
    check("download before completion -> 409", r.status_code == 409, r.text)

    # fill the gap
    put_chunk(client, sid, 512, 1024, data[512:1024], 201)

    # --- Idempotent exact retry --------------------------------------------
    r = put_chunk(client, sid, 0, 512, data[0:512], 200)
    check("exact duplicate flagged", r.json().get("duplicate") is True)
    put_chunk(client, sid, 0, 512, b"X" * 512, 409)  # same range, other bytes

    # --- Chunk checksum header ---------------------------------------------
    chunk = data[0:512]
    good = hashlib.sha256(chunk).hexdigest()
    put_chunk(client, sid, 0, 512, chunk, 200, headers={"X-Chunk-SHA256": good})
    put_chunk(
        client, sid, 0, 512, chunk, 422,
        headers={"X-Chunk-SHA256": "0" * 64},
    )

    # --- Atomic completion --------------------------------------------------
    r = client.put(f"/sessions/{sid}/complete")
    check("complete when covered -> 200", r.status_code == 200, r.text)
    check("completed status", r.json()["status"] == "completed")
    check("received bytes == total", r.json()["received_bytes"] == len(data))

    r = client.get(f"/sessions/{sid}/download")
    check("download -> 200", r.status_code == 200, r.text)
    check(
        "downloaded bytes match",
        r.content == data and hashlib.sha256(r.content).hexdigest() == digest,
    )
    check(
        "download carries checksum header",
        r.headers.get("X-Content-SHA256") == digest,
    )
    check(
        "content-length header set",
        r.headers.get("content-length") == str(len(data)),
    )

    # completed session rejects further chunk writes
    put_chunk(client, sid, 0, 1, data[0:1], 409)

    # --- SHA-256 mismatch: nothing must be downloadable --------------------
    bad = b"hello world"
    sid2, _ = create_session(client, bad, filename="bad.txt")
    put_chunk(client, sid2, 0, len(bad), b"HELLO WORLD", 201)
    r = client.put(f"/sessions/{sid2}/complete")
    check("checksum mismatch finalize -> 422", r.status_code == 422, r.text)

    r = client.get(f"/sessions/{sid2}/download")
    check("download after failed verify -> 409", r.status_code == 409, r.text)
    r = client.get(f"/sessions/{sid2}")
    check(
        "failed session back to uploading",
        r.json()["status"] == "uploading",
        r.text,
    )
    bad_final = os.path.join(DATA_DIR, "files", sid2)
    check("no final artifact after failed verify", not os.path.exists(bad_final))
    leftovers = [
        n for n in os.listdir(os.path.join(DATA_DIR, "files"))
        if sid2 in n
    ]
    check("no leftover artifact after failed verify", leftovers == [], str(leftovers))

    # repair by re-uploading: replace whole range requires delete+recreate
    client.delete(f"/sessions/{sid2}")
    sid2, _ = create_session(client, bad, filename="bad.txt")
    put_chunk(client, sid2, 0, len(bad), bad, 201)
    r = client.put(f"/sessions/{sid2}/complete")
    check("repaired upload completes", r.status_code == 200, r.text)

    # --- Empty file ---------------------------------------------------------
    sid3, _ = create_session(client, b"", filename="empty.dat")
    r = client.put(f"/sessions/{sid3}/complete")
    check("empty file completes with no chunks", r.status_code == 200, r.text)
    r = client.get(f"/sessions/{sid3}/download")
    check("empty file downloadable", r.status_code == 200 and r.content == b"")

    # --- Validation on session creation ------------------------------------
    r = client.post(
        "/sessions",
        json={"total_size": -1, "sha256": "a" * 64, "filename": "x"},
    )
    check("negative size rejected -> 422", r.status_code == 422)
    r = client.post(
        "/sessions",
        json={"total_size": 1, "sha256": "xyz", "filename": "x"},
    )
    check("bad sha256 rejected -> 422", r.status_code == 422)
    r = client.post(
        "/sessions",
        json={"total_size": 999999999, "sha256": "a" * 64, "filename": "x"},
    )
    check("oversized file rejected -> 413", r.status_code == 413, r.text)

    r = client.get("/sessions/does-not-exist")
    check("unknown session -> 404", r.status_code == 404)

    # --- Expiry cleanup -----------------------------------------------------
    sid4, _ = create_session(client, data, filename="expiring.bin")
    put_chunk(client, sid4, 0, 100, data[:100], 201)
    staging = os.path.join(DATA_DIR, "staging", sid4)
    check("staging dir exists while alive", os.path.isdir(staging))

    import time

    print("waiting for session expiry + background purge (~3s)...")
    gone = False
    for _ in range(30):
        time.sleep(0.5)
        r = client.get(f"/sessions/{sid4}")
        if r.status_code == 404:
            gone = True
            break
    check("expired session purged (404)", gone)
    check("expired staging data removed", not os.path.exists(staging))

    # a completed session survives the cleanup sweeps
    r = client.get(f"/sessions/{sid}/download")
    check("completed file survives cleanup", r.status_code == 200)

print()
if failures:
    print(f"{len(failures)} FAILURE(S): {failures}")
    sys.exit(1)
print("ALL SMOKE TESTS PASSED")
