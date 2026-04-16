"""
Shared helpers for collection-scan integration tests.

Imported by test_audio.py, test_photo.py, test_video.py, test_idempotency.py.
"""
import sys
from pathlib import Path

# Add integration_tests root and this directory to sys.path so helpers are importable.
for _p in [
    str(Path(__file__).resolve().parent.parent.parent),
    str(Path(__file__).resolve().parent),
]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import os
from collections import namedtuple

import httpx
import psycopg

from helpers.auth import bearer  # re-exported for test files
from helpers.poll import wait_for_job  # re-exported for test files

BASE_URL: str = os.environ.get("BASE_URL", "")

RowCounts = namedtuple("RowCounts", ["items", "cols", "audio_meta", "photo_meta", "video_meta"])

_TREE_CTE = """
    WITH RECURSIVE tree AS (
        SELECT id FROM collections WHERE id = %s
        UNION ALL
        SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
    )
"""

_EMPTY_COUNTS = RowCounts(0, 0, 0, 0, 0)


# ── Database helpers ───────────────────────────────────────────────────────────

def count_rows(dsn: str, collection_id: int) -> RowCounts:
    """Returns counts of media items, collections, and per-type metadata in one query."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            WITH RECURSIVE tree AS (
                SELECT id FROM collections WHERE id = %s
                UNION ALL
                SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
            )
            SELECT
                (SELECT COUNT(*) FROM media_items
                 WHERE collection_id IN (SELECT id FROM tree)),
                (SELECT COUNT(*) FROM tree),
                (SELECT COUNT(*) FROM audio_metadata am
                 JOIN media_items mi ON mi.id = am.media_item_id
                 WHERE mi.collection_id IN (SELECT id FROM tree)),
                (SELECT COUNT(*) FROM photo_metadata pm
                 JOIN media_items mi ON mi.id = pm.media_item_id
                 WHERE mi.collection_id IN (SELECT id FROM tree)),
                (SELECT COUNT(*) FROM video_metadata vm
                 JOIN media_items mi ON mi.id = vm.media_item_id
                 WHERE mi.collection_id IN (SELECT id FROM tree))
            """,
            [collection_id],
        ).fetchone()
    return RowCounts(*row)


def count_non_missing_items(dsn: str, collection_id: int) -> int:
    """Count media items with missing_since IS NULL in the collection tree."""
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            _TREE_CTE + """
            SELECT COUNT(*) FROM media_items
            WHERE collection_id IN (SELECT id FROM tree) AND missing_since IS NULL
            """,
            [collection_id],
        ).fetchone()[0]


def get_first_item_id(dsn: str, collection_id: int) -> int | None:
    """Return the ID of the first non-missing item in the collection tree, or None."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            _TREE_CTE + """
            SELECT id FROM media_items
            WHERE collection_id IN (SELECT id FROM tree) AND missing_since IS NULL
            ORDER BY id
            LIMIT 1
            """,
            [collection_id],
        ).fetchone()
    return row[0] if row else None


def corrupt_hashes(dsn: str, collection_id: int) -> None:
    """Set file_hash = 'deadbeef' for all items in the collection tree."""
    with psycopg.connect(dsn) as conn:
        conn.execute(
            _TREE_CTE + """
            UPDATE media_items SET file_hash = 'deadbeef'
            WHERE collection_id IN (SELECT id FROM tree)
            """,
            [collection_id],
        )
        conn.commit()


def count_stale_hashes(dsn: str, collection_id: int) -> int:
    """Count items still holding the corrupted 'deadbeef' hash."""
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            _TREE_CTE + """
            SELECT COUNT(*) FROM media_items
            WHERE collection_id IN (SELECT id FROM tree) AND file_hash = 'deadbeef'
            """,
            [collection_id],
        ).fetchone()[0]


def set_collection_path(dsn: str, collection_id: int, relative_path: str) -> None:
    """Update a collection's relative_path in the database."""
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE collections SET relative_path = %s WHERE id = %s",
            [relative_path, collection_id],
        )
        conn.commit()


# ── API helpers ────────────────────────────────────────────────────────────────

def trigger_collection_scan(token: str, collection_id: int) -> int:
    """Queue a collection_scan job and return the job ID."""
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/jobs",
        headers=bearer(token),
        json={"type": "collection_scan", "related_id": collection_id, "related_type": "collection"},
    )
    r.raise_for_status()
    return r.json()["id"]


def get_scan_job(token: str, job_id: int) -> dict:
    """Return the job record for the given job ID."""
    r = httpx.get(f"{BASE_URL}/api/v1/admin/jobs/{job_id}", headers=bearer(token))
    r.raise_for_status()
    return r.json()


def create_collection(token: str, name: str, collection_type: str, relative_path: str) -> tuple[int, int]:
    """Create a collection and return (collection_id, scan_job_id)."""
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(token),
        json={"name": name, "type": collection_type, "relative_path": relative_path},
    )
    r.raise_for_status()
    data = r.json()
    return data["id"], data["scan_job_id"]


def grant_collection_access(token: str, user_id: int, collection_id: int) -> None:
    """Grant the given user access to the root collection."""
    r = httpx.patch(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access",
        headers=bearer(token),
        json={"collection_ids": [collection_id]},
    )
    r.raise_for_status()


def list_artists(token: str, collection_id: int) -> list:
    """Return the list of artists for a music collection (no collection_access needed)."""
    r = httpx.get(
        f"{BASE_URL}/api/v1/collections/{collection_id}/artists",
        headers=bearer(token),
    )
    r.raise_for_status()
    return r.json()["artists"]
