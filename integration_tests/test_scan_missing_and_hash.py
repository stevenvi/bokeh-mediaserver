"""
Integration tests for missing-file restoration and hash-change detection.

TestMissingFileRestoration: verifies that items marked missing when a collection
path becomes unreachable are restored (without re-processing) when the path is
corrected.

TestHashChangeDetection: verifies that corrupting a file_hash in the DB causes
the next scan to detect the change, re-process the file, and update the hash —
without creating duplicate rows.
"""

import psycopg
import httpx
import pytest
from conftest import BASE_URL
from helpers.auth import bearer
from helpers.poll import wait_for_job


# ── Shared helpers ─────────────────────────────────────────────────────────────

def trigger_collection_scan(token: str, collection_id: int) -> int:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/jobs",
        headers=bearer(token),
        json={"type": "collection_scan", "related_id": collection_id, "related_type": "collection"},
    )
    r.raise_for_status()
    return r.json()["id"]


def get_job(token: str, job_id: int) -> dict:
    r = httpx.get(f"{BASE_URL}/api/v1/admin/jobs/{job_id}", headers=bearer(token))
    r.raise_for_status()
    return r.json()


def list_artists(token: str, collection_id: int) -> list:
    r = httpx.get(
        f"{BASE_URL}/api/v1/collections/{collection_id}/artists",
        headers=bearer(token),
    )
    r.raise_for_status()
    return r.json()["artists"]


_TREE_CTE = """
    WITH RECURSIVE tree AS (
        SELECT id FROM collections WHERE id = %s
        UNION ALL
        SELECT c.id FROM collections c JOIN tree t ON c.parent_collection_id = t.id
    )
"""


def count_rows(dsn: str, collection_id: int) -> tuple[int, int, int]:
    """Returns (media_item_count, collection_count, audio_metadata_count)."""
    with psycopg.connect(dsn) as conn:
        items = conn.execute(
            _TREE_CTE + " SELECT COUNT(*) FROM media_items WHERE collection_id IN (SELECT id FROM tree)",
            [collection_id],
        ).fetchone()[0]
        cols = conn.execute(
            _TREE_CTE + " SELECT COUNT(*) FROM tree",
            [collection_id],
        ).fetchone()[0]
        meta = conn.execute(
            _TREE_CTE + """
            SELECT COUNT(*) FROM audio_metadata am
            JOIN media_items mi ON mi.id = am.media_item_id
            WHERE mi.collection_id IN (SELECT id FROM tree)
            """,
            [collection_id],
        ).fetchone()[0]
    return items, cols, meta


def create_music_collection(token: str, name: str) -> tuple[int, int]:
    """Creates an audio:music collection on music-collection. Returns (collection_id, scan_job_id)."""
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(token),
        json={"name": name, "type": "audio:music", "relative_path": "music-collection"},
    )
    r.raise_for_status()
    data = r.json()
    return data["id"], data["scan_job_id"]


# ── TestMissingFileRestoration ─────────────────────────────────────────────────

class TestMissingFileRestoration:
    """
    Verifies that when a collection's path becomes unreachable all items are
    marked missing, and when the path is restored all items are un-marked without
    triggering re-processing.
    """

    collection_id: int = 0
    baseline: tuple[int, int, int] = (0, 0, 0)

    def test_01_create_and_scan(self, admin_token, db_dsn):
        collection_id, scan_job_id = create_music_collection(
            admin_token, "Missing File Test"
        )
        TestMissingFileRestoration.collection_id = collection_id

        wait_for_job(admin_token, scan_job_id, timeout=60)

        artists = list_artists(admin_token, collection_id)
        assert len(artists) == 1, f"expected 1 artist after initial scan, got {len(artists)}"

        TestMissingFileRestoration.baseline = count_rows(db_dsn, collection_id)
        items, cols, meta = TestMissingFileRestoration.baseline
        assert items > 0, "expected media items after initial scan"
        assert meta > 0, "expected audio metadata after initial scan"

    def test_02_simulate_unreachable_path(self, admin_token, db_dsn):
        if not TestMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestMissingFileRestoration.collection_id

        # Point the collection at a path that does not exist so the walk finds nothing.
        with psycopg.connect(db_dsn) as conn:
            conn.execute(
                "UPDATE collections SET relative_path = 'does_not_exist' WHERE id = %s",
                [collection_id],
            )
            conn.commit()

        job_id = trigger_collection_scan(admin_token, collection_id)
        wait_for_job(admin_token, job_id, timeout=60)

        artists = list_artists(admin_token, collection_id)
        assert len(artists) == 0, (
            f"expected 0 artists after path made unreachable, got {len(artists)}"
        )

        job = get_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs when only marking items missing, got {job['subjobs_enqueued']}"
        )

    def test_03_restore_path_no_reprocessing(self, admin_token, db_dsn):
        if not TestMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestMissingFileRestoration.collection_id

        # Restore the correct path.
        with psycopg.connect(db_dsn) as conn:
            conn.execute(
                "UPDATE collections SET relative_path = 'music-collection' WHERE id = %s",
                [collection_id],
            )
            conn.commit()

        job_id = trigger_collection_scan(admin_token, collection_id)
        wait_for_job(admin_token, job_id, timeout=60)

        artists = list_artists(admin_token, collection_id)
        assert len(artists) == 1, (
            f"expected 1 artist after path restored, got {len(artists)}"
        )

        job = get_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on restore (hash unchanged, metadata present), "
            f"got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, collection_id)
        assert current == TestMissingFileRestoration.baseline, (
            f"row counts changed after restore: baseline={TestMissingFileRestoration.baseline}, "
            f"current={current}"
        )


# ── TestHashChangeDetection ────────────────────────────────────────────────────

class TestHashChangeDetection:
    """
    Verifies that when stored file_hashes differ from the actual file content
    the scan detects the change, updates the hash, and re-queues a sub-job for
    each affected item — without creating duplicate rows.
    """

    collection_id: int = 0
    baseline: tuple[int, int, int] = (0, 0, 0)

    def test_01_create_and_scan(self, admin_token, db_dsn):
        collection_id, scan_job_id = create_music_collection(
            admin_token, "Hash Change Test"
        )
        TestHashChangeDetection.collection_id = collection_id

        wait_for_job(admin_token, scan_job_id, timeout=60)

        artists = list_artists(admin_token, collection_id)
        assert len(artists) == 1, f"expected 1 artist after initial scan, got {len(artists)}"

        TestHashChangeDetection.baseline = count_rows(db_dsn, collection_id)
        items, cols, meta = TestHashChangeDetection.baseline
        assert items > 0, "expected media items after initial scan"
        assert meta > 0, "expected audio metadata after initial scan"

    def test_02_rescan_is_idempotent(self, admin_token, db_dsn):
        if not TestHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestHashChangeDetection.collection_id

        job_id = trigger_collection_scan(admin_token, collection_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on idempotent rescan, got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, collection_id)
        assert current == TestHashChangeDetection.baseline, (
            f"row counts changed on idempotent rescan: baseline={TestHashChangeDetection.baseline}, "
            f"current={current}"
        )

    def test_03_corrupt_hashes_triggers_reprocessing(self, admin_token, db_dsn):
        if not TestHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestHashChangeDetection.collection_id
        baseline_items, _, _ = TestHashChangeDetection.baseline

        # Corrupt every item's stored hash so the scan detects a change.
        with psycopg.connect(db_dsn) as conn:
            conn.execute(
                _TREE_CTE + """
                UPDATE media_items SET file_hash = 'deadbeef'
                WHERE collection_id IN (SELECT id FROM tree)
                """,
                [collection_id],
            )
            conn.commit()

        job_id = trigger_collection_scan(admin_token, collection_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == baseline_items, (
            f"expected {baseline_items} sub-jobs (one per audio file), "
            f"got {job['subjobs_enqueued']}"
        )

        # Scan should have updated hashes to the real values before queuing sub-jobs.
        with psycopg.connect(db_dsn) as conn:
            stale = conn.execute(
                _TREE_CTE + """
                SELECT COUNT(*) FROM media_items
                WHERE collection_id IN (SELECT id FROM tree) AND file_hash = 'deadbeef'
                """,
                [collection_id],
            ).fetchone()[0]
        assert stale == 0, f"{stale} item(s) still have the corrupted hash after rescan"

        artists = list_artists(admin_token, collection_id)
        assert len(artists) == 1, (
            f"expected 1 artist after reprocessing, got {len(artists)}"
        )

        current = count_rows(db_dsn, collection_id)
        assert current == TestHashChangeDetection.baseline, (
            f"row counts changed after reprocessing: baseline={TestHashChangeDetection.baseline}, "
            f"current={current}"
        )
