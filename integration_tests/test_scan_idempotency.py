"""
Scan idempotency tests: verify that repeated filesystem and metadata scans do not
create duplicate media_items or collections in the database.
"""

import psycopg
import httpx
import pytest
from conftest import BASE_URL
from helpers.auth import bearer
from helpers.poll import wait_for_scan_and_processing


def create_collection(token: str, name: str, relative_path: str, col_type: str) -> tuple[int, int]:
    """Returns (collection_id, scan_job_id)."""
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(token),
        json={"name": name, "type": col_type, "relative_path": relative_path},
    )
    r.raise_for_status()
    data = r.json()
    return data["id"], data["scan_job_id"]


def trigger_scan(token: str, collection_id: int, scan_type: str) -> int:
    """Queues a scan job and returns the job_id."""
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections/{collection_id}/scan",
        headers=bearer(token),
        params={"type": scan_type},
    )
    r.raise_for_status()
    return r.json()["job_id"]


def count_rows(dsn: str, collection_id: int) -> tuple[int, int]:
    """Returns (media_item_count, collection_count) for the collection tree."""
    with psycopg.connect(dsn) as conn:
        item_count = conn.execute(
            """
            WITH RECURSIVE tree AS (
                SELECT id FROM collections WHERE id = %s
                UNION ALL
                SELECT c.id FROM collections c
                JOIN tree t ON c.parent_collection_id = t.id
            )
            SELECT COUNT(*) FROM media_items WHERE collection_id IN (SELECT id FROM tree)
            """,
            [collection_id],
        ).fetchone()[0]

        col_count = conn.execute(
            """
            WITH RECURSIVE tree AS (
                SELECT id FROM collections WHERE id = %s
                UNION ALL
                SELECT c.id FROM collections c
                JOIN tree t ON c.parent_collection_id = t.id
            )
            SELECT COUNT(*) FROM tree
            """,
            [collection_id],
        ).fetchone()[0]

    return item_count, col_count


class TestScanIdempotency:
    """
    Verifies that running a filesystem scan followed by a metadata scan on an
    already-scanned collection does not produce duplicate media_items or
    collections in the database.
    """

    collection_id: int = 0

    def test_01_create_and_initial_scan(self, admin_token, db_dsn):
        collection_id, scan_job_id = create_collection(
            admin_token, "Idempotency Music", "music-collection", "audio:music"
        )
        TestScanIdempotency.collection_id = collection_id
        wait_for_scan_and_processing(admin_token, scan_job_id, collection_id, db_dsn, timeout=60)

    def test_02_filesystem_scan_no_duplicates(self, admin_token, db_dsn):
        if not TestScanIdempotency.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestScanIdempotency.collection_id
        items_before, cols_before = count_rows(db_dsn, collection_id)
        assert items_before > 0, "expected media items after initial scan"
        assert cols_before > 0, "expected at least one collection"

        job_id = trigger_scan(admin_token, collection_id, "filesystem")
        wait_for_scan_and_processing(admin_token, job_id, collection_id, db_dsn, timeout=60)

        items_after, cols_after = count_rows(db_dsn, collection_id)
        assert items_after == items_before, (
            f"filesystem scan created duplicates: {items_before} items before, {items_after} after"
        )
        assert cols_after == cols_before, (
            f"filesystem scan created duplicate collections: {cols_before} before, {cols_after} after"
        )

    def test_03_metadata_scan_no_duplicates(self, admin_token, db_dsn):
        if not TestScanIdempotency.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        collection_id = TestScanIdempotency.collection_id
        items_before, cols_before = count_rows(db_dsn, collection_id)
        assert items_before > 0, "expected media items after filesystem scan"

        job_id = trigger_scan(admin_token, collection_id, "metadata")
        wait_for_scan_and_processing(admin_token, job_id, collection_id, db_dsn, timeout=60)

        items_after, cols_after = count_rows(db_dsn, collection_id)
        assert items_after == items_before, (
            f"metadata scan created duplicates: {items_before} items before, {items_after} after"
        )
        assert cols_after == cols_before, (
            f"metadata scan created duplicate collections: {cols_before} before, {cols_after} after"
        )
