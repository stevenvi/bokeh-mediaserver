"""
Collection-scan tests for image:photo collections.

TestPhotoMissingFileRestoration: items marked missing when the collection path
becomes unreachable are restored (without re-processing) when the path is
corrected.

TestPhotoHashChangeDetection: corrupting file_hash triggers re-processing on the
next scan and updates hashes without creating duplicate rows.

Both tests also verify the image serving endpoint (/images/{id}/tiles/image.dzi)
returns data when items are available and 404 when items are marked missing.

Note: the server-side fix for MediaItemFileHashAndPath (adding missing_since IS NULL
filter) is required for the 404 assertions in test_02 to pass.
"""

import httpx
import pytest

from tests.jobs.collection_scan.common import (
    BASE_URL, RowCounts, bearer, wait_for_job,
    create_collection, count_rows, count_non_missing_items, get_first_item_id,
    corrupt_hashes, count_stale_hashes, set_collection_path,
    trigger_collection_scan, get_scan_job,
    grant_collection_access,
)

_PHOTO_PATH = "photo-album-1"
_COLLECTION_TYPE = "image:photo"


def _dzi_url(item_id: int) -> str:
    return f"{BASE_URL}/images/{item_id}/tiles/image.dzi"


# ── TestPhotoMissingFileRestoration ───────────────────────────────────────────

class TestPhotoMissingFileRestoration:
    """
    Verifies that when an image:photo collection path becomes unreachable all
    items are marked missing (DZI serving fails with 404), and when the path is
    restored all items are un-marked without triggering re-processing (DZI works
    again, 0 sub-jobs).
    """

    collection_id: int = 0
    baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
    item_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        col_id, scan_job_id = create_collection(
            admin_token, "Photo Missing Test", _COLLECTION_TYPE, _PHOTO_PATH
        )
        TestPhotoMissingFileRestoration.collection_id = col_id

        wait_for_job(admin_token, scan_job_id, timeout=120)

        visible = count_non_missing_items(db_dsn, col_id)
        assert visible > 0, f"expected items after initial scan, got {visible}"

        grant_collection_access(admin_token, admin_user_id, col_id)

        item_id = get_first_item_id(db_dsn, col_id)
        assert item_id is not None, "expected at least one media item after scan"
        TestPhotoMissingFileRestoration.item_id = item_id

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from DZI manifest endpoint, got {r.status_code}"
        )

        TestPhotoMissingFileRestoration.baseline = count_rows(db_dsn, col_id)
        rc = TestPhotoMissingFileRestoration.baseline
        assert rc.items > 0, "expected media items after initial scan"
        assert rc.photo_meta > 0, "expected photo metadata after initial scan"

    def test_02_simulate_unreachable_path(self, admin_token, db_dsn):
        if not TestPhotoMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestPhotoMissingFileRestoration.collection_id
        item_id = TestPhotoMissingFileRestoration.item_id

        set_collection_path(db_dsn, col_id, "does_not_exist")

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        visible = count_non_missing_items(db_dsn, col_id)
        assert visible == 0, (
            f"expected 0 visible items after path made unreachable, got {visible}"
        )

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 404, (
            f"expected 404 from DZI endpoint for missing item, got {r.status_code}"
        )

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs when only marking items missing, got {job['subjobs_enqueued']}"
        )

    def test_03_restore_path_no_reprocessing(self, admin_token, db_dsn):
        if not TestPhotoMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestPhotoMissingFileRestoration.collection_id
        item_id = TestPhotoMissingFileRestoration.item_id

        set_collection_path(db_dsn, col_id, _PHOTO_PATH)

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=120)

        visible = count_non_missing_items(db_dsn, col_id)
        assert visible == TestPhotoMissingFileRestoration.baseline.items, (
            f"expected {TestPhotoMissingFileRestoration.baseline.items} visible items "
            f"after path restored, got {visible}"
        )

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from DZI endpoint after restore, got {r.status_code}"
        )

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on restore (hash unchanged, metadata present), "
            f"got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestPhotoMissingFileRestoration.baseline, (
            f"row counts changed after restore: "
            f"baseline={TestPhotoMissingFileRestoration.baseline}, current={current}"
        )


# ── TestPhotoHashChangeDetection ──────────────────────────────────────────────

class TestPhotoHashChangeDetection:
    """
    Verifies that when stored file_hashes differ from actual content, the scan
    detects the change, updates hashes, and re-queues scan_photo sub-jobs without
    creating duplicate rows. DZI serving is also verified at each stage.
    """

    collection_id: int = 0
    baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
    item_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        col_id, scan_job_id = create_collection(
            admin_token, "Photo Hash Test", _COLLECTION_TYPE, _PHOTO_PATH
        )
        TestPhotoHashChangeDetection.collection_id = col_id

        wait_for_job(admin_token, scan_job_id, timeout=120)

        visible = count_non_missing_items(db_dsn, col_id)
        assert visible > 0, f"expected items after initial scan, got {visible}"

        grant_collection_access(admin_token, admin_user_id, col_id)

        item_id = get_first_item_id(db_dsn, col_id)
        assert item_id is not None, "expected at least one media item after scan"
        TestPhotoHashChangeDetection.item_id = item_id

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from DZI manifest endpoint, got {r.status_code}"
        )

        TestPhotoHashChangeDetection.baseline = count_rows(db_dsn, col_id)
        rc = TestPhotoHashChangeDetection.baseline
        assert rc.items > 0, "expected media items after initial scan"
        assert rc.photo_meta > 0, "expected photo metadata after initial scan"

    def test_02_rescan_is_idempotent(self, admin_token, db_dsn):
        if not TestPhotoHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestPhotoHashChangeDetection.collection_id
        item_id = TestPhotoHashChangeDetection.item_id

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=120)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on idempotent rescan, got {job['subjobs_enqueued']}"
        )

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from DZI endpoint on idempotent rescan, got {r.status_code}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestPhotoHashChangeDetection.baseline, (
            f"row counts changed on idempotent rescan: "
            f"baseline={TestPhotoHashChangeDetection.baseline}, current={current}"
        )

    def test_03_corrupt_hashes_triggers_reprocessing(self, admin_token, db_dsn):
        if not TestPhotoHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestPhotoHashChangeDetection.collection_id
        item_id = TestPhotoHashChangeDetection.item_id
        baseline_items = TestPhotoHashChangeDetection.baseline.items

        corrupt_hashes(db_dsn, col_id)

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=120)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == baseline_items, (
            f"expected {baseline_items} sub-jobs (one per photo file), "
            f"got {job['subjobs_enqueued']}"
        )

        assert count_stale_hashes(db_dsn, col_id) == 0, (
            "some items still have the corrupted hash after rescan"
        )

        r = httpx.get(_dzi_url(item_id), headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from DZI endpoint after reprocessing, got {r.status_code}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestPhotoHashChangeDetection.baseline, (
            f"row counts changed after reprocessing: "
            f"baseline={TestPhotoHashChangeDetection.baseline}, current={current}"
        )
