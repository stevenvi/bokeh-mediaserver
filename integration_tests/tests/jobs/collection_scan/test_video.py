"""
Collection-scan tests for video:movie and video:home_movie collections.

The scan behaviour and streaming semantics are identical for both collection
types — the only difference is the type string used at creation time. A factory
function generates test classes for each type, ensuring the logic is defined
once while maintaining isolated class-level state per type.

Four classes are produced:
  TestVideoMovieMissingFileRestoration
  TestVideoHomeMovieMissingFileRestoration
  TestVideoMovieHashChangeDetection
  TestVideoHomeMovieHashChangeDetection

Each verifies:
  - /videos/{id}/raw returns 200 when items are available
  - /videos/{id}/raw returns 404 when items are marked missing
  - A path-restore scan un-marks items without re-processing (0 sub-jobs)
  - Hash corruption triggers per-item re-processing without duplicate rows
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

_VIDEO_PATH = "video-collection"


def _raw_url(item_id: int) -> str:
    return f"{BASE_URL}/videos/{item_id}/raw"


def _make_missing_file_test(collection_type: str) -> type:
    """
    Return a pytest-collectable test class that runs the missing-file
    restoration scenario for the given video collection type.
    """
    safe = collection_type.replace(":", "_").replace("-", "_")

    class _Test:
        collection_id: int = 0
        baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
        item_id: int = 0

        def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
            col_id, scan_job_id = create_collection(
                admin_token,
                f"Video Missing Test ({collection_type})",
                collection_type,
                _VIDEO_PATH,
            )
            _Test.collection_id = col_id

            wait_for_job(admin_token, scan_job_id, timeout=60)

            visible = count_non_missing_items(db_dsn, col_id)
            assert visible > 0, f"expected items after initial scan, got {visible}"

            grant_collection_access(admin_token, admin_user_id, col_id)

            item_id = get_first_item_id(db_dsn, col_id)
            assert item_id is not None, "expected at least one media item after scan"
            _Test.item_id = item_id

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 200, (
                f"expected 200 from raw video endpoint, got {r.status_code}"
            )

            _Test.baseline = count_rows(db_dsn, col_id)
            rc = _Test.baseline
            assert rc.items > 0, "expected media items after initial scan"
            assert rc.video_meta > 0, "expected video metadata after initial scan"

        def test_02_simulate_unreachable_path(self, admin_token, db_dsn):
            if not _Test.collection_id:
                pytest.fail("collection not created — test_01 must have failed")

            col_id = _Test.collection_id
            item_id = _Test.item_id

            set_collection_path(db_dsn, col_id, "does_not_exist")

            job_id = trigger_collection_scan(admin_token, col_id)
            wait_for_job(admin_token, job_id, timeout=60)

            visible = count_non_missing_items(db_dsn, col_id)
            assert visible == 0, (
                f"expected 0 visible items after path made unreachable, got {visible}"
            )

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 404, (
                f"expected 404 from raw video endpoint for missing item, got {r.status_code}"
            )

            job = get_scan_job(admin_token, job_id)
            assert job["subjobs_enqueued"] == 0, (
                f"expected 0 sub-jobs when only marking items missing, "
                f"got {job['subjobs_enqueued']}"
            )

        def test_03_restore_path_no_reprocessing(self, admin_token, db_dsn):
            if not _Test.collection_id:
                pytest.fail("collection not created — test_01 must have failed")

            col_id = _Test.collection_id
            item_id = _Test.item_id

            set_collection_path(db_dsn, col_id, _VIDEO_PATH)

            job_id = trigger_collection_scan(admin_token, col_id)
            wait_for_job(admin_token, job_id, timeout=60)

            visible = count_non_missing_items(db_dsn, col_id)
            assert visible == _Test.baseline.items, (
                f"expected {_Test.baseline.items} visible items after restore, "
                f"got {visible}"
            )

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 200, (
                f"expected 200 from raw video endpoint after restore, got {r.status_code}"
            )

            job = get_scan_job(admin_token, job_id)
            assert job["subjobs_enqueued"] == 0, (
                f"expected 0 sub-jobs on restore (hash unchanged, metadata present), "
                f"got {job['subjobs_enqueued']}"
            )

            current = count_rows(db_dsn, col_id)
            assert current == _Test.baseline, (
                f"row counts changed after restore: "
                f"baseline={_Test.baseline}, current={current}"
            )

    _Test.__name__ = f"TestVideo_{safe}_MissingFileRestoration"
    _Test.__qualname__ = _Test.__name__
    return _Test


def _make_hash_change_test(collection_type: str) -> type:
    """
    Return a pytest-collectable test class that runs the hash-change detection
    scenario for the given video collection type.
    """
    safe = collection_type.replace(":", "_").replace("-", "_")

    class _Test:
        collection_id: int = 0
        baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
        item_id: int = 0

        def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
            col_id, scan_job_id = create_collection(
                admin_token,
                f"Video Hash Test ({collection_type})",
                collection_type,
                _VIDEO_PATH,
            )
            _Test.collection_id = col_id

            wait_for_job(admin_token, scan_job_id, timeout=60)

            visible = count_non_missing_items(db_dsn, col_id)
            assert visible > 0, f"expected items after initial scan, got {visible}"

            grant_collection_access(admin_token, admin_user_id, col_id)

            item_id = get_first_item_id(db_dsn, col_id)
            assert item_id is not None, "expected at least one media item after scan"
            _Test.item_id = item_id

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 200, (
                f"expected 200 from raw video endpoint, got {r.status_code}"
            )

            _Test.baseline = count_rows(db_dsn, col_id)
            rc = _Test.baseline
            assert rc.items > 0, "expected media items after initial scan"
            assert rc.video_meta > 0, "expected video metadata after initial scan"

        def test_02_rescan_is_idempotent(self, admin_token, db_dsn):
            if not _Test.collection_id:
                pytest.fail("collection not created — test_01 must have failed")

            col_id = _Test.collection_id
            item_id = _Test.item_id

            job_id = trigger_collection_scan(admin_token, col_id)
            wait_for_job(admin_token, job_id, timeout=60)

            job = get_scan_job(admin_token, job_id)
            assert job["subjobs_enqueued"] == 0, (
                f"expected 0 sub-jobs on idempotent rescan, got {job['subjobs_enqueued']}"
            )

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 200, (
                f"expected 200 from raw video endpoint on idempotent rescan, "
                f"got {r.status_code}"
            )

            current = count_rows(db_dsn, col_id)
            assert current == _Test.baseline, (
                f"row counts changed on idempotent rescan: "
                f"baseline={_Test.baseline}, current={current}"
            )

        def test_03_corrupt_hashes_triggers_reprocessing(self, admin_token, db_dsn):
            if not _Test.collection_id:
                pytest.fail("collection not created — test_01 must have failed")

            col_id = _Test.collection_id
            item_id = _Test.item_id
            baseline_items = _Test.baseline.items

            corrupt_hashes(db_dsn, col_id)

            job_id = trigger_collection_scan(admin_token, col_id)
            wait_for_job(admin_token, job_id, timeout=60)

            job = get_scan_job(admin_token, job_id)
            assert job["subjobs_enqueued"] == baseline_items, (
                f"expected {baseline_items} sub-jobs (one per video file), "
                f"got {job['subjobs_enqueued']}"
            )

            assert count_stale_hashes(db_dsn, col_id) == 0, (
                "some items still have the corrupted hash after rescan"
            )

            r = httpx.head(_raw_url(item_id), headers=bearer(admin_token))
            assert r.status_code == 200, (
                f"expected 200 from raw video endpoint after reprocessing, "
                f"got {r.status_code}"
            )

            current = count_rows(db_dsn, col_id)
            assert current == _Test.baseline, (
                f"row counts changed after reprocessing: "
                f"baseline={_Test.baseline}, current={current}"
            )

    _Test.__name__ = f"TestVideo_{safe}_HashChangeDetection"
    _Test.__qualname__ = _Test.__name__
    return _Test


# ── Concrete test classes (discovered by pytest via their Test* names) ─────────

TestVideoMovieMissingFileRestoration = _make_missing_file_test("video:movie")
TestVideoHomeMovieMissingFileRestoration = _make_missing_file_test("video:home_movie")
TestVideoMovieHashChangeDetection = _make_hash_change_test("video:movie")
TestVideoHomeMovieHashChangeDetection = _make_hash_change_test("video:home_movie")
