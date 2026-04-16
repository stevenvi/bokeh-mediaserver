"""
Scan idempotency tests: verify that repeated scans do not create duplicate
media_items, collections, or metadata rows.
"""

import pytest

from tests.jobs.collection_scan.common import (
    RowCounts, wait_for_job,
    create_collection, count_rows,
    trigger_collection_scan, get_scan_job,
)


class TestScanIdempotency:
    """
    Verifies that running repeated collection scans on an already-scanned
    collection does not produce duplicate media_items, collections, or metadata.
    """

    collection_id: int = 0
    baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)

    def test_01_create_and_initial_scan(self, admin_token, db_dsn):
        col_id, scan_job_id = create_collection(
            admin_token, "Idempotency Music", "audio:music", "music-collection"
        )
        TestScanIdempotency.collection_id = col_id
        wait_for_job(admin_token, scan_job_id, timeout=60)

        TestScanIdempotency.baseline = count_rows(db_dsn, col_id)
        rc = TestScanIdempotency.baseline
        assert rc.items > 0, "expected media items after initial scan"
        assert rc.cols > 0, "expected at least one collection"
        assert rc.audio_meta > 0, "expected audio metadata after initial scan"

    def test_02_rescan_produces_no_duplicates(self, admin_token, db_dsn):
        if not TestScanIdempotency.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestScanIdempotency.collection_id

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on idempotent rescan, got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestScanIdempotency.baseline, (
            f"counts changed on rescan — possible duplicates: "
            f"baseline={TestScanIdempotency.baseline}, current={current}"
        )

    def test_03_second_rescan_still_idempotent(self, admin_token, db_dsn):
        if not TestScanIdempotency.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestScanIdempotency.collection_id

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on second idempotent rescan, got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestScanIdempotency.baseline, (
            f"counts changed on second rescan: "
            f"baseline={TestScanIdempotency.baseline}, current={current}"
        )
