"""
Collection-scan tests for audio:music collections.

TestAudioMissingFileRestoration: items marked missing when the collection path
becomes unreachable are restored (without re-processing) when the path is corrected.

TestAudioHashChangeDetection: corrupting file_hash triggers re-processing on the
next scan and updates hashes without creating duplicate rows.

Both tests also verify that the audio streaming endpoint (/audio/{id}/stream)
returns data when items are available and 404 when items are marked missing.
"""

import httpx
import pytest

from tests.jobs.collection_scan.common import (
    BASE_URL, RowCounts, bearer, wait_for_job,
    create_collection, count_rows, get_first_item_id,
    corrupt_hashes, count_stale_hashes, set_collection_path,
    trigger_collection_scan, get_scan_job,
    grant_collection_access, list_artists,
)

_MUSIC_PATH = "music-collection"
_COLLECTION_TYPE = "audio:music"


# ── TestAudioMissingFileRestoration ───────────────────────────────────────────

class TestAudioMissingFileRestoration:
    """
    Verifies that when an audio:music collection path becomes unreachable all
    items are marked missing (streaming fails with 404), and when the path is
    restored all items are un-marked without triggering re-processing (streaming
    works again, 0 sub-jobs).
    """

    collection_id: int = 0
    baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
    item_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        col_id, scan_job_id = create_collection(
            admin_token, "Audio Missing Test", _COLLECTION_TYPE, _MUSIC_PATH
        )
        TestAudioMissingFileRestoration.collection_id = col_id

        wait_for_job(admin_token, scan_job_id, timeout=60)

        artists = list_artists(admin_token, col_id)
        assert len(artists) == 1, f"expected 1 artist after initial scan, got {len(artists)}"

        grant_collection_access(admin_token, admin_user_id, col_id)

        item_id = get_first_item_id(db_dsn, col_id)
        assert item_id is not None, "expected at least one media item after scan"
        TestAudioMissingFileRestoration.item_id = item_id

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200, f"expected 200 from audio stream, got {r.status_code}"

        TestAudioMissingFileRestoration.baseline = count_rows(db_dsn, col_id)
        rc = TestAudioMissingFileRestoration.baseline
        assert rc.items > 0, "expected media items after initial scan"
        assert rc.audio_meta > 0, "expected audio metadata after initial scan"

    def test_02_simulate_unreachable_path(self, admin_token, db_dsn):
        if not TestAudioMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestAudioMissingFileRestoration.collection_id
        item_id = TestAudioMissingFileRestoration.item_id

        set_collection_path(db_dsn, col_id, "does_not_exist")

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        artists = list_artists(admin_token, col_id)
        assert len(artists) == 0, (
            f"expected 0 artists after path made unreachable, got {len(artists)}"
        )

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 404, (
            f"expected 404 from audio stream for missing item, got {r.status_code}"
        )

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs when only marking items missing, got {job['subjobs_enqueued']}"
        )

    def test_03_restore_path_no_reprocessing(self, admin_token, db_dsn):
        if not TestAudioMissingFileRestoration.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestAudioMissingFileRestoration.collection_id
        item_id = TestAudioMissingFileRestoration.item_id

        set_collection_path(db_dsn, col_id, _MUSIC_PATH)

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        artists = list_artists(admin_token, col_id)
        assert len(artists) == 1, (
            f"expected 1 artist after path restored, got {len(artists)}"
        )

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from audio stream after restore, got {r.status_code}"
        )

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on restore (hash unchanged, metadata present), "
            f"got {job['subjobs_enqueued']}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestAudioMissingFileRestoration.baseline, (
            f"row counts changed after restore: "
            f"baseline={TestAudioMissingFileRestoration.baseline}, current={current}"
        )


# ── TestAudioHashChangeDetection ──────────────────────────────────────────────

class TestAudioHashChangeDetection:
    """
    Verifies that when stored file_hashes differ from actual content, the scan
    detects the change, updates hashes, and re-queues sub-jobs without creating
    duplicate rows. Streaming is also verified at each stage.
    """

    collection_id: int = 0
    baseline: RowCounts = RowCounts(0, 0, 0, 0, 0)
    item_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        col_id, scan_job_id = create_collection(
            admin_token, "Audio Hash Test", _COLLECTION_TYPE, _MUSIC_PATH
        )
        TestAudioHashChangeDetection.collection_id = col_id

        wait_for_job(admin_token, scan_job_id, timeout=60)

        artists = list_artists(admin_token, col_id)
        assert len(artists) == 1, f"expected 1 artist after initial scan, got {len(artists)}"

        grant_collection_access(admin_token, admin_user_id, col_id)

        item_id = get_first_item_id(db_dsn, col_id)
        assert item_id is not None, "expected at least one media item after scan"
        TestAudioHashChangeDetection.item_id = item_id

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200, f"expected 200 from audio stream, got {r.status_code}"

        TestAudioHashChangeDetection.baseline = count_rows(db_dsn, col_id)
        rc = TestAudioHashChangeDetection.baseline
        assert rc.items > 0, "expected media items after initial scan"
        assert rc.audio_meta > 0, "expected audio metadata after initial scan"

    def test_02_rescan_is_idempotent(self, admin_token, db_dsn):
        if not TestAudioHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestAudioHashChangeDetection.collection_id
        item_id = TestAudioHashChangeDetection.item_id

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == 0, (
            f"expected 0 sub-jobs on idempotent rescan, got {job['subjobs_enqueued']}"
        )

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from audio stream on idempotent rescan, got {r.status_code}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestAudioHashChangeDetection.baseline, (
            f"row counts changed on idempotent rescan: "
            f"baseline={TestAudioHashChangeDetection.baseline}, current={current}"
        )

    def test_03_corrupt_hashes_triggers_reprocessing(self, admin_token, db_dsn):
        if not TestAudioHashChangeDetection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        col_id = TestAudioHashChangeDetection.collection_id
        item_id = TestAudioHashChangeDetection.item_id
        baseline_items = TestAudioHashChangeDetection.baseline.items

        corrupt_hashes(db_dsn, col_id)

        job_id = trigger_collection_scan(admin_token, col_id)
        wait_for_job(admin_token, job_id, timeout=60)

        job = get_scan_job(admin_token, job_id)
        assert job["subjobs_enqueued"] == baseline_items, (
            f"expected {baseline_items} sub-jobs (one per audio file), "
            f"got {job['subjobs_enqueued']}"
        )

        assert count_stale_hashes(db_dsn, col_id) == 0, (
            "some items still have the corrupted hash after rescan"
        )

        artists = list_artists(admin_token, col_id)
        assert len(artists) == 1, (
            f"expected 1 artist after reprocessing, got {len(artists)}"
        )

        r = httpx.head(f"{BASE_URL}/audio/{item_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200, (
            f"expected 200 from audio stream after reprocessing, got {r.status_code}"
        )

        current = count_rows(db_dsn, col_id)
        assert current == TestAudioHashChangeDetection.baseline, (
            f"row counts changed after reprocessing: "
            f"baseline={TestAudioHashChangeDetection.baseline}, current={current}"
        )
