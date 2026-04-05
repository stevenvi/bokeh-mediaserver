import time
from conftest import BASE_URL
import psycopg
import httpx
from helpers.auth import bearer


def poll_until(fn, *, timeout: float = 30.0, interval: float = 0.5):
    """Call fn() repeatedly until it returns a truthy value or timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    raise TimeoutError(f"Condition not met within {timeout}s")


def wait_for_job(base_url: str, token: str, job_id: int, *, timeout: float = 30.0):
    """
    Poll until a job reaches a terminal state (done or failed). Returns the job dict.
    Note: process_media jobs are deleted on completion — a 404 is treated as done.
    """
    def check():
        r = httpx.get(f"{base_url}/api/v1/admin/jobs/{job_id}", headers=bearer(token))
        if r.status_code == 404:
            return {"id": job_id, "status": "done"}  # deleted = completed
        if r.status_code != 200:
            return None
        job = r.json()
        if job.get("status") in ("done", "failed"):
            return job
        return None

    return poll_until(check, timeout=timeout)


def wait_for_scan_and_processing(
    token: str,
    scan_job_id: int,
    collection_id: int,
    dsn: str,
    *,
    timeout: float = 60.0,
):
    """
    Wait for a scan job (initial_scan, filesystem_scan, or metadata_scan) to
    complete, then wait for all process_media jobs spawned for items in that
    collection (and its sub-collections) to finish.

    process_media jobs are deleted on success, so absence from the jobs table
    is the completion signal.

    Raises TimeoutError with a helpful message if either phase times out.
    """
    deadline = time.monotonic() + timeout

    # Phase 1: wait for the scan job to reach done/failed
    while time.monotonic() < deadline:
        r = httpx.get(
            f"{BASE_URL}/api/v1/admin/jobs/{scan_job_id}", headers=bearer(token)
        )
        if r.status_code == 200:
            job = r.json()
            if job["status"] == "failed":
                raise RuntimeError(
                    f"Scan job {scan_job_id} failed: {job.get('error_message')}"
                )
            if job["status"] == "done":
                break
        time.sleep(0.5)
    else:
        raise TimeoutError(
            f"Scan job {scan_job_id} did not complete within {timeout}s. "
            "Check WORKER_COUNT."
        )

    # Phase 2: wait for all process_media jobs for this collection tree to be gone
    while time.monotonic() < deadline:
        with psycopg.connect(dsn) as conn:
            pending = conn.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id FROM collections WHERE id = %s
                    UNION ALL
                    SELECT c.id FROM collections c
                    JOIN tree t ON c.parent_collection_id = t.id
                )
                SELECT COUNT(*) FROM jobs j
                JOIN media_items mi ON mi.id = j.related_id
                WHERE j.type = 'process_media'
                  AND mi.collection_id IN (SELECT id FROM tree)
                """,
                [collection_id],
            ).fetchone()[0]
        if pending == 0:
            return
        time.sleep(0.5)

    raise TimeoutError(
        f"Processing for collection {collection_id} did not complete within {timeout}s. "
        "Try increasing PROCESSING_WORKER_COUNT or the timeout."
    )
