import time
from tests.conftest import BASE_URL
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


def wait_for_job(token: str, job_id: int, *, timeout: float = 60.0) -> dict:
    """
    Poll until a job reaches a terminal state (done or failed). Returns the job dict.
    Raises RuntimeError if the job fails, TimeoutError if it does not finish in time.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = httpx.get(f"{BASE_URL}/api/v1/admin/jobs/{job_id}", headers=bearer(token))
        if r.status_code == 200:
            job = r.json()
            if job["status"] == "failed":
                raise RuntimeError(f"Job {job_id} failed: {job.get('error_message')}")
            if job["status"] == "done":
                return job
        time.sleep(0.5)
    raise TimeoutError(f"Job {job_id} did not complete within {timeout}s.")
