import os
import shutil
from typing import Any
import uuid
import psycopg
import pytest
import httpx
from helpers.auth import bearer
from pydantic import BaseModel

BASE_URL = os.environ.get("BASE_URL")
DATA_PATH = os.environ.get("DATA_PATH")
DB_DSN = os.environ.get("DB_DSN")

if not BASE_URL:
    raise ValueError("BASE_URL environment variable is not set")
if not DATA_PATH:
    raise ValueError("DATA_PATH environment variable is not set")
if not DB_DSN:   
    raise ValueError("DB_DSN environment variable is not set")


# ── Session: verify Docker environment and server reachability ────────────────

@pytest.fixture(scope="session", autouse=True)
def server_ready():
    if not os.path.exists("/.dockerenv"):
        pytest.exit(
            "Refusing to run outside a Docker container — "
            "it would wipe DATA_PATH on the host filesystem.",
            returncode=1,
        )
    r = httpx.get(f"{BASE_URL}/api/v1/system/health")
    assert r.status_code == 200, f"Server not healthy: {r.status_code}"

# ── Module: wipe state before each test file ──────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def reset_state(server_ready):

    # Clear derived filesystem artifacts (safe: this is a container volume, but I still have reservations about this...)
    shutil.rmtree(DATA_PATH, ignore_errors=True)
    os.makedirs(DATA_PATH, exist_ok=True)

    # Truncate app tables that tests create. Skip:
    #   - server_config: singleton read by server at startup
    #   - users: admin is seeded by migration on first startup; preserve it
    with psycopg.connect(DB_DSN, autocommit=True) as conn:
        conn.execute("""
            TRUNCATE jobs, photo_metadata, media_items,
                     collection_access, collections, devices
            CASCADE
        """)

# ── Shared fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def base_url():
    return BASE_URL

@pytest.fixture(scope="session")
def data_path():
    return DATA_PATH

@pytest.fixture(scope="session")
def db_dsn():
    return DB_DSN

# ── Per-module admin token ─────────────────────────────────────────────────────
# Module-scoped so it re-logs in after reset_state truncates devices.

@pytest.fixture(scope="module")
def admin_token(base_url, reset_state):
    r = httpx.post(f"{base_url}/api/v1/auth/login", json={
        "provider": "local",
        "credentials": {"username": "admin", "password": "admin"},
        "device_uuid": str(uuid.uuid4()),
    })
    assert r.status_code == 200, f"Admin login failed: {r.text}"
    return r.json()["access_token"]

@pytest.fixture(scope="module")
def admin_user_id(base_url, admin_token):
    r = httpx.get(f"{base_url}/api/v1/auth/me", headers=bearer(admin_token))
    assert r.status_code == 200
    return r.json()["id"]


# ── Helper Functions ────────────────────────────────────────────────────────────

class LoginResponse(BaseModel):
    access_token: str
    access_token_expires_in: int
    refresh_token: str
    refresh_token_expires_in: int
    device_id: int

def login_local(username: str, password: str, device_uuid: str = 'test_device_uuid', device_name: str | None = None) -> LoginResponse:
    req: dict[str, Any] = {
        "provider": "local",
        "credentials": {
            "username": username, 
            "password": password
        }, 
        "device_uuid": device_uuid,
    }
    if device_name:
        req["device_name"] = device_name

    r = httpx.post(
        f"{BASE_URL}/api/v1/auth/login",
        json=req,
    )
    r.raise_for_status()
    return LoginResponse(**r.json())


def refresh(refresh_token: str, device_uuid: str) -> LoginResponse:
    r = httpx.post(
        f"{BASE_URL}/api/v1/auth/refresh",
        json={"refresh_token": refresh_token, "device_uuid": device_uuid},
    )
    r.raise_for_status()
    return LoginResponse(**r.json())


def create_user_local(admin_token: str, name: str, password: str, is_admin: bool = False) -> int:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/users",
        headers=bearer(admin_token),
        json={"name": name, "is_admin": is_admin, "auth_provider": "local", "credentials": {"password": password}},
    )
    r.raise_for_status()
    return r.json()["id"]

