"""
Device management integration tests.

All device operations are performed on a dedicated non-admin user created at
the start of the class. The admin user is only used for user creation and
revoke-all at the end.

Tests run in definition order within the class, sharing state via class-level
variables. pytest.fail() is used as a guard where missing prior state would
cause a confusing error.
"""

import uuid
import httpx
import pytest
from helpers.auth import bearer, decode_jwt
from pydantic import AwareDatetime, BaseModel
from conftest import BASE_URL, LoginResponse, create_user_local, login_local, refresh


# ── Convenience functions ────────────────────────────────────────────────────

class DeviceAccess(BaseModel):
    ip: str
    agent: str
    last_seen: AwareDatetime

class UserDevice(BaseModel):
    id: int
    device_name: str
    last_seen_at: AwareDatetime
    created_at: AwareDatetime | None
    banned_at: AwareDatetime | None = None
    access_history: list[DeviceAccess]
    

def get_devices(token: str) -> list[UserDevice]:
    r = httpx.get(f"{BASE_URL}/api/v1/auth/devices", headers=bearer(token))
    assert r.status_code == 200, f"Failed to get devices: {r.status_code} {r.text}"
    return [UserDevice(**d) for d in r.json()]

def expect_no_devices(token: str) -> None:
    devices = get_devices(token)
    assert len(devices) == 0, f"Expected no devices, but got: {devices}"

def delete_device(token: str, device_id: int) -> None:
    r = httpx.delete(f"{BASE_URL}/api/v1/auth/devices/{device_id}", headers=bearer(token))
    r.raise_for_status()

def delete_all_devices(admin_token: str, user_id: int) -> None:
    r = httpx.delete(f"{BASE_URL}/api/v1/admin/users/{user_id}/devices", headers=bearer(admin_token))
    r.raise_for_status()

def ban_device(token: str, device_id: int) -> None:
    r = httpx.post(f"{BASE_URL}/api/v1/auth/devices/{device_id}/ban", headers=bearer(token))
    r.raise_for_status()

def unban_device(token: str, device_id: int) -> None:
    r = httpx.delete(f"{BASE_URL}/api/v1/auth/devices/{device_id}/ban", headers=bearer(token))
    r.raise_for_status()

def is_token_valid(token) -> bool:
    """GET /api/v1/collections as a generic authenticated-endpoint probe."""
    r = httpx.get(f"{BASE_URL}/api/v1/collections", headers=bearer(token))
    if r.status_code >= 200 and r.status_code < 400:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Device happy path
# ─────────────────────────────────────────────────────────────────────────────

class TestDeviceHappyPath:
    # non-admin user credentials
    username = "devicetestuser"
    password = "devpass123"

    # device 1
    d1_uuid: str = None
    d1_auth: LoginResponse = None
    d1: UserDevice = None

    # device 2
    d2_uuid: str = None
    d2_auth: LoginResponse = None
    d2: UserDevice = None

    # device 3
    d3_uuid: str = None
    d3_auth: LoginResponse = None
    d3: UserDevice = None

    # non-admin user's ID
    user_id: int = None

    def test_00_create_non_admin_user(self, admin_token):
        TestDeviceHappyPath.user_id = create_user_local(admin_token, TestDeviceHappyPath.username, TestDeviceHappyPath.password)


    def test_01_login_device1_no_name(self):
        TestDeviceHappyPath.d1_uuid = str(uuid.uuid4())
        TestDeviceHappyPath.d1_auth = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d1_uuid)
        devices = get_devices(TestDeviceHappyPath.d1_auth.access_token)
        assert len(devices) == 1
        TestDeviceHappyPath.d1 = devices[0]
        assert TestDeviceHappyPath.d1.id == TestDeviceHappyPath.d1_auth.device_id
        assert TestDeviceHappyPath.d1.device_name == ""

    def test_02_login_device1_with_name(self):
        new_auth = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d1_uuid, "My Phone")
        # Token and refresh are rotated; device_id is the same
        assert new_auth.device_id == TestDeviceHappyPath.d1_auth.device_id
        TestDeviceHappyPath.d1_auth = new_auth
        
        devices = get_devices(TestDeviceHappyPath.d1_auth.access_token)
        assert len(devices) == 1
        TestDeviceHappyPath.d1 = devices[0]
        assert TestDeviceHappyPath.d1.id == TestDeviceHappyPath.d1_auth.device_id
        assert TestDeviceHappyPath.d1.device_name == "My Phone"

    def test_03_login_second_device(self):
        TestDeviceHappyPath.d2_uuid = str(uuid.uuid4())
        TestDeviceHappyPath.d2_auth = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d2_uuid, "My Tablet")
        assert TestDeviceHappyPath.d2_auth.device_id != TestDeviceHappyPath.d1_auth.device_id

        # Verify device 2 appears in device list with correct names
        devices = get_devices(TestDeviceHappyPath.d2_auth.access_token)
        assert len(devices) == 2
        for d in devices:
            if d.id == TestDeviceHappyPath.d1_auth.device_id:
                assert d.device_name == "My Phone"
                TestDeviceHappyPath.d1 = d
            elif d.id == TestDeviceHappyPath.d2_auth.device_id:
                assert d.device_name == "My Tablet"
                TestDeviceHappyPath.d2 = d
            else:
                pytest.fail(f"Unknown device ID {d.id} in list")

    def test_03_delete_device1_from_device2_token(self):
        delete_device(TestDeviceHappyPath.d2_auth.access_token, TestDeviceHappyPath.d1.id)

        # Access token from deleted device should be rejected (DeviceGuard).
        assert not is_token_valid(TestDeviceHappyPath.d1_auth.access_token)
        assert is_token_valid(TestDeviceHappyPath.d2_auth.access_token)

        # A device cannot delete itself.
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
             delete_device(TestDeviceHappyPath.d2_auth.access_token, TestDeviceHappyPath.d2.id)
        assert exc_info.value.response.status_code == 403

        # Refresh token for deleted device 1 should be invalid.
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            refresh(TestDeviceHappyPath.d1_auth.refresh_token, TestDeviceHappyPath.d1_uuid)
        assert exc_info.value.response.status_code in (401, 403)

        # Device 2 should refresh just fine though, and login fine as well
        TestDeviceHappyPath.d2_auth = refresh(TestDeviceHappyPath.d2_auth.refresh_token, TestDeviceHappyPath.d2_uuid)
        TestDeviceHappyPath.d2_auth = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d2_uuid, "My Tablet")

        # And Device 1 can always re-login as well, getting a new device ID and auth tokens
        new_d1 = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d1_uuid, "My Phone")
        assert new_d1.device_id != TestDeviceHappyPath.d1_auth.device_id
        TestDeviceHappyPath.d1_auth = new_d1

    def test_04_login_third_device_to_be_banned(self):
        TestDeviceHappyPath.d3_uuid = str(uuid.uuid4())
        TestDeviceHappyPath.d3_auth = login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d3_uuid, "My TV")
        ban_device(TestDeviceHappyPath.d2_auth.access_token, TestDeviceHappyPath.d3_auth.device_id)

        # Expect device 3 to show as banned in device list
        devices = get_devices(TestDeviceHappyPath.d2_auth.access_token)
        device_map = {d.id: d for d in devices}
        assert TestDeviceHappyPath.d3_auth.device_id in device_map
        assert device_map[TestDeviceHappyPath.d3_auth.device_id].banned_at is not None

        # And if they're banned, their token should be rejected immediately
        assert not is_token_valid(TestDeviceHappyPath.d3_auth.access_token)

        # And a banned user cannot refresh their token
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            refresh(TestDeviceHappyPath.d3_auth.refresh_token, TestDeviceHappyPath.d3_uuid)
        assert exc_info.value.response.status_code in (401, 403)

        # And a banned device cannot be logged in again
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            login_local(TestDeviceHappyPath.username, TestDeviceHappyPath.password, TestDeviceHappyPath.d3_uuid)
        assert exc_info.value.response.status_code == 403

    def test_05_admin_revokes_all_devices(TestDeviceHappyPath, base_url, admin_token):
        delete_all_devices(admin_token, TestDeviceHappyPath.user_id)

        # After admin revokes all devices, device 2's token should be rejected.
        assert not is_token_valid(TestDeviceHappyPath.d2_auth.access_token)
