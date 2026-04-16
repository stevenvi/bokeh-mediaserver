import uuid
import time
import httpx
from helpers.auth import bearer, decode_jwt
from tests.conftest import login_local
import pytest


def assert_error(response, expected_status_code):
    assert response.status_code == expected_status_code
    body = response.json()
    assert "error" in body
    assert "message" in body

# ── invalid credentials ───────────────────────────────────────────────────────

def test_login_wrong_password(base_url):
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        login_local("admin", "wrong")
    assert exc_info.value.response.status_code == 401

def test_login_unknown_user(base_url):
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        login_local("nobody", "password")
    assert exc_info.value.response.status_code == 401


# ── valid credentials ─────────────────────────────────────────────────────────

def test_login_admin_success():
    tokens = login_local("admin", "admin")

    # Verify access token structure and claims
    claims = decode_jwt(tokens.access_token)
    assert "sub" in claims                   # user ID as string
    assert "iat" in claims
    assert "exp" in claims
    assert claims["adm"] is True             # admin flag
    assert isinstance(claims["did"], int)    # device ID

    # Token lifetime is consistent with server's reported expiration
    assert claims["exp"] - claims["iat"] == tokens.access_token_expires_in

    # Token was issued recently and is not yet expired
    now = int(time.time())
    assert claims["iat"] <= now
    assert claims["exp"] > now
