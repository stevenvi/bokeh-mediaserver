import base64
import json


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def decode_jwt(token: str) -> dict:
    """Decode JWT payload without verifying signature."""
    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    return json.loads(base64.urlsafe_b64decode(payload_b64))
