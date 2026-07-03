#!/usr/bin/env python3
"""MatrixRTC -> LiveKit token chain + room-alias derivation helpers.

Shared by agent_matrix.py and the validation scripts. Pure stdlib + httpx.

The LiveKit room name (the JWT `video.room` claim) is derived by lk-jwt-service
(element-hq/lk-jwt-service main.go) as:

    slot_id    = "m.call#ROOM"                      # hard-coded for m.call
    alias_raw  = json.dumps([matrix_room_id, slot_id], no-spaces)
    lk_room    = base64_nopad( sha256(alias_raw) )

The legacy /sfu/get and modern /get_token endpoints derive the SAME alias for the
same Matrix room as long as slot_id == "m.call#ROOM" -- which is the only slot the
m.call application defines. So Element X (modern) and us (legacy) land in the same
LiveKit SFU room.
"""
from __future__ import annotations

import base64
import hashlib
import json

import httpx

CLIENT_API = "https://matrix-client.matrix.org"
WELL_KNOWN = "https://matrix.org/.well-known/matrix/client"
MCALL_SLOT_ID = "m.call#ROOM"


def derive_lk_room_alias(matrix_room_id: str, slot_id: str = MCALL_SLOT_ID) -> str:
    """Replicate the lk-jwt-service room-alias derivation used by the DEPLOYED
    matrix.org service (livekit-jwt.call.matrix.org).

    Empirically verified against a live JWT: the deployed version (lk-jwt-service
    commit fa22603 era) hashes the pipe-joined string, NOT the json-array that the
    current `main` branch uses:

        lk_room = base64_nopad( sha256(f"{room_id}|{slot_id}") )

    Both the legacy /sfu/get and the modern /get_token use this same scheme with
    slot_id = "m.call#ROOM", so a legacy-token client (us) and a modern-token
    client (Element X) derive the SAME LiveKit room for the same Matrix room.
    """
    alias_raw = f"{matrix_room_id}|{slot_id}"
    digest = hashlib.sha256(alias_raw.encode()).digest()
    return base64.b64encode(digest).decode().rstrip("=")


def jwt_payload(jwt: str) -> dict:
    """Decode (without verifying) a JWT's payload."""
    body = jwt.split(".")[1]
    body += "=" * (-len(body) % 4)
    return json.loads(base64.urlsafe_b64decode(body))


async def get_openid_token(http: httpx.AsyncClient, user_id: str, access_token: str) -> dict:
    """POST /openid/request_token -> {access_token, token_type, matrix_server_name, expires_in}."""
    url = f"{CLIENT_API}/_matrix/client/v3/user/{user_id}/openid/request_token"
    r = await http.post(url, headers={"Authorization": f"Bearer {access_token}"}, json={})
    r.raise_for_status()
    return r.json()


async def discover_livekit_service(http: httpx.AsyncClient) -> str:
    """Read the homeserver well-known for the MSC4143 livekit foci service URL."""
    r = await http.get(WELL_KNOWN)
    r.raise_for_status()
    data = r.json()
    foci = data.get("org.matrix.msc4143.rtc_foci", [])
    for f in foci:
        if f.get("type") == "livekit" and f.get("livekit_service_url"):
            return f["livekit_service_url"]
    if foci:
        return foci[0].get("livekit_service_url", "")
    raise RuntimeError(f"no rtc_foci in well-known: {data}")


async def sfu_get_legacy(
    http: httpx.AsyncClient,
    lk_service_url: str,
    matrix_room_id: str,
    openid_token: dict,
    device_id: str,
) -> dict:
    """Legacy POST /sfu/get -> {url, jwt}. The granted LiveKit room equals
    derive_lk_room_alias(matrix_room_id) (slot hard-coded to m.call#ROOM)."""
    r = await http.post(
        f"{lk_service_url.rstrip('/')}/sfu/get",
        json={"room": matrix_room_id, "openid_token": openid_token, "device_id": device_id},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


async def sfu_get_token(
    http: httpx.AsyncClient,
    lk_service_url: str,
    matrix_room_id: str,
    openid_token: dict,
    user_id: str,
    device_id: str,
    slot_id: str = MCALL_SLOT_ID,
) -> dict:
    """Modern MSC4195 POST /get_token -> {url, jwt}."""
    member_id = f"_{user_id}_{device_id}"
    r = await http.post(
        f"{lk_service_url.rstrip('/')}/get_token",
        json={
            "room_id": matrix_room_id,
            "slot_id": slot_id,
            "openid_token": openid_token,
            "member": {
                "id": member_id,
                "claimed_user_id": user_id,
                "claimed_device_id": device_id,
            },
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


async def full_token_chain(
    http: httpx.AsyncClient,
    user_id: str,
    access_token: str,
    matrix_room_id: str,
    device_id: str,
    prefer: str = "legacy",
) -> dict:
    """Run the whole chain. Returns dict with url, jwt, lk_room (from JWT),
    expected_lk_room (locally derived), openid, lk_service_url."""
    lk_service_url = await discover_livekit_service(http)
    openid = await get_openid_token(http, user_id, access_token)
    if prefer == "modern":
        resp = await sfu_get_token(http, lk_service_url, matrix_room_id, openid, user_id, device_id)
    else:
        resp = await sfu_get_legacy(http, lk_service_url, matrix_room_id, openid, device_id)
    payload = jwt_payload(resp["jwt"])
    lk_room = (payload.get("video") or {}).get("room") or payload.get("room")
    return {
        "lk_service_url": lk_service_url,
        "openid": openid,
        "url": resp.get("url"),
        "jwt": resp["jwt"],
        "lk_room": lk_room,
        "expected_lk_room": derive_lk_room_alias(matrix_room_id),
        "jwt_payload": payload,
    }
