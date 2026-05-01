"""
Library-wide search endpoint integration tests.

Each endpoint is exercised against a freshly scanned collection. We assert
both inclusion (expected hits returned) and exclusion (clearly non-matching
items absent), and that scoping respects the user's collection_access — the
admin user has access to test collections; a fresh user without grants must
see empty results.
"""

import asyncio
import uuid
import httpx
import pytest

from tests.conftest import BASE_URL, create_user_local, login_local
from helpers.auth import bearer
from helpers.poll import wait_for_job
from pydantic import BaseModel


# ── Helpers ───────────────────────────────────────────────────────────────────

class CreateCollectionResponse(BaseModel):
    id: int
    scan_job_id: int


def create_collection(token: str, name: str, relative_path: str, col_type: str) -> CreateCollectionResponse:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(token),
        json={"name": name, "type": col_type, "relative_path": relative_path},
    )
    r.raise_for_status()
    return CreateCollectionResponse(**r.json())


def grant_access(token: str, user_id: int, collection_id: int) -> None:
    r = httpx.patch(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access",
        headers=bearer(token),
        json={"collection_ids": [collection_id]},
    )
    r.raise_for_status()


def search(token: str, path: str, q: str, **params) -> dict:
    r = httpx.get(
        f"{BASE_URL}/api/v1/search/{path}",
        headers=bearer(token),
        params={"q": q, **params},
    )
    r.raise_for_status()
    return r.json()


# ── Fixture: scan all the test collections used here ──────────────────────────

class TestSearch:
    photo_id: int = 0
    music_id: int = 0
    movie_id: int = 0

    def test_01_setup(self, admin_token, admin_user_id):
        photos = create_collection(admin_token, "Search Photos", "photo-album-1", "image:photo")
        music = create_collection(admin_token, "Search Music", "music-collection", "audio:music")
        movies = create_collection(admin_token, "Search Movies", "video-collection", "video:movie")
        TestSearch.photo_id = photos.id
        TestSearch.music_id = music.id
        TestSearch.movie_id = movies.id

        for cid in (photos.id, music.id, movies.id):
            grant_access(admin_token, admin_user_id, cid)

        wait_for_job(admin_token, photos.scan_job_id, timeout=180)
        wait_for_job(admin_token, music.scan_job_id, timeout=180)
        wait_for_job(admin_token, movies.scan_job_id, timeout=180)

    def test_02_missing_q_returns_400(self, admin_token):
        r = httpx.get(f"{BASE_URL}/api/v1/search/videos", headers=bearer(admin_token))
        assert r.status_code == 400

    def test_03_search_videos(self, admin_token):
        if not TestSearch.movie_id:
            pytest.fail("setup failed")
        data = search(admin_token, "videos", "storm")
        movie_titles = {m["title"] for m in data["video:movie"]}
        assert "Storm Front" in movie_titles
        assert "Iron Sky" not in movie_titles
        assert "The Quiet Hour" not in movie_titles
        # No home_movie collection seeded → that bucket should be empty.
        assert data["video:home_movie"] == []

    def test_04_search_photos(self, admin_token):
        if not TestSearch.photo_id:
            pytest.fail("setup failed")
        data = search(admin_token, "photos", "sunflower")
        titles = {item["title"] for item in data["items"]}
        assert any("sunflower" in t.lower() for t in titles), f"sunflower not in {titles}"
        assert not any("moon" in t.lower() for t in titles)
        assert not any("rollercoaster" in t.lower() for t in titles)

    def test_05_search_audio_artists(self, admin_token):
        if not TestSearch.music_id:
            pytest.fail("setup failed")
        data = search(admin_token, "audio/artists", "artist")
        names = {a["name"] for a in data["artists"]}
        assert "Artist One" in names
        # Only audio:music seeded — shows bucket is empty.
        assert data["shows"] == []

    def test_06_search_audio_albums(self, admin_token):
        if not TestSearch.music_id:
            pytest.fail("setup failed")
        alpha = search(admin_token, "audio/albums", "alpha")
        names = {a["name"] for a in alpha["albums"]}
        assert "Album Alpha" in names
        assert "Album Beta" not in names

    def test_07_search_audio_tracks(self, admin_token):
        if not TestSearch.music_id:
            pytest.fail("setup failed")
        data = search(admin_token, "audio/tracks", "overture")
        titles = {t["title"] for t in data["tracks"]}
        assert "Overture" in titles
        assert "Finale" not in titles
        assert "First Light" not in titles
        for t in data["tracks"]:
            if t["title"] == "Overture":
                assert t["artist_name"] == "Artist One"
                assert t["album_name"] == "Album Beta"

    def test_08_pagination(self, admin_token):
        first = search(admin_token, "audio/tracks", "light OR ground OR closing OR overture OR finale", limit=2, offset=0)
        second = search(admin_token, "audio/tracks", "light OR ground OR closing OR overture OR finale", limit=2, offset=2)
        first_ids = {t["id"] for t in first["tracks"]}
        second_ids = {t["id"] for t in second["tracks"]}
        assert first_ids.isdisjoint(second_ids), f"pagination overlap: {first_ids & second_ids}"

    def test_09_access_scoping(self, admin_token):
        # A user with no grants must see nothing for any endpoint.
        username = f"search_scope_{uuid.uuid4().hex[:8]}"
        create_user_local(admin_token, username, "pw12345")
        login = login_local(username, "pw12345", device_uuid=str(uuid.uuid4()))
        token = login.access_token

        assert search(token, "videos", "storm")["video:movie"] == []
        assert search(token, "photos", "sunflower")["items"] == []
        artists = search(token, "audio/artists", "artist")
        assert artists["artists"] == [] and artists["shows"] == []
        assert search(token, "audio/albums", "alpha")["albums"] == []
        assert search(token, "audio/tracks", "overture")["tracks"] == []

    def test_10_parallel_audio_endpoints(self, admin_token):
        async def runs():
            async with httpx.AsyncClient(headers=bearer(admin_token)) as c:
                results = await asyncio.gather(
                    c.get(f"{BASE_URL}/api/v1/search/audio/artists", params={"q": "artist"}),
                    c.get(f"{BASE_URL}/api/v1/search/audio/albums", params={"q": "alpha"}),
                    c.get(f"{BASE_URL}/api/v1/search/audio/tracks", params={"q": "overture"}),
                )
            return results

        results = asyncio.run(runs())
        for r in results:
            assert r.status_code == 200
