"""
Music and video collection integration tests.

Tests are organised as pytest classes. Within each class, methods run in
definition order and share state via class-level variables.
"""

import httpx
import pytest
from tests.conftest import BASE_URL
from helpers.auth import bearer
from helpers.poll import wait_for_job
from pydantic import BaseModel


# ── Shared models ─────────────────────────────────────────────────────────────

class CreateCollectionResponse(BaseModel):
    id: int
    scan_job_id: int

class ArtistSummary(BaseModel):
    id: int
    name: str
    sort_name: str

class AlbumSummary(BaseModel):
    album_id: int
    name: str
    year: int | None = None
    track_count: int
    total_duration: float

class TrackView(BaseModel):
    id: int
    title: str
    mime_type: str
    track_number: int | None = None
    disc_number: int | None = None
    duration_seconds: float | None = None
    artist_name: str | None = None

class VideoItemView(BaseModel):
    id: int
    title: str
    mime_type: str
    duration_seconds: int | None = None
    width: int | None = None
    height: int | None = None
    video_codec: str | None = None
    audio_codec: str | None = None
    bitrate_kbps: int | None = None
    bookmark_seconds: int | None = None
    manual_thumbnail: bool = False

class CollectionView(BaseModel):
    id: int
    name: str
    type: str


# ── Shared helpers ─────────────────────────────────────────────────────────────

def create_collection(token: str, name: str, relative_path: str, col_type: str) -> CreateCollectionResponse:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(token),
        json={"name": name, "type": col_type, "relative_path": relative_path},
    )
    r.raise_for_status()
    return CreateCollectionResponse(**r.json())


def grant_collection_access(token: str, user_id: int, collection_id: int) -> None:
    r = httpx.patch(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access",
        headers=bearer(token),
        json={"collection_ids": [collection_id]},
    )
    r.raise_for_status()


def list_artists(token: str, collection_id: int) -> list[ArtistSummary]:
    r = httpx.get(f"{BASE_URL}/api/v1/collections/{collection_id}/artists", headers=bearer(token))
    r.raise_for_status()
    return [ArtistSummary(**a) for a in r.json()["artists"]]


def list_artist_albums(token: str, collection_id: int, artist_id: int) -> list[AlbumSummary]:
    r = httpx.get(
        f"{BASE_URL}/api/v1/collections/{collection_id}/artists/{artist_id}/albums",
        headers=bearer(token),
    )
    r.raise_for_status()
    return [AlbumSummary(**a) for a in r.json()["albums"]]


def list_album_tracks(token: str, collection_id: int, album_id: int) -> tuple[list[TrackView], float, int]:
    r = httpx.get(
        f"{BASE_URL}/api/v1/collections/{collection_id}/albums/{album_id}/tracks",
        headers=bearer(token),
    )
    r.raise_for_status()
    data = r.json()
    return [TrackView(**t) for t in data["tracks"]], data["total_duration"], data["disc_count"]


def list_video_items(token: str, collection_id: int) -> list[VideoItemView]:
    r = httpx.get(f"{BASE_URL}/api/v1/collections/{collection_id}/videos", headers=bearer(token))
    r.raise_for_status()
    return [VideoItemView(**item) for item in r.json()["items"]]


def list_subcollections(token: str, collection_id: int) -> list[CollectionView]:
    r = httpx.get(
        f"{BASE_URL}/api/v1/collections/{collection_id}/collections",
        headers=bearer(token),
    )
    r.raise_for_status()
    return [CollectionView(**c) for c in r.json()]


# ── Music: audio:music ────────────────────────────────────────────────────────

class TestMusicCollection:
    """
    audio:music collection containing:
      music-collection/artist-one/album-alpha/  — 3 tracks ("First Light", "Middle Ground", "Closing Time")
      music-collection/artist-one/album-beta/   — 2 tracks ("Overture", "Finale")
    All tracks are by "Artist One".
    """

    collection_id: int = 0
    artist_id: int = 0
    album_alpha_id: int = 0
    album_beta_id: int = 0
    track_ids: list[int] = []

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        resp = create_collection(admin_token, "Test Music", "music-collection", "audio:music")
        TestMusicCollection.collection_id = resp.id
        grant_collection_access(admin_token, admin_user_id, resp.id)
        wait_for_job(admin_token, resp.scan_job_id, timeout=60)

    def test_02_one_artist(self, admin_token):
        if not TestMusicCollection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        artists = list_artists(admin_token, TestMusicCollection.collection_id)
        assert len(artists) == 1, f"expected 1 artist, got {[a.name for a in artists]}"
        assert artists[0].name == "Artist One"
        TestMusicCollection.artist_id = artists[0].id

    def test_03_two_albums(self, admin_token):
        if not TestMusicCollection.artist_id:
            pytest.fail("artist not found — test_02 must have failed")

        albums = list_artist_albums(admin_token, TestMusicCollection.collection_id, TestMusicCollection.artist_id)
        assert len(albums) == 2, f"expected 2 albums, got {[a.name for a in albums]}"

        names = {a.name for a in albums}
        assert names == {"Album Alpha", "Album Beta"}

        for album in albums:
            if album.name == "Album Alpha":
                assert album.track_count == 3, f"Album Alpha: expected 3 tracks, got {album.track_count}"
                TestMusicCollection.album_alpha_id = album.album_id
            elif album.name == "Album Beta":
                assert album.track_count == 2, f"Album Beta: expected 2 tracks, got {album.track_count}"
                TestMusicCollection.album_beta_id = album.album_id

    def test_04_album_alpha_tracks(self, admin_token):
        if not TestMusicCollection.album_alpha_id:
            pytest.fail("Album Alpha not found — test_03 must have failed")

        tracks, total_duration, disc_count = list_album_tracks(
            admin_token, TestMusicCollection.collection_id, TestMusicCollection.album_alpha_id
        )

        assert len(tracks) == 3
        assert disc_count == 1
        assert total_duration > 0

        titles = [t.title for t in tracks]
        assert "First Light" in titles
        assert "Middle Ground" in titles
        assert "Closing Time" in titles

        track_numbers = sorted(t.track_number for t in tracks if t.track_number is not None)
        assert track_numbers == [1, 2, 3]

        for t in tracks:
            assert t.mime_type == "audio/mpeg"
            assert t.artist_name == "Artist One"
            assert t.duration_seconds is not None and t.duration_seconds > 0

        # Tracks ordered by disc then track number
        assert tracks[0].title == "First Light"
        assert tracks[1].title == "Middle Ground"
        assert tracks[2].title == "Closing Time"

        TestMusicCollection.track_ids = [t.id for t in tracks]

    def test_05_album_beta_tracks(self, admin_token):
        if not TestMusicCollection.album_beta_id:
            pytest.fail("Album Beta not found — test_03 must have failed")

        tracks, _, disc_count = list_album_tracks(
            admin_token, TestMusicCollection.collection_id, TestMusicCollection.album_beta_id
        )

        assert len(tracks) == 2
        assert disc_count == 1
        titles = [t.title for t in tracks]
        assert titles == ["Overture", "Finale"]

    def test_06_audio_stream(self, admin_token):
        if not TestMusicCollection.track_ids:
            pytest.fail("no tracks found — test_04 must have failed")

        track_id = TestMusicCollection.track_ids[0]
        r = httpx.get(f"{BASE_URL}/audio/{track_id}/stream", headers=bearer(admin_token))
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("audio/")


# ── Movies: video:movie ───────────────────────────────────────────────────────

class TestMovieCollection:
    """
    video:movie collection over video-collection/ which contains:
      action/storm-front.mp4, action/iron-sky.mp4
      drama/quiet-hour.mp4

    video:movie returns all videos across subdirectories as a flat list sorted by title.
    """

    collection_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        resp = create_collection(admin_token, "Test Movies", "video-collection", "video:movie")
        TestMovieCollection.collection_id = resp.id
        grant_collection_access(admin_token, admin_user_id, resp.id)
        wait_for_job(admin_token, resp.scan_job_id, timeout=60)

    def test_02_flat_items(self, admin_token):
        if not TestMovieCollection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        items = list_video_items(admin_token, TestMovieCollection.collection_id)
        assert len(items) == 3, f"expected 3 items, got {len(items)}: {[i.title for i in items]}"

        titles = [i.title for i in items]
        assert "Iron Sky" in titles
        assert "Storm Front" in titles
        assert "The Quiet Hour" in titles

        # video:movie sorts flat by title
        assert titles == sorted(titles), f"items not sorted by title: {titles}"

    def test_03_video_metadata(self, admin_token):
        if not TestMovieCollection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        items = list_video_items(admin_token, TestMovieCollection.collection_id)
        for item in items:
            assert item.mime_type == "video/mp4"
            assert item.duration_seconds is not None, f"item {item.title!r} has no duration"
            assert item.width == 320
            assert item.height == 240


# ── Home movies: video:home_movie ─────────────────────────────────────────────

class TestHomeMovieCollection:
    """
    video:home_movie collection over the same video-collection/ directory.

    video:home_movie is browsed hierarchically:
      - root lists sub-collections (action, drama)
      - each sub-collection lists only its own direct items
      - root collection itself has no direct items
    """

    collection_id: int = 0
    action_id: int = 0
    drama_id: int = 0

    def test_01_create_and_scan(self, admin_token, admin_user_id, db_dsn):
        resp = create_collection(admin_token, "Test Home Movies", "video-collection", "video:home_movie")
        TestHomeMovieCollection.collection_id = resp.id
        grant_collection_access(admin_token, admin_user_id, resp.id)
        wait_for_job(admin_token, resp.scan_job_id, timeout=60)

    def test_02_subcollections(self, admin_token):
        if not TestHomeMovieCollection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        subcols = list_subcollections(admin_token, TestHomeMovieCollection.collection_id)
        assert len(subcols) == 2, f"expected 2 subcollections, got {[c.name for c in subcols]}"

        names = {c.name for c in subcols}
        assert "action" in names
        assert "drama" in names

        for c in subcols:
            if c.name == "action":
                TestHomeMovieCollection.action_id = c.id
            elif c.name == "drama":
                TestHomeMovieCollection.drama_id = c.id

    def test_03_root_has_no_direct_items(self, admin_token):
        """The root directory has no MP4 files — only sub-directories."""
        if not TestHomeMovieCollection.collection_id:
            pytest.fail("collection not created — test_01 must have failed")

        items = list_video_items(admin_token, TestHomeMovieCollection.collection_id)
        assert len(items) == 0, f"root should have no direct items, got {[i.title for i in items]}"

    def test_04_action_subcollection(self, admin_token):
        if not TestHomeMovieCollection.action_id:
            pytest.fail("action subcollection not found — test_02 must have failed")

        items = list_video_items(admin_token, TestHomeMovieCollection.action_id)
        assert len(items) == 2, f"expected 2 items in action, got {[i.title for i in items]}"

        titles = {i.title for i in items}
        assert "Storm Front" in titles
        assert "Iron Sky" in titles

    def test_05_drama_subcollection(self, admin_token):
        if not TestHomeMovieCollection.drama_id:
            pytest.fail("drama subcollection not found — test_02 must have failed")

        items = list_video_items(admin_token, TestHomeMovieCollection.drama_id)
        assert len(items) == 1, f"expected 1 item in drama, got {[i.title for i in items]}"
        assert items[0].title == "The Quiet Hour"
