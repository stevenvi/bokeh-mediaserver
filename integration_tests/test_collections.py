"""
Collection integration tests.

Tests are organised as pytest classes. Within each class, methods run in
definition order and share state via class-level variables — later methods
depend on earlier ones having passed. pytest.fail() is used as a guard where
missing prior state would cause a confusing error rather than a clear failure.
"""

import os
from typing import Any
import uuid
import httpx
from conftest import BASE_URL, admin_token, admin_user_id, create_user_local, login_local
import pytest
from helpers.auth import bearer
from helpers.poll import wait_for_job
from helpers.filesystem import variant_path, dzi_manifest_path, dzi_tiles_dir
from pydantic import BaseModel, AwareDatetime

# ── Helper Functions ────────────────────────────────────────────────────────

class CollectionUser(BaseModel):
    id: int
    name: str
    type: str

class CollectionAdmin(CollectionUser):
    relative_path: str
    is_enabled: bool
    last_scanned_at: AwareDatetime | None
    created_at: AwareDatetime

class PhotoItem(BaseModel):
    width_px: int
    height_px: int
    created_at: AwareDatetime | None = None
    camera_make: str | None = None
    camera_model: str | None = None
    lens_model: str | None = None
    shutter_speed: str | None = None
    aperture: float | None = None
    iso: int | None = None
    focal_length_mm: float | None = None
    focal_length_35mm_equiv: float | None = None

class MediaItemSummary(BaseModel):
    id: int
    title: str
    mime_type: str

class MediaItem(MediaItemSummary):
    photo: PhotoItem | None

def get_collections(token: str) -> list[CollectionUser]:
    r = httpx.get(f"{BASE_URL}/api/v1/collections", headers=bearer(token))
    r.raise_for_status()
    return [CollectionUser(**coll) for coll in r.json()]

def get_collections_admin(admin_token: str) -> list[CollectionAdmin]:
    r = httpx.get(f"{BASE_URL}/api/v1/admin/collections", headers=bearer(admin_token))
    r.raise_for_status()
    return [CollectionAdmin(**coll) for coll in r.json()]

def get_subcollections(token: str, collection_id: int) -> list[CollectionUser]:
    r = httpx.get(f"{BASE_URL}/api/v1/collections/{collection_id}/collections", headers=bearer(token))
    r.raise_for_status()
    return [CollectionUser(**coll) for coll in r.json()]

def get_collection_by_id(token: str, collection_id: int) -> CollectionUser:
    r = httpx.get(f"{BASE_URL}/api/v1/collections/{collection_id}", headers=bearer(token))
    r.raise_for_status()
    return CollectionUser(**r.json())

def get_collection_items(token: str, collection_id: int) -> list[MediaItemSummary]:
    r = httpx.get(f"{BASE_URL}/api/v1/collections/{collection_id}/items", headers=bearer(token))
    r.raise_for_status()
    return [MediaItemSummary(**item) for item in r.json().get("items")]

def get_media_item(token: str, item_id: int) -> MediaItem:
    r = httpx.get(f"{BASE_URL}/api/v1/media/{item_id}", headers=bearer(token))
    r.raise_for_status()
    return MediaItem(**r.json())

def image_variant_exists(token: str, item_id: int, variant: str, mime_type: str) -> bool:
    r = httpx.head(f"{BASE_URL}/images/{item_id}/{variant}", headers={**bearer(token), "Accept": mime_type})
    content_type = r.headers.get("content-type", "")
    return r.status_code == 200 and content_type.startswith("image/")

def get_image_exif(token: str, item_id: int) -> dict[str, Any]:
    r = httpx.get(f"{BASE_URL}/images/{item_id}/exif", headers=bearer(token))
    r.raise_for_status()
    assert r.headers["content-type"].startswith("application/json")
    return r.json()

def get_dzi_manifest(token: str, item_id: int) -> str:
    r = httpx.get(f"{BASE_URL}/images/{item_id}/tiles/image.dzi", headers=bearer(token))
    r.raise_for_status()
    assert "xml" in r.headers.get("content-type", "").lower()
    return r.text

def hide_item(token: str, item_id: int) -> None:
    r = httpx.post(f"{BASE_URL}/api/v1/admin/media/{item_id}/hide", headers=bearer(token))
    r.raise_for_status()

def unhide_item(token: str, item_id: int) -> None:
    r = httpx.delete(f"{BASE_URL}/api/v1/admin/media/{item_id}/hide", headers=bearer(token))
    r.raise_for_status()

def trigger_rescan(token: str, collection_id: int) -> int:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/jobs",
        headers=bearer(token),
        json={"type": "collection_scan", "related_id": collection_id, "related_type": "collection"},
    )
    r.raise_for_status()
    return r.json()["id"]

class CreateCollectionResponse(BaseModel):
    id: int
    scan_job_id: int

def create_collection(admin_token: str, name: str, relative_path: str, type: str) -> CreateCollectionResponse:
    r = httpx.post(
        f"{BASE_URL}/api/v1/admin/collections",
        headers=bearer(admin_token),
        json={
            "name": name,
            "type": type,
            "relative_path": relative_path,
        },
    )
    r.raise_for_status()
    return CreateCollectionResponse(**r.json())

def list_collection_access(admin_token: str, user_id: int) -> list[int]:
    r = httpx.get(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access",
        headers=bearer(admin_token),
    )
    r.raise_for_status()
    return r.json()


def grant_collection_access(admin_token: str, user_id: int, collection_id: int) -> None:
    r = httpx.patch(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access",
        headers=bearer(admin_token),
        json={"collection_ids": [collection_id]},
    )
    r.raise_for_status()

def revoke_collection_access(admin_token: str, user_id: int, collection_id: int) -> None:
    r = httpx.delete(
        f"{BASE_URL}/api/v1/admin/users/{user_id}/collection_access/{collection_id}",
        headers=bearer(admin_token),
    )
    r.raise_for_status()

# ── Variant existence helper ─────────────────────────────────────────────────
# Mirrors imaging.GenerateVariant: a variant is only written when the source's
# longest edge strictly exceeds the variant's target size.
_VARIANT_SIZES = {"thumb": 400, "small": 1280, "preview": 1920}

def should_variant_exist(width: int, height: int, variant: str) -> bool:
    return max(width, height) > _VARIANT_SIZES[variant]


# ── Known BLAKE2b-256 hashes for photo-album-1 test images ──────────────────
# Computed offline from the files in testdata/media/photo-album-1/.
PHOTO_ALBUM_1_FILES = {
    "sunflower": {
        "hash": "4137f161bcd94ffa71c899310dba6bba7dae550a235918bfb5ccb948a69206fb",
        "title": "sunflower",
        "mime_type": "image/avif",
        "camera_make": "NIKON CORPORATION",
        "camera_model": "NIKON D7500",
        "lens_model": "35.0 mm f/1.8",
        "width_px": 4167,
        "height_px": 2778,
        "taken_at_prefix": "2020-04-26",
        "iso": 50,
        "focal_length_mm": 35.0,
        "focal_length_35mm": 52.0,
        "aperture": 1.8,
    },
    "rollercoaster": {
        "hash": "0dacf893b6176f9ab3683e6186f816b9b5b1fbd59a47e15e227f4475ac5b41f3",
        "title": "rollercoaster",
        "mime_type": "image/jpeg",
        "camera_make": "NIKON",
        "camera_model": "COOLPIX AW100",
        "lens_model": None,
        "width_px": 576,
        "height_px": 865,
        "taken_at_prefix": "2013-05-27",
        "iso": 125,
        "focal_length_mm": 13.9,
        "focal_length_35mm": 78.0,
        "aperture": 4.4,
    },
    "moon": {
        "hash": "b35fc5dabb4e43ffa8ae9b5deb8eb3380333720e9a333bdc8845fa1027b02eb3",
        "title": "moon",
        "mime_type": "image/avif",
        "camera_make": "SONY",
        "camera_model": "ILCE-7RM3",
        "lens_model": "FE 200-600mm F5.6-6.3 G OSS",
        "width_px": 395,
        "height_px": 395,
        "taken_at_prefix": None,  # no DateTimeOriginal in this file
        "iso": 100,
        "focal_length_mm": 600.0,
        "focal_length_35mm": None,  # not in EXIF for this file
        "aperture": 6.3,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. Initial state
# ─────────────────────────────────────────────────────────────────────────────

class TestInitialState:
    def test_initial_state(self, base_url, admin_token):
        """After reset, all collection endpoints should reflect an empty library."""
        # User-facing list: empty array (not null)
        collections = get_collections(admin_token)
        assert len(collections) == 0

        # Admin list: also empty
        collections_admin = get_collections_admin(admin_token)
        assert len(collections_admin) == 0

        # Per-collection endpoints all 404 when nothing exists
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_collection_by_id(admin_token, 1234)
        assert exc_info.value.response.status_code == 404

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_collection_items(admin_token, 1234)
        assert exc_info.value.response.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# 2. Single-tier collection (photo-album-1: 3 flat images)
# ─────────────────────────────────────────────────────────────────────────────

class TestSingleTierCollection:
    collection_id: int = None
    scan_job_id: int = None
    item_map: dict[str, MediaItem] = {}  # title → item dict (populated after grant access)

    def test_01_create_collection(self, admin_token, db_dsn):
        # Create collection
        collection = create_collection(admin_token, 'Photo Album 1', 'photo-album-1', 'image:photo')
        TestSingleTierCollection.collection_id = collection.id
        TestSingleTierCollection.scan_job_id = collection.scan_job_id

        # Then wait for the scan job to complete
        wait_for_job(admin_token, TestSingleTierCollection.scan_job_id, timeout=60)

    def test_02_no_access_until_granted(self, admin_token):
        # Collection exists but admin has no collection_access row yet
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_collection_by_id(admin_token, TestSingleTierCollection.collection_id)  
        assert exc_info.value.response.status_code == 404

        # Grant access to it now
        grant_collection_access(admin_token, 1, TestSingleTierCollection.collection_id)  # user_id=1 is admin created in conftest.py
        get_collection_by_id(admin_token, TestSingleTierCollection.collection_id)  # should succeed now

    def test_03_collection_has_three_items(self, admin_token):
        items = get_collection_items(admin_token, TestSingleTierCollection.collection_id)
        assert len(items) == 3, f"expected 3 items, got {len(items)}"

        # Index by title for later tests
        for item in items:
            TestSingleTierCollection.item_map[item.title] = item

    def test_04_filesystem_variants_exist(self, data_path):
        """Images should have AVIF variants and DZI tiles for sizes that fit within source dimensions."""
        for name, info in PHOTO_ALBUM_1_FILES.items():
            h = info["hash"]
            w, ht = info["width_px"], info["height_px"]
            for variant in ("thumb", "small", "preview"):
                if should_variant_exist(w, ht, variant):
                    path = variant_path(data_path, h, variant, "avif")
                    assert os.path.isfile(path), f"missing {variant}.avif for {name}"

            # thumb JPEG is only pre-generated when thumb itself is generated
            if should_variant_exist(w, ht, "thumb"):
                jpeg = variant_path(data_path, h, "thumb", "jpg")
                assert os.path.isfile(jpeg), f"missing thumb.jpg for {name}"
            
            # DZI manifest
            assert os.path.isfile(dzi_manifest_path(data_path, h)), f"missing DZI for {name}"
            
            # At least one tile level directory
            tiles_dir = dzi_tiles_dir(data_path, h)
            assert os.path.isdir(tiles_dir), f"missing tiles dir for {name}"
            assert len(os.listdir(tiles_dir)) > 0, f"tiles dir empty for {name}"

    def test_05_exif_data_matches_expectations(self, admin_token):
        """GET /media/:id should return EXIF values that match known-good hardcoded values."""
        for name, expected in PHOTO_ALBUM_1_FILES.items():
            item_id = TestSingleTierCollection.item_map[expected["title"]].id
            media_item = get_media_item(admin_token, item_id)
            assert media_item.photo is not None, f"no photo metadata for {name}"

            photo: PhotoItem = media_item.photo
            assert photo.camera_make == expected["camera_make"], name
            assert photo.camera_model == expected["camera_model"], name
            assert photo.lens_model == expected["lens_model"], name
            assert photo.width_px == expected["width_px"], name
            assert photo.height_px == expected["height_px"], name
            assert photo.iso == expected["iso"], name
            assert pytest.approx(photo.focal_length_mm, rel=1e-3) == expected["focal_length_mm"], name
            assert pytest.approx(photo.aperture, rel=1e-3) == expected["aperture"], name
            if expected["focal_length_35mm"] is not None:
                assert pytest.approx(photo.focal_length_35mm_equiv, rel=1e-3) == expected["focal_length_35mm"], name
            if expected["taken_at_prefix"] is not None:
                assert photo.created_at.isoformat().startswith(expected["taken_at_prefix"]), name

    def test_06_image_variants_served(self, admin_token):
        """GET /images/:id/{variant} should return image data."""
        item_map = TestSingleTierCollection.item_map
        if not item_map:
            pytest.fail("item_map empty — test_05 must have failed")

        for name, expected in PHOTO_ALBUM_1_FILES.items():
            item_id = item_map[expected["title"]].id
            w, h = expected["width_px"], expected["height_px"]
            for variant in ("thumb", "small", "preview"):
                if should_variant_exist(w, h, variant):
                    assert image_variant_exists(admin_token, item_id, variant, "image/avif"), f"{variant} AVIF for {name} not served"

            # thumb JPEG is only pre-generated when thumb itself is generated
            if should_variant_exist(w, h, "thumb"):
                assert image_variant_exists(admin_token, item_id, "thumb", "image/jpeg"), f"thumb JPEG for {name} not served"

    def test_07_exif_endpoint(self, admin_token):
        """GET /images/:id/exif should return JSON EXIF data."""
        item_map = TestSingleTierCollection.item_map
        if not item_map:
            pytest.fail("item_map empty — test_05 must have failed")

        item_id = item_map["sunflower"].id
        exif = get_image_exif(admin_token, item_id)
        assert "Make" in exif  # exiftool always includes at least basic fields

        # TODO: This test is possibly inadequate

    def test_08_dzi_manifest(self, admin_token):
        """GET /images/:id/tiles/image.dzi should return an XML manifest."""
        item_map = TestSingleTierCollection.item_map
        if not item_map:
            pytest.fail("item_map empty — test_05 must have failed")

        item_id = item_map["sunflower"].id
        dzi = get_dzi_manifest(admin_token, item_id)
        assert "<Image" in dzi
        assert "<Size" in dzi

        # TODO: This test is inadequate

    def test_09_dzi_tiles_served(self, admin_token, data_path):
        """GET /images/:id/tiles/* should serve tile files."""
        item_map = TestSingleTierCollection.item_map
        if not item_map:
            pytest.fail("item_map empty — test_05 must have failed")

        item_id = item_map["sunflower"].id
        h = PHOTO_ALBUM_1_FILES["sunflower"]["hash"]
        tiles_base = os.path.join(dzi_tiles_dir(data_path, h), "image_files")

        # Find the lowest-resolution level (smallest number) and its first tile
        levels = sorted(int(d) for d in os.listdir(tiles_base) if d.isdigit())
        assert len(levels) > 0, "no tile levels found"
        lowest = levels[0]
        tiles = os.listdir(os.path.join(tiles_base, str(lowest)))
        assert len(tiles) > 0, f"no tiles in level {lowest}"
        tile_name = tiles[0]

        r = httpx.get(
            f"{BASE_URL}/images/{item_id}/tiles/image_files/{lowest}/{tile_name}",
            headers=bearer(admin_token),
        )
        r.raise_for_status()

        # TODO: This test is inadequate

    def test_10_hide_item(self, admin_token, db_dsn):
        item_map = TestSingleTierCollection.item_map
        if not item_map:
            pytest.fail("item_map empty — test_05 must have failed")
        item_id = item_map["sunflower"].id
        hide_item(admin_token, item_id)

        # Hiding it should make it disappear from the collection's item list...
        collection_items = get_collection_items(admin_token, TestSingleTierCollection.collection_id)
        assert item_id not in [i.id for i in collection_items]
        assert len(collection_items) == 2

        # Rescanning the collection should not make the hidden item reappear...
        scan_job_id = trigger_rescan(admin_token, TestSingleTierCollection.collection_id)
        wait_for_job(admin_token, scan_job_id, timeout=60)
        collection_items = get_collection_items(admin_token, TestSingleTierCollection.collection_id)
        assert item_id not in [i.id for i in collection_items], "hidden item reappeared after rescan"
        assert len(collection_items) == 2

        # Hidden items should be fully hidden
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_media_item(admin_token, item_id)
        assert exc_info.value.response.status_code == 404

        # Unhide it and it should show up again
        unhide_item(admin_token, item_id)
        get_media_item(admin_token, item_id)  # should succeed now
        collection_items = get_collection_items(admin_token, TestSingleTierCollection.collection_id)
        item_ids = [i.id for i in collection_items]
        assert item_id in item_ids
        assert len(collection_items) == 3


# ─────────────────────────────────────────────────────────────────────────────
# 3. Multi-tier collection (photo-album-2: nested sub-albums)
# ─────────────────────────────────────────────────────────────────────────────
#
# Directory structure:
#   photo-album-2/
#     dragonfly.jpg, shell.jpg           (root-level items)
#     nested-album-1/
#       field.avif, sunflower.avif
#       nested-album-3/
#         ipod.jpg, rose-and-butterfly.avif
#     nested-album-2/
#       bee.jpg, fisheye.jpg

ALBUM2_TOTAL_ITEMS = 8   # all images across all levels
ALBUM2_ROOT_ITEMS = 2    # dragonfly.jpg + shell.jpg
ALBUM2_SUBCOLLECTIONS = 2  # nest-1, nest-2

class TestMultiTierCollection:
    collection_id: int = None
    scan_job_id: int = None
    token: str = None
    sub_collection_ids: list = []

    def test_01_create_collection(self, admin_token, admin_user_id, db_dsn):
        r = create_collection(admin_token, "Photo Album 2", "photo-album-2", "image:photo")
        TestMultiTierCollection.collection_id = r.id
        TestMultiTierCollection.scan_job_id = r.scan_job_id

        wait_for_job(admin_token, TestMultiTierCollection.scan_job_id, timeout=120)
        grant_collection_access(admin_token, admin_user_id, TestMultiTierCollection.collection_id)
        
        # item list should be as expected
        items = get_collection_items(admin_token, TestMultiTierCollection.collection_id)
        assert len(items) == ALBUM2_ROOT_ITEMS, (
            f"expected {ALBUM2_ROOT_ITEMS} root items, got {len(items)}"
        )

        # subalbum list should be as expected
        sub_collections = get_subcollections(admin_token, TestMultiTierCollection.collection_id)
        assert len(sub_collections) == ALBUM2_SUBCOLLECTIONS, (
            f"expected {ALBUM2_SUBCOLLECTIONS} sub-collections, got {len(sub_collections)}"
        )
        TestMultiTierCollection.sub_collection_ids = [s.id for s in sub_collections]

        # subalbums do not need additional grants to read them
        for sid in TestMultiTierCollection.sub_collection_ids:
            get_collection_by_id(admin_token, sid)

    def test_02_traverse_full_tree(self, admin_token):
        """
        Walk the entire collection tree recursively via API and confirm all
        media items are reachable.
        """
        def collect_items(collection_id) -> list[MediaItem]:
            items: list[MediaItem] = []
            
            # Items in this collection
            r = get_collection_items(admin_token, collection_id)
            items.extend(r)
            
            # Recurse into sub-collections
            sc = get_subcollections(admin_token, collection_id)
            for si in sc:
                items.extend(collect_items(si.id))

            return items

        all_items = collect_items(TestMultiTierCollection.collection_id)
        assert len(all_items) == ALBUM2_TOTAL_ITEMS, (
            f"expected {ALBUM2_TOTAL_ITEMS} total items, got {len(all_items)}"
        )

    def test_03_ungranted_user_cannot_access_subalbums(self, admin_token):
        # Create and log in as new user
        user_id = create_user_local(admin_token, "testuser", "testpass123", False)
        tokens = login_local("testuser", "testpass123", str(uuid.uuid4()))
        TestMultiTierCollection.token = tokens.access_token

        # New user with no access grant can't see sub-collections.
        sub_ids = TestMultiTierCollection.sub_collection_ids
        for sid in sub_ids:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                get_collection_by_id(TestMultiTierCollection.token, sid)
            assert exc_info.value.response.status_code == 404

        # Nor can they access media
        items = get_collection_items(admin_token, TestMultiTierCollection.collection_id)
        assert len(items) == 2
        item_id = items[0].id

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_media_item(TestMultiTierCollection.token, item_id)
        assert exc_info.value.response.status_code == 404

        assert False == image_variant_exists(TestMultiTierCollection.token, item_id, 'thumb', 'image/avif')

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_image_exif(TestMultiTierCollection.token, item_id)
        assert exc_info.value.response.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# 4. Collection access restrictions
# ─────────────────────────────────────────────────────────────────────────────

class TestCollectionAccessRestrictions:
    collection_id: int = None
    user_id: int = None
    user_token: str = None

    def test_01_comprehensive_access_test(self, admin_token):
        # Create collection (no scan needed, we only test access control)
        collection = create_collection(admin_token, "Access Test Album", "photo-album-3", "image:photo")

        # Create user
        user = create_user_local(admin_token, "accesstestuser", "pass123", False)
        tokens = login_local("accesstestuser", "pass123", str(uuid.uuid4()))

        # Should have no collection access yet
        collection_access = list_collection_access(admin_token, user)
        assert len(collection_access) == 0

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_collection_by_id(tokens.access_token, collection.id)
        assert exc_info.value.response.status_code == 404

        # Grant access and they should be able to read it now
        grant_collection_access(admin_token, user, collection.id)
        collection_access = list_collection_access(admin_token, user)
        assert len(collection_access) == 1
        assert collection_access[0] == collection.id
        
        c = get_collection_by_id(tokens.access_token, collection.id)
        assert c.id == collection.id

        # And revoking makes it inaccessible again
        revoke_collection_access(admin_token, user, collection.id)
        collection_access = list_collection_access(admin_token, user)
        assert len(collection_access) == 0

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            get_collection_by_id(tokens.access_token, collection.id)
        assert exc_info.value.response.status_code == 404
