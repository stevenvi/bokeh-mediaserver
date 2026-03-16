CREATE TABLE media_items (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    collection_id               integer NOT NULL
                                    REFERENCES collections(id) ON DELETE CASCADE,
    title                       text NOT NULL,
    fs_path                     text NOT NULL,
    file_size_bytes             bigint NOT NULL, -- total size of the file
    file_hash_prefix            text NOT NULL,   -- hash of the first 64 KB of the file, for quick duplicate detection
    mime_type                   text NOT NULL,
    ordinal                     integer,         -- track number, episode number, etc.
    missing_since               timestamptz,
    indexed_at                  timestamptz NOT NULL DEFAULT now(),
    created_at                  timestamptz NOT NULL DEFAULT now(),
    search_vector               tsvector GENERATED ALWAYS AS (
                                    to_tsvector('english', title)
                                ) STORED
);

CREATE INDEX idx_media_items_collection ON media_items(collection_id);
CREATE UNIQUE INDEX idx_media_items_fs_path ON media_items(fs_path);
CREATE INDEX idx_media_items_indexed_at ON media_items(indexed_at);
CREATE INDEX idx_media_items_search     ON media_items USING GIN(search_vector);

CREATE TABLE photo_metadata (
    media_item_id               integer PRIMARY KEY
                                    REFERENCES media_items(id) ON DELETE CASCADE,
    width_px                    integer,
    height_px                   integer,
    taken_at                    timestamptz,
    camera_make                 text,
    camera_model                text,
    lens_model                  text,
    shutter_speed               text,
    aperture                    numeric(4,1),
    iso                         integer,
    focal_length_mm             numeric(6,1),
    focal_length_35mm_equiv     numeric(6,1),
    sensor_width_mm             numeric(5,2),
    sensor_height_mm            numeric(5,2),
    gps_lat                     numeric(9,6),
    gps_lng                     numeric(9,6),
    color_space                 text,
    placeholder                 text,          -- base64-encoded 32x32 AVIF, embedded in API response
    variants_generated_at       timestamptz DEFAULT NULL,
    exif_raw                    jsonb
);

CREATE INDEX idx_photo_metadata_taken_at ON photo_metadata(taken_at);
CREATE INDEX idx_photo_metadata_shutter_spssed ON photo_metadata(shutter_speed);
CREATE INDEX idx_photo_metadata_aperture ON photo_metadata(aperture);
CREATE INDEX idx_photo_metadata_iso ON photo_metadata(iso);
CREATE INDEX idx_photo_metadata_focal_length ON photo_metadata(focal_length_mm);