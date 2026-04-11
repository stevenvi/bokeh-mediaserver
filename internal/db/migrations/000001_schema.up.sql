-- Initial schema for Bokeh Media Server.

CREATE TABLE server_config (
    id                          bigint PRIMARY KEY CHECK (id = 1),
    server_name                 text NOT NULL DEFAULT 'My Bokeh Media Server',
    server_url                  text NOT NULL DEFAULT 'http://localhost:3000',
    log_path                    text NOT NULL DEFAULT '',
    log_level                   text NOT NULL DEFAULT 'warn'
                                    CHECK (log_level IN ('error','warn','info','debug')),
    transcode_bitrate_kbps      int NOT NULL DEFAULT 4000,
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO server_config (id) VALUES (1);

CREATE TABLE users (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                        text NOT NULL,
    is_admin                    boolean NOT NULL DEFAULT false,
    local_access_only           boolean NOT NULL DEFAULT false,
    auth_provider               text NOT NULL DEFAULT 'local',
    auth_data                   jsonb NOT NULL DEFAULT '{}',
    config                      jsonb NOT NULL DEFAULT '{}',
    created_at                  timestamptz NOT NULL DEFAULT now(),
    last_seen_at                timestamptz
);

CREATE INDEX idx_users_name_provider ON users(name, auth_provider);

-- Development admin user: admin / admin (bcrypt hash)
INSERT INTO users (name, is_admin, local_access_only, auth_provider, auth_data)
VALUES (
    'admin',
    true,
    true,
    'local',
    '{"password_hash": "$2a$10$4jXsM1XJS0KMA/YWo.EEIuBF8WwnbIusxElCkxe9hoZ7fzTLjGyTm"}'
);

CREATE TABLE devices (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    device_uuid                 text NOT NULL,
    user_id                     bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    refresh_token_hash          text UNIQUE,
    previous_refresh_token_hash text UNIQUE,
    expires_at                  timestamptz,
    device_name                 text NOT NULL DEFAULT '',
    banned_at                   timestamptz,
    access_history              jsonb NOT NULL DEFAULT '[]',
    created_at                  timestamptz NOT NULL DEFAULT now(),
    last_seen_at                timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_devices_user_uuid ON devices(user_id, device_uuid);
CREATE INDEX idx_devices_user_id ON devices(user_id);
CREATE INDEX idx_devices_token_hash ON devices(refresh_token_hash) WHERE refresh_token_hash IS NOT NULL;
CREATE INDEX idx_devices_banned ON devices(banned_at) WHERE banned_at IS NOT NULL;
CREATE INDEX idx_devices_lru ON devices(user_id, last_seen_at) WHERE banned_at IS NULL;

CREATE TABLE collections (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    parent_collection_id        bigint REFERENCES collections(id) ON DELETE CASCADE,
    root_collection_id          bigint REFERENCES collections(id) ON DELETE CASCADE,
    name                        text NOT NULL,
    type                        text NOT NULL
                                    CHECK (type IN (
                                        'video:movie',
                                        'video:home_movie',
                                        'audio:music',
                                        'audio:show',
                                        'image:photo'
                                    )),
    relative_path               text,
    is_enabled                  boolean NOT NULL DEFAULT true,
    manual_thumbnail            boolean NOT NULL DEFAULT false,
    last_scanned_at             timestamptz,
    missing_since               timestamptz,
    created_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_collections_parent ON collections(parent_collection_id);
CREATE INDEX idx_collections_root   ON collections(root_collection_id);
CREATE INDEX idx_collections_enabled ON collections(id) WHERE is_enabled = true;
-- Sub-collections are uniquely identified by their path within a library.
-- Root collections (relative_path IS NULL) are exempt via the partial index condition.
CREATE UNIQUE INDEX idx_collections_root_path
    ON collections(root_collection_id, relative_path)
    WHERE relative_path IS NOT NULL;

CREATE TABLE collection_access (
    user_id                     bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    collection_id               bigint NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, collection_id)
);

-- PK is (user_id, collection_id); queries that filter by collection_id alone need this.
CREATE INDEX idx_collection_access_collection_id ON collection_access(collection_id);

CREATE TABLE media_items (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    collection_id               bigint NOT NULL
                                    REFERENCES collections(id) ON DELETE CASCADE,
    title                       text NOT NULL,
    relative_path               text NOT NULL,
    file_size_bytes             bigint NOT NULL,
    file_hash                   text NOT NULL,
    mime_type                   text NOT NULL,
    ordinal                     integer,
    missing_since               timestamptz,
    indexed_at                  timestamptz NOT NULL DEFAULT now(),
    created_at                  timestamptz NOT NULL DEFAULT now(),
    hidden_at                   timestamptz,
    search_vector               tsvector GENERATED ALWAYS AS (
                                    to_tsvector('english', title)
                                ) STORED
);

CREATE INDEX idx_media_items_collection ON media_items(collection_id);
CREATE INDEX idx_media_items_collection_active ON media_items(collection_id)
    WHERE missing_since IS NULL AND hidden_at IS NULL;
CREATE UNIQUE INDEX idx_media_items_relative_path ON media_items(relative_path, collection_id);
CREATE INDEX idx_media_items_indexed_at ON media_items(indexed_at);
CREATE INDEX idx_media_items_search     ON media_items USING GIN(search_vector);

CREATE TABLE photo_metadata (
    media_item_id               bigint PRIMARY KEY
                                    REFERENCES media_items(id) ON DELETE CASCADE,
    width_px                    integer,
    height_px                   integer,
    created_at                  timestamptz,
    camera_make                 text,
    camera_model                text,
    lens_model                  text,
    shutter_speed               text,
    aperture                    numeric(4,1),
    iso                         integer,
    focal_length_mm             numeric(6,1),
    focal_length_35mm_equiv     numeric(6,1),
    color_space                 text,
    description                 text,
    placeholder                 text,
    variants_generated_at       timestamptz DEFAULT NULL,
    exif_raw                    jsonb
);

CREATE INDEX idx_photo_metadata_created_at ON photo_metadata(created_at);
CREATE INDEX idx_photo_metadata_variants_pending ON photo_metadata(variants_generated_at)
    WHERE variants_generated_at IS NULL;
CREATE INDEX idx_photo_metadata_shutter_speed ON photo_metadata(shutter_speed);
CREATE INDEX idx_photo_metadata_aperture ON photo_metadata(aperture);
CREATE INDEX idx_photo_metadata_iso ON photo_metadata(iso);
CREATE INDEX idx_photo_metadata_focal_length ON photo_metadata(focal_length_mm);

CREATE TABLE artists (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                        text NOT NULL,
    sort_name                   text NOT NULL,
    manual_thumbnail            boolean NOT NULL DEFAULT false,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    search_vector               tsvector GENERATED ALWAYS AS (
                                    to_tsvector('simple', name)
                                ) STORED
);

CREATE UNIQUE INDEX idx_artists_name ON artists(name);
CREATE INDEX idx_artists_sort ON artists(sort_name);
CREATE INDEX idx_artists_search ON artists USING GIN(search_vector);

CREATE TABLE audio_albums (
    id                  bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                text NOT NULL,
    artist_id           bigint REFERENCES artists(id) ON DELETE SET NULL,
    year                smallint,
    genre               text,
    root_collection_id  bigint NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    is_compilation      boolean NOT NULL DEFAULT false,
    manual_cover        boolean NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    search_vector       tsvector GENERATED ALWAYS AS (
                            to_tsvector('simple', name)
                        ) STORED
);

-- One album per (name, artist, root library). COALESCE handles NULL artist_id.
CREATE UNIQUE INDEX idx_audio_albums_identity
    ON audio_albums(name, COALESCE(artist_id, 0), root_collection_id);
CREATE INDEX idx_audio_albums_artist    ON audio_albums(artist_id);
CREATE INDEX idx_audio_albums_root_coll ON audio_albums(root_collection_id);
CREATE INDEX idx_audio_albums_search    ON audio_albums USING GIN(search_vector);

CREATE TABLE audio_metadata (
    media_item_id               bigint PRIMARY KEY
                                    REFERENCES media_items(id) ON DELETE CASCADE,
    artist_id                   bigint REFERENCES artists(id) ON DELETE SET NULL,
    album_artist_id             bigint REFERENCES artists(id) ON DELETE SET NULL,
    album_id                    bigint REFERENCES audio_albums(id) ON DELETE SET NULL,
    title                       text,
    track_number                smallint,
    disc_number                 smallint DEFAULT 1,
    duration_seconds            numeric(8,2),
    genre                       text,
    year                        smallint,
    replay_gain_db              numeric(5,2),
    has_embedded_art            boolean NOT NULL DEFAULT false,
    processed_at                timestamptz
);

CREATE INDEX idx_audio_meta_album ON audio_metadata(album_id);

CREATE INDEX idx_audio_meta_artist ON audio_metadata(artist_id);
CREATE INDEX idx_audio_meta_album_artist ON audio_metadata(album_artist_id);

CREATE TABLE jobs (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    type                        text NOT NULL,
    status                      text NOT NULL DEFAULT 'queued'
                                    CHECK (status IN ('queued','running','running_sub_jobs','done','failed')),
    related_id                  bigint,
    related_type                text,
    log                         text,
    error_message               text,
    queued_at                   timestamptz NOT NULL DEFAULT now(),
    started_at                  timestamptz,
    completed_at                timestamptz,
    parent_job_id               bigint REFERENCES jobs(id) ON DELETE CASCADE,
    current_step                int NOT NULL DEFAULT 0
);

CREATE INDEX idx_jobs_parent ON jobs(parent_job_id) WHERE parent_job_id IS NOT NULL;

CREATE TABLE jobs_schedule (
    name        text PRIMARY KEY,
    cron        text NOT NULL,
    description text,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO jobs_schedule (name, cron, description) VALUES
    ('collection_scan',  '0 3 * * *',   'Scan all enabled collections for changes'),
    ('integrity_check',  '0 4 * * 0',   'Prune stale items'),
    ('device_cleanup',   '0 2 1 * *',   'Remove inactive device sessions'),
    ('cover_cycle',      '0 5 * * 1',   'Refresh auto-generated sub-collection and artist thumbnails')
ON CONFLICT DO NOTHING;

CREATE INDEX idx_jobs_lookup ON jobs(type, related_id, status);
CREATE INDEX idx_jobs_queued ON jobs(status, queued_at) WHERE status = 'queued';

CREATE TABLE video_metadata (
    media_item_id               bigint PRIMARY KEY
                                    REFERENCES media_items(id) ON DELETE CASCADE,
    duration_seconds            int,
    width                       int,
    height                      int,
    bitrate_kbps                int,
    video_codec                 text,
    audio_codec                 text,
    transcoded_at               timestamptz,
    date                        date,       -- release date (movies) or start date (home movies)
    end_date                    date,       -- home movies only
    author                      text,       -- home movies only
    manual_thumbnail            bool NOT NULL DEFAULT false
);

CREATE TABLE video_bookmarks (
    user_id                     bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    media_item_id               bigint NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    position_seconds            int NOT NULL,
    last_watched_at             timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, media_item_id)
);

CREATE INDEX idx_video_bookmarks_last_watched ON video_bookmarks(last_watched_at);

CREATE OR REPLACE FUNCTION cull_old_bookmarks()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM video_bookmarks
    WHERE user_id = NEW.user_id
      AND last_watched_at < NOW() - INTERVAL '30 days';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER video_bookmark_cull
AFTER INSERT OR UPDATE ON video_bookmarks
FOR EACH ROW EXECUTE FUNCTION cull_old_bookmarks();
