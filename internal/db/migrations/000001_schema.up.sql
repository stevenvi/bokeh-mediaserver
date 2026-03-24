-- Initial schema for Bokeh Media Server.

CREATE TABLE server_config (
    id                          bigint PRIMARY KEY CHECK (id = 1),
    server_name                 text NOT NULL DEFAULT 'My Bokeh Media Server',
    server_url                  text NOT NULL DEFAULT 'http://localhost:3000',
    worker_count                smallint NOT NULL DEFAULT 2,
    log_path                    text NOT NULL DEFAULT '',
    log_level                   text NOT NULL DEFAULT 'warn'
                                    CHECK (log_level IN ('error','warn','info','debug')),
    scan_schedule               text DEFAULT '0 3 * * *',
    integrity_schedule          text DEFAULT '0 4 * * 0',
    device_cleanup_schedule     text DEFAULT '0 2 1 * *',
    cover_cycle_schedule        text DEFAULT '0 5 * * 1',
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO server_config (id) VALUES (1);

CREATE TABLE users (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                        text NOT NULL,
    is_admin                    boolean NOT NULL DEFAULT false,
    auth_provider               text NOT NULL DEFAULT 'local',
    auth_data                   jsonb NOT NULL DEFAULT '{}',
    config                      jsonb NOT NULL DEFAULT '{}',
    created_at                  timestamptz NOT NULL DEFAULT now(),
    last_seen_at                timestamptz
);

CREATE INDEX idx_users_name_provider ON users(name, auth_provider);

-- Development admin user: admin / admin (bcrypt hash)
INSERT INTO users (name, is_admin, auth_provider, auth_data)
VALUES (
    'admin',
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
    name                        text NOT NULL,
    type                        text NOT NULL
                                    CHECK (type IN (
                                        'video:movie',
                                        'video:home_movie',
                                        'audio:album',
                                        'audio:radio',
                                        'image:photo'
                                    )),
    relative_path               text,
    is_enabled                  boolean NOT NULL DEFAULT true,
    manual_cover                boolean NOT NULL DEFAULT false,
    last_scanned_at             timestamptz,
    missing_since               timestamptz,
    created_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_collections_parent ON collections(parent_collection_id);
CREATE INDEX idx_collections_enabled ON collections(id) WHERE is_enabled = true;
CREATE UNIQUE INDEX idx_collections_relative_path ON collections(relative_path)
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
CREATE UNIQUE INDEX idx_media_items_relative_path ON media_items(relative_path);
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

CREATE TABLE jobs (
    id                          bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    type                        text NOT NULL
                                    CHECK (type IN (
                                        'library_scan',
                                        'process_media',
                                        'transcode',
                                        'thumbnail_gen',
                                        'waveform_gen',
                                        'orphan_cleanup',
                                        'integrity_check',
                                        'device_cleanup',
                                        'cover_cycle'
                                    )),
    status                      text NOT NULL DEFAULT 'queued'
                                    CHECK (status IN ('queued','running','done','failed')),
    related_id                  bigint,
    related_type                text,
    log                         text,
    error_message               text,
    queued_at                   timestamptz NOT NULL DEFAULT now(),
    started_at                  timestamptz,
    completed_at                timestamptz
);

CREATE INDEX idx_jobs_lookup ON jobs(type, related_id, status);
CREATE INDEX idx_jobs_queued ON jobs(status, queued_at) WHERE status = 'queued';
