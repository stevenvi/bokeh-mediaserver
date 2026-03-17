CREATE TABLE server_config (
    id                          integer PRIMARY KEY CHECK (id = 1),
    server_name                 text NOT NULL DEFAULT 'My Bokeh Media Server',
    server_url                  text NOT NULL DEFAULT 'http://localhost:3000',
    worker_count                smallint NOT NULL DEFAULT 2,
    log_path                    text NOT NULL DEFAULT '',
    log_level                   text NOT NULL DEFAULT 'warn'
                                    CHECK (log_level IN ('error','warn','info','debug')),
    scan_schedule               text,
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

-- Development seed. Removed before public release.
INSERT INTO server_config (id) VALUES (1);

CREATE TABLE users (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                        text NOT NULL,
    is_admin                    boolean NOT NULL DEFAULT false,
    auth_provider               text NOT NULL DEFAULT 'local',
    auth_data                   jsonb NOT NULL DEFAULT '{}',
    refresh_token_hash          text,
    refresh_token_expires_at    timestamptz,
    config                      jsonb NOT NULL DEFAULT '{}',
    created_at                  timestamptz NOT NULL DEFAULT now(),
    last_seen_at                timestamptz
);

-- Development admin user: admin / admin
-- Password hash is bcrypt of "admin"
-- TODO: Set up to be removed as part of first user setup wizard
--       Probably could make the password random characters printed 
--       to the console instead of hardcoding it.
INSERT INTO users (name, is_admin, auth_provider, auth_data)
VALUES (
    'admin',
    true,
    'local',
    '{"password_hash": "$2a$12$YwqOBCGGjlimOCaFCKXH6OFat1jqKy3xMJXBXbJYO0eBdwQWFNXNW"}'
);

CREATE TABLE collections (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    parent_collection_id        integer REFERENCES collections(id) ON DELETE CASCADE,
    name                        text NOT NULL,
    type                        text NOT NULL
                                    CHECK (type IN (
                                        'video:movie',
                                        'video:home_movie',
                                        'audio:album',
                                        'audio:radio',
                                        'image:photo'                                        
                                    )),
    root_path                   text,
    is_enabled                  boolean NOT NULL DEFAULT true,
    last_scanned_at             timestamptz,
    missing_since               timestamptz,
    created_at                  timestamptz NOT NULL DEFAULT now()
);

-- Top-level collection queries and recursive CTE traversal
CREATE INDEX idx_collections_parent ON collections(parent_collection_id);

-- Required for ON CONFLICT (root_path) in the indexer folder walk
-- This means your collections must have distinct paths and no overlapping,
-- which could prove problematic: for example, if I keep my photos and videos
-- stored together, I cannot create both a Home Movies and a Photos collection
-- from that same path, I'll have to pick one media type or the other.
CREATE UNIQUE INDEX idx_collections_root_path ON collections(root_path)
    WHERE root_path IS NOT NULL;

CREATE TABLE collection_access (
    user_id                     integer NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    collection_id               integer NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    granted_at                  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, collection_id)
);

CREATE TABLE jobs (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    type                        text NOT NULL
                                    CHECK (type IN (
                                        'library_scan',
                                        'transcode',
                                        'thumbnail_gen',
                                        'waveform_gen'
                                    )),
    status                      text NOT NULL DEFAULT 'queued'
                                    CHECK (status IN ('queued','running','done','failed')),
    related_id                  integer,
    related_type                text,
    log                         text,
    error_message               text,
    queued_at                   timestamptz NOT NULL DEFAULT now(),
    started_at                  timestamptz,
    completed_at                timestamptz
);

CREATE INDEX idx_jobs_lookup ON jobs(type, related_id, status);
