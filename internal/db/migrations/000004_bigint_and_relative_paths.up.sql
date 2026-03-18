-- Rename path columns to make their semantics explicit.
-- All paths are relative to the global MEDIA_PATH — never absolute.
ALTER TABLE collections RENAME COLUMN root_path TO relative_path;
ALTER TABLE media_items RENAME COLUMN fs_path TO relative_path;

-- Rename the associated unique indexes
ALTER INDEX idx_collections_root_path RENAME TO idx_collections_relative_path;
ALTER INDEX idx_media_items_fs_path RENAME TO idx_media_items_relative_path;

-- Widen all ID/FK columns to bigint.
-- Primary keys (GENERATED ALWAYS AS IDENTITY sequences update automatically)
ALTER TABLE server_config ALTER COLUMN id TYPE bigint;
ALTER TABLE users ALTER COLUMN id TYPE bigint;
ALTER TABLE collections ALTER COLUMN id TYPE bigint;
ALTER TABLE jobs ALTER COLUMN id TYPE bigint;
ALTER TABLE media_items ALTER COLUMN id TYPE bigint;

-- Foreign key columns (widened after PKs)
ALTER TABLE collections ALTER COLUMN parent_collection_id TYPE bigint;
ALTER TABLE collection_access ALTER COLUMN user_id TYPE bigint;
ALTER TABLE collection_access ALTER COLUMN collection_id TYPE bigint;
ALTER TABLE jobs ALTER COLUMN related_id TYPE bigint;
ALTER TABLE media_items ALTER COLUMN collection_id TYPE bigint;

-- photo_metadata has a combined PK+FK
ALTER TABLE photo_metadata ALTER COLUMN media_item_id TYPE bigint;
