-- Rename columns back
ALTER TABLE media_items RENAME COLUMN relative_path TO fs_path;
ALTER TABLE collections RENAME COLUMN relative_path TO root_path;
ALTER INDEX idx_media_items_relative_path RENAME TO idx_media_items_fs_path;
ALTER INDEX idx_collections_relative_path RENAME TO idx_collections_root_path;

-- Note: bigint → integer downgrade is intentionally omitted.
-- Narrowing integer columns risks data loss if any ID exceeds 2^31-1.
