-- Replace the FNV-64 prefix hash with a BLAKE2b-256 content hash.
-- Existing values are FNV hashes; the next scan will recompute and overwrite them.
ALTER TABLE media_items RENAME COLUMN file_hash_prefix TO file_hash;

-- Add description to photo_metadata, populated from the exiftool composite Description tag.
ALTER TABLE photo_metadata ADD COLUMN description text;
