ALTER TABLE photo_metadata DROP COLUMN description;
ALTER TABLE media_items RENAME COLUMN file_hash TO file_hash_prefix;
