ALTER TABLE photo_metadata
    DROP COLUMN IF EXISTS sensor_width_mm,
    DROP COLUMN IF EXISTS sensor_height_mm,
    DROP COLUMN IF EXISTS gps_lat,
    DROP COLUMN IF EXISTS gps_lng;

ALTER TABLE photo_metadata RENAME COLUMN taken_at TO created_at;
