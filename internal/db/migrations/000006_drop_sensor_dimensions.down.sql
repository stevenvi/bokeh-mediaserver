ALTER TABLE photo_metadata RENAME COLUMN created_at TO taken_at;

ALTER TABLE photo_metadata
    ADD COLUMN IF NOT EXISTS sensor_width_mm  numeric(5,2),
    ADD COLUMN IF NOT EXISTS sensor_height_mm numeric(5,2),
    ADD COLUMN IF NOT EXISTS gps_lat          numeric(10,7),
    ADD COLUMN IF NOT EXISTS gps_lng          numeric(10,7);
