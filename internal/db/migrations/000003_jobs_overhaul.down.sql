-- Remove queued jobs index
DROP INDEX IF EXISTS idx_jobs_queued;

-- Remove integrity_schedule column
ALTER TABLE server_config DROP COLUMN IF EXISTS integrity_schedule;

-- Restore original job type constraint
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_type_check CHECK (type IN (
    'library_scan',
    'transcode',
    'thumbnail_gen',
    'waveform_gen'
));

-- Delete any jobs with new types that would violate the restored constraint
DELETE FROM jobs WHERE type IN ('process_media', 'orphan_cleanup', 'integrity_check');
