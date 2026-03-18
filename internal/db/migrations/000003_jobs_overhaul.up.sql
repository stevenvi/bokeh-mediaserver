-- Add new job types to the CHECK constraint and add integrity_schedule to server_config.

-- Widen the job type constraint to include new types
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_type_check CHECK (type IN (
    'library_scan',
    'process_media',
    'transcode',
    'thumbnail_gen',
    'waveform_gen',
    'orphan_cleanup',
    'integrity_check'
));

-- Add integrity_schedule column to server_config
ALTER TABLE server_config ADD COLUMN IF NOT EXISTS integrity_schedule text;

-- Set default scan_schedule if NULL
UPDATE server_config SET scan_schedule = '0 3 * * *' WHERE id = 1 AND scan_schedule IS NULL;

-- Index for dispatcher polling: find queued jobs quickly
CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs(status, queued_at) WHERE status = 'queued';
