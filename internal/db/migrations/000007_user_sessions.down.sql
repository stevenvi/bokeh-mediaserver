DROP TABLE IF EXISTS user_sessions;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS refresh_token_hash         text,
    ADD COLUMN IF NOT EXISTS refresh_token_expires_at   timestamptz;
