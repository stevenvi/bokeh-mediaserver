ALTER TABLE users
    DROP COLUMN IF EXISTS refresh_token_hash,
    DROP COLUMN IF EXISTS refresh_token_expires_at;

CREATE TABLE user_sessions (
    id                  bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_id             bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash          text NOT NULL UNIQUE,
    -- Stores the immediately prior token hash to detect replay of a rotated token.
    -- If a request arrives with a hash matching this column, the session has been
    -- compromised (attacker replayed an old token) and all sessions are revoked.
    previous_token_hash text UNIQUE,
    expires_at          timestamptz NOT NULL,
    device_name         text,
    ip_address          text,
    last_used_at        timestamptz NOT NULL DEFAULT now(),
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
