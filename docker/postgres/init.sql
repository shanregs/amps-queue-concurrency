-- AMPS Queue Concurrency — PostgreSQL schema initialisation
-- Runs once when the postgres container starts (docker-entrypoint-initdb.d)

CREATE TABLE IF NOT EXISTS processed_messages (
    id              BIGSERIAL PRIMARY KEY,
    message_id      VARCHAR(512) NOT NULL,
    topic           VARCHAR(255),
    payload         TEXT,
    status          VARCHAR(20)  NOT NULL DEFAULT 'PROCESSED',
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    last_error      TEXT,
    received_at     TIMESTAMPTZ  NOT NULL,
    processed_at    TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    processed_by    VARCHAR(255)
);

-- Idempotency constraint — the UNIQUE index is the cross-JVM deduplication key
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_message_id   ON processed_messages (message_id);
CREATE        INDEX IF NOT EXISTS idx_pm_status        ON processed_messages (status);
CREATE        INDEX IF NOT EXISTS idx_pm_received_at   ON processed_messages (received_at);
CREATE        INDEX IF NOT EXISTS idx_pm_processed_by  ON processed_messages (processed_by);

-- Constraint check — status must be one of the known enum values
ALTER TABLE processed_messages
    ADD CONSTRAINT chk_status
    CHECK (status IN ('PROCESSED', 'FAILED', 'DISCARDED'));

-- Grant to app user
GRANT ALL PRIVILEGES ON TABLE processed_messages TO amps_user;
GRANT USAGE, SELECT ON SEQUENCE processed_messages_id_seq TO amps_user;
