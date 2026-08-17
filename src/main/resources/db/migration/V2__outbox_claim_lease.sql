ALTER TABLE outbox_events
    ADD COLUMN claimed_by VARCHAR(64) NULL,
    ADD COLUMN claimed_until TIMESTAMPTZ NULL;

CREATE INDEX idx_outbox_claimable
    ON outbox_events (claimed_until, next_attempt_at, occurred_at)
    WHERE published_at IS NULL;
