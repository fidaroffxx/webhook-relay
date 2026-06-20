CREATE TABLE IF NOT EXISTS subscriptions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    target_url  TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id  UUID NOT NULL REFERENCES subscriptions(id),
    event_type       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    payload_path     TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at     TIMESTAMPTZ
);

CREATE INDEX idx_events_subscription ON events(subscription_id);
CREATE INDEX idx_events_status ON events(status);

CREATE TABLE IF NOT EXISTS deliveries (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id     UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    attempt      INT  NOT NULL,
    status       TEXT NOT NULL,
    status_code  INT,
    error        TEXT,
    log_path     TEXT,
    duration_ms  INT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_event ON deliveries(event_id);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT NOT NULL,
    topic        TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, event_id)
);
