-- Seed objects for testing and easy Power BI validation
CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.sample_events (
    id BIGSERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,
    event_value NUMERIC(12,2) NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO runtime.sample_events (event_name, event_value)
VALUES
    ('startup', 1.00),
    ('heartbeat', 2.50),
    ('powerbi_check', 3.75);