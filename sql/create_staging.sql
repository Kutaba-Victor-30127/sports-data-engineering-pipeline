CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS  staging.raw_events(
    event_id BIGINT NOT NULL,
    sport VARCHAR(50) NOT NULL,
    league VARCHAR(100) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    home_odds DECIMAL(10, 2) NOT NULL,
    draw_odds DECIMAL(10, 2) NOT NULL,
    away_odds DECIMAL(10, 2) NOT NULL,

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    source_file Text,
    batch_id Text,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ,

    CONSTRAINT uq_raw_events_event_id UNIQUE (event_id)
);  

