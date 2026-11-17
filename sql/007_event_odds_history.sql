BEGIN;

CREATE TABLE IF NOT EXISTS staging.event_odds_history (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    event_id BIGINT NOT NULL,

    sport VARCHAR(50) NOT NULL,
    league VARCHAR(100) NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,

    event_time TIMESTAMP NOT NULL,

    home_odds DECIMAL(10, 2) NOT NULL,
    draw_odds DECIMAL(10, 2) NOT NULL,
    away_odds DECIMAL(10, 2) NOT NULL,

    source_file TEXT NOT NULL,
    batch_id TEXT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_event_odds_history_version
        UNIQUE (
            event_id,
            source_updated_at
        )
);

CREATE INDEX IF NOT EXISTS
    ix_event_odds_history_event_id
ON staging.event_odds_history (
    event_id
);

CREATE INDEX IF NOT EXISTS
    ix_event_odds_history_source_updated_at
ON staging.event_odds_history (
    source_updated_at
);

COMMIT;