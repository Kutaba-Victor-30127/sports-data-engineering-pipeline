BEGIN;

ALTER TABLE staging.raw_events
    ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ;

COMMIT;