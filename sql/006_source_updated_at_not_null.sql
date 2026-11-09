BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM staging.raw_events
        WHERE source_updated_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'Cannot enforce NOT NULL on source_updated_at: existing NULL values found';
    END IF;
END
$$;

ALTER TABLE staging.raw_events
    ALTER COLUMN source_updated_at SET NOT NULL;

COMMIT;