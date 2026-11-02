BEGIN;

ALTER TABLE ops.pipeline_runs
    ADD COLUMN run_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ADD COLUMN ignored_count INTEGER;

ALTER TABLE ops.pipeline_runs
    DROP CONSTRAINT pipeline_runs_pkey;

ALTER TABLE ops.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id);

CREATE INDEX ix_pipeline_runs_batch_id
    ON ops.pipeline_runs (batch_id);

ALTER TABLE ops.pipeline_runs
    ALTER COLUMN source_row_count DROP DEFAULT,
    ALTER COLUMN source_row_count DROP NOT NULL,
    ALTER COLUMN valid_row_count DROP DEFAULT,
    ALTER COLUMN valid_row_count DROP NOT NULL,
    ALTER COLUMN rejected_row_count DROP DEFAULT,
    ALTER COLUMN rejected_row_count DROP NOT NULL,
    ALTER COLUMN inserted_count DROP DEFAULT,
    ALTER COLUMN inserted_count DROP NOT NULL,
    ALTER COLUMN updated_count DROP DEFAULT,
    ALTER COLUMN updated_count DROP NOT NULL;

COMMIT;

