BEGIN;

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs(
    batch_id TEXT PRIMARY KEY,

    source_file TEXT NOT NULL,

    source_row_count INTEGER NOT NULL DEFAULT 0,
    valid_row_count INTEGER NOT NULL DEFAULT 0,
    rejected_row_count INTEGER NOT NULL DEFAULT 0,

    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    status VARCHAR(20) NOT NULL,

    error_message TEXT,

    CONSTRAINT chk_pipeline_run_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
); 

COMMIT;