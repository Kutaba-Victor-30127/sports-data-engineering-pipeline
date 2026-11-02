import pandas as pd
import pytest
from sqlalchemy import text

from src.database.connection import get_engine
from src.ingestion.run_batch import run_batch


TEST_BATCH_PREFIX = "pytest-observability-"


@pytest.fixture(autouse=True)
def clean_test_pipeline_runs():
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text("""
                DELETE FROM ops.pipeline_runs
                WHERE batch_id LIKE :prefix
            """),
            {
                "prefix": f"{TEST_BATCH_PREFIX}%"
            },
        )

    yield

    with engine.begin() as connection:
        connection.execute(
            text("""
                DELETE FROM ops.pipeline_runs
                WHERE batch_id LIKE :prefix
            """),
            {
                "prefix": f"{TEST_BATCH_PREFIX}%"
            },
        )


def test_successful_run_records_observability(
    monkeypatch,
):
    batch_id = (
        f"{TEST_BATCH_PREFIX}success"
    )

    batch = {
        "batch_id": batch_id,
        "csv_key": "raw/test/events.csv",
    }

    def fake_load_events(
        s3_key,
        batch_id,
        on_counts=None,
    ):
        if on_counts is not None:
            on_counts(
                source_row_count=5,
            )

            on_counts(
                source_row_count=5,
                valid_row_count=4,
                rejected_row_count=1,
            )

        valid_df = pd.DataFrame(
            {"event_id": [1, 2, 3, 4]}
        )

        rejected_df = pd.DataFrame(
            {"event_id": [5]}
        )

        return valid_df, rejected_df

    def fake_load_to_staging(
        df,
        connection=None,
    ):
        return {
            "inserted_count": 4,
            "updated_count": 0,
            "ignored_count": 0,
        }

    monkeypatch.setattr(
        "src.ingestion.run_batch.load_events",
        fake_load_events,
    )

    monkeypatch.setattr(
        "src.ingestion.run_batch.load_to_staging",
        fake_load_to_staging,
    )

    run_batch(
        batch=batch
    )

    engine = get_engine()

    with engine.connect() as connection:
        run = connection.execute(
            text("""
                SELECT
                    source_row_count,
                    valid_row_count,
                    rejected_row_count,
                    inserted_count,
                    updated_count,
                    ignored_count,
                    status,
                    error_message,
                    finished_at
                FROM ops.pipeline_runs
                WHERE batch_id = :batch_id
                ORDER BY run_id DESC
                LIMIT 1
            """),
            {
                "batch_id": batch_id
            },
        ).mappings().one()

    assert run["source_row_count"] == 5
    assert run["valid_row_count"] == 4
    assert run["rejected_row_count"] == 1

    assert run["inserted_count"] == 4
    assert run["updated_count"] == 0
    assert run["ignored_count"] == 0

    assert run["status"] == "SUCCESS"
    assert run["error_message"] is None
    assert run["finished_at"] is not None


def test_failed_run_records_error(
    monkeypatch,
):
    batch_id = (
        f"{TEST_BATCH_PREFIX}failure"
    )

    batch = {
        "batch_id": batch_id,
        "csv_key": "raw/test/events.csv",
    }

    def fake_load_events(
        s3_key,
        batch_id,
        on_counts=None,
    ):
        if on_counts is not None:
            on_counts(
                source_row_count=5
            )

        raise RuntimeError(
            "simulated ingestion failure"
        )

    monkeypatch.setattr(
        "src.ingestion.run_batch.load_events",
        fake_load_events,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated ingestion failure",
    ):
        run_batch(
            batch=batch
        )

    engine = get_engine()

    with engine.connect() as connection:
        run = connection.execute(
            text("""
                SELECT
                    source_row_count,
                    valid_row_count,
                    rejected_row_count,
                    status,
                    error_message,
                    finished_at
                FROM ops.pipeline_runs
                WHERE batch_id = :batch_id
                ORDER BY run_id DESC
                LIMIT 1
            """),
            {
                "batch_id": batch_id
            },
        ).mappings().one()

    assert run["source_row_count"] == 5

    assert run["valid_row_count"] is None
    assert run["rejected_row_count"] is None

    assert run["status"] == "FAILED"

    assert (
        "simulated ingestion failure"
        in run["error_message"]
    )

    assert run["finished_at"] is not None