from sqlalchemy import text

from contextlib import nullcontext

from src.database.connection import get_engine


def start_pipeline_run(batch_id, source_file):
    engine = get_engine()

    with engine.begin() as connection:
        result = connection.execute(
            text("""
                INSERT INTO ops.pipeline_runs (
                    batch_id,
                    source_file,
                    status
                )
                VALUES (
                    :batch_id,
                    :source_file,
                    'RUNNING'
                )
                RETURNING run_id
            """),
            {
                "batch_id": batch_id,
                "source_file": source_file,
            },
        )

        return result.scalar_one()


def record_pipeline_run_counts(
    run_id,
    source_row_count,
    valid_row_count=None,
    rejected_row_count=None,
):
    with get_engine().begin() as connection:
        connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    source_row_count = :source_row_count,
                    valid_row_count = :valid_row_count,
                    rejected_row_count = :rejected_row_count
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "source_row_count": source_row_count,
                "valid_row_count": valid_row_count,
                "rejected_row_count": rejected_row_count,
            },
        )


def complete_pipeline_run(
    run_id,
    source_row_count,
    valid_row_count,
    rejected_row_count,
    inserted_count,
    updated_count,
    ignored_count,
    connection=None,
):
    context = (
        nullcontext(connection)
        if connection is not None
        else get_engine().begin()
    )

    with context as connection:
        connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    source_row_count = :source_row_count,
                    valid_row_count = :valid_row_count,
                    rejected_row_count = :rejected_row_count,
                    inserted_count = :inserted_count,
                    updated_count = :updated_count,
                    ignored_count = :ignored_count,
                    finished_at = NOW(),
                    status = 'SUCCESS',
                    error_message = NULL
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "source_row_count": source_row_count,
                "valid_row_count": valid_row_count,
                "rejected_row_count": rejected_row_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "ignored_count": ignored_count,
            },
        )



def fail_pipeline_run(run_id, error_message):
    with get_engine().begin() as connection:
        connection.execute(
            text("""
                UPDATE ops.pipeline_runs
                SET
                    finished_at = NOW(),
                    status = 'FAILED',
                    error_message = :error_message
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "error_message": str(error_message),
            },
        )