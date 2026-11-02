import logging

from src.aws.s3_client import (
    create_batch_metadata,
    upload_events_batch,
)
from src.database.connection import get_engine
from src.ingestion.load_events import (
    load_events,
    load_to_staging,
)
from src.observability.pipeline_runs import (
    complete_pipeline_run,
    fail_pipeline_run,
    record_pipeline_run_counts,
    start_pipeline_run,
)


logger = logging.getLogger(__name__)


def run_batch(batch=None):
    upload_needed = batch is None

    if upload_needed:
        batch = create_batch_metadata()

    batch_id = batch["batch_id"]
    source_file = batch["csv_key"]

    run_id = start_pipeline_run(
        batch_id=batch_id,
        source_file=source_file,
    )

    def save_counts(**counts):
        record_pipeline_run_counts(
            run_id=run_id,
            **counts,
        )

    try:
        if upload_needed:
            upload_events_batch(
                batch_id=batch_id,
                ingestion_date=batch["ingestion_date"],
            )

        logger.info(
            "Processing run %s for batch %s from %s",
            run_id,
            batch_id,
            source_file,
        )

        valid_df, rejected_df = load_events(
            s3_key=source_file,
            batch_id=batch_id,
            on_counts=save_counts,
        )

        with get_engine().begin() as connection:
            load_metrics = load_to_staging(
                valid_df,
                connection=connection,
            )

            complete_pipeline_run(
                run_id=run_id,
                source_row_count=(
                    len(valid_df) + len(rejected_df)
                ),
                valid_row_count=len(valid_df),
                rejected_row_count=len(rejected_df),
                inserted_count=load_metrics[
                    "inserted_count"
                ],
                updated_count=load_metrics[
                    "updated_count"
                ],
                ignored_count=load_metrics[
                    "ignored_count"
                ],
                connection=connection,
            )

        logger.info(
            "Run %s completed for batch %s.",
            run_id,
            batch_id,
        )

        return batch

    except Exception as exc:
        logger.exception(
            "Run %s failed for batch %s.",
            run_id,
            batch_id,
        )

        try:
            fail_pipeline_run(
                run_id=run_id,
                error_message=exc,
            )
        except Exception:
            logger.exception(
                "Could not record failure for run %s.",
                run_id,
            )

        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    run_batch()