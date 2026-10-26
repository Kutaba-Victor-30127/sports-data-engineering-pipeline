import logging

from src.aws.s3_client import upload_events_batch
from src.ingestion.load_events import load_events, load_to_staging

logger = logging.getLogger(__name__)


def run_batch():

    batch = upload_events_batch()

    logger.info(
        "Processing batch %s from %s",
        batch["batch_id"],
        batch["csv_key"]
    )   

    events = load_events(
        s3_key=batch["csv_key"],
        batch_id=batch["batch_id"]
    )

    load_to_staging(events)

    logger.info(
        "Batch %s completed successfully.",
        batch["batch_id"]
    )

    return batch

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    run_batch()