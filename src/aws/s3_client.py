import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

from datetime import datetime, timezone
from uuid import uuid4


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def upload_file_to_s3(local_file, s3_key):
    s3 = get_s3_client()

    s3.upload_file(
        str(local_file),
        S3_BUCKET_NAME,
        s3_key
    )

    logger.info(
        "Uploaded %s to s3://%s/%s",
        local_file,
        S3_BUCKET_NAME,
        s3_key
    )


def read_file_from_s3(s3_key):
    s3 = get_s3_client()

    response = s3.get_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key
    )

    logger.info(
        "Read s3://%s/%s",
        S3_BUCKET_NAME,
        s3_key
    )

    return response["Body"].read()


def upload_text_to_s3(content, s3_key, content_type="application/json"):
    s3 = get_s3_client()

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=content.encode("utf-8"),
        ContentType=content_type,
    )

    logger.info(
        "Uploaded quarantine data to s3://%s/%s",
        S3_BUCKET_NAME,
        s3_key,
    )


def create_batch_metadata(
    batch_id=None,
    ingestion_date=None
):
    batch_id = batch_id or str(uuid4())

    ingestion_date = (
        ingestion_date
        or datetime.now(timezone.utc).date().isoformat()
    )

    prefix = (
        f"raw/events/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}/"
    )

    return {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "csv_key": f"{prefix}events.csv",
        "json_key": f"{prefix}events.json",
    }


def upload_events_batch(batch_id=None, ingestion_date=None):
    batch = create_batch_metadata(
        batch_id=batch_id,
        ingestion_date=ingestion_date
    )

    batch_id = batch["batch_id"]
    csv_key = batch["csv_key"]
    json_key = batch["json_key"]

    events_csv = PROJECT_ROOT / "data" / "raw" / "events.csv"
    events_json = PROJECT_ROOT / "data" / "raw" / "events.json"

    upload_file_to_s3(
        events_csv,
        csv_key
    )

    upload_file_to_s3(
        events_json,
        json_key
    )

    logger.info(
        "Created immutable raw batch %s",
        batch_id
    )

    logger.info(
        "CSV key: %s",
        csv_key
    )

    logger.info(
        "JSON key: %s",
        json_key
    )

    return batch



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    batch = upload_events_batch()

    logger.info(
        "Batch ready for processing: %s",
        batch
    )