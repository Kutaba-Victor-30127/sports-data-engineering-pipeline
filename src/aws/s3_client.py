import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    events_csv = PROJECT_ROOT / "data" / "raw" / "events.csv"
    events_json = PROJECT_ROOT / "data" / "raw" / "events.json"

    upload_file_to_s3(
        events_csv,
        "raw/events/events.csv"
    )

    upload_file_to_s3(
        events_json,
        "raw/events/events.json"
    )