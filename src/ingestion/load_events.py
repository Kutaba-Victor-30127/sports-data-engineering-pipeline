import argparse
import logging
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    func,
    or_,
    select,
    text,
)

from sqlalchemy.dialects.postgresql import insert

from src.aws.s3_client import (
    read_file_from_s3,
    upload_text_to_s3,
)
from src.database.connection import get_engine
from src.validation.event_validator import partition_events


logger = logging.getLogger(__name__)


def load_events(
    s3_key,
    batch_id,
    on_counts=None
):
    file_content = read_file_from_s3(s3_key)

    df = pd.read_csv(
    BytesIO(file_content)
    )

    if on_counts is not None:
        on_counts(
            source_row_count=len(df)
        )

    logger.info(
        "Loaded %s events from S3.",
        len(df),
    )

    df["source_file"] = s3_key
    df["batch_id"] = batch_id
    df["ingested_at"] = datetime.now(timezone.utc)

    valid_df, rejected_df = partition_events(df)

    if on_counts is not None:
        on_counts(
            source_row_count=len(df),
            valid_row_count=len(valid_df),
            rejected_row_count=len(rejected_df),
        )

    logger.info(
        "Validation result: %s valid, %s rejected.",
        len(valid_df),
        len(rejected_df),
    )

    if not rejected_df.empty:
        batch_path = (
            s3_key
            .removeprefix("raw/events/")
            .rsplit("/", 1)[0]
        )

        rejected_key = (
            f"rejected/events/{batch_path}/rejected.json"
        )

        rejected_json = rejected_df.to_json(
            orient="records",
            date_format="iso",
            indent=2,
        )

        upload_text_to_s3(
            rejected_json,
            rejected_key
        )

        logger.warning(
            "%s rejected records written to %s.",
            len(rejected_df),
            rejected_key,
        )

    logger.info(
        "Prepared batch %s from %s.",
        batch_id,
        s3_key,
    )

    return valid_df, rejected_df


def load_to_staging(
    df,
    connection=None
    ):
    if df.empty:
        logger.warning(
            "No valid records to load into staging."
        )

        return {
            "inserted_count": 0,
            "updated_count": 0,
            "ignored_count": 0,
        }

    if connection is None:
        with get_engine().begin() as connection:
            return load_to_staging(
                df,
                connection,
            )

    metadata = MetaData()

    raw_events = Table(
        "raw_events",
        metadata,
        schema="staging",
        autoload_with=connection,
    )

    records = df.to_dict(orient="records")

    insert_stmt = insert(raw_events)

    source_is_newer = or_(
        raw_events.c.source_updated_at.is_(None),
        insert_stmt.excluded.source_updated_at
        > raw_events.c.source_updated_at,
    )

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[raw_events.c.event_id],
        set_={
            "sport": insert_stmt.excluded.sport,
            "league": insert_stmt.excluded.league,
            "home_team": insert_stmt.excluded.home_team,
            "away_team": insert_stmt.excluded.away_team,
            "event_time": insert_stmt.excluded.event_time,
            "home_odds": insert_stmt.excluded.home_odds,
            "draw_odds": insert_stmt.excluded.draw_odds,
            "away_odds": insert_stmt.excluded.away_odds,
            "source_file": insert_stmt.excluded.source_file,
            "batch_id": insert_stmt.excluded.batch_id,
            "ingested_at": insert_stmt.excluded.ingested_at,
            "source_updated_at": insert_stmt.excluded.source_updated_at,
        },
        where=source_is_newer,
    )

    connection.execute(
        text(
            "LOCK TABLE staging.raw_events "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    )

    existing_rows = connection.execute(
        select(
            raw_events.c.event_id,
            raw_events.c.source_updated_at,
        ).where(
            raw_events.c.event_id.in_(
                df["event_id"].tolist()
            )
        )
    ).all()

    existing_versions = {
        row.event_id: row.source_updated_at
        for row in existing_rows
    }

    inserted_count = 0
    updated_count = 0

    for record in records:
        event_id = record["event_id"]

        incoming_version = pd.Timestamp(
            record["source_updated_at"]
        )

        if event_id not in existing_versions:
            inserted_count += 1
            continue

        existing_version = existing_versions[
            event_id
        ]

        if (
            existing_version is None
            or incoming_version
            > pd.Timestamp(existing_version)
        ):
            updated_count += 1

    connection.execute(
        upsert_stmt,
        records,
    )

    ignored_count = (
        len(records)
        - inserted_count
        - updated_count
    )

    count = connection.execute(
        select(func.count()).select_from(
            raw_events
        )
    ).scalar_one()

    logger.info(
        "Inserted: %s, updated: %s, ignored: %s.",
        inserted_count,
        updated_count,
        ignored_count,
    )

    logger.info(
        "Total rows in staging.raw_events: %s",
        count,
    )

    logger.info(
        "Batch processed successfully."
    )

    return {
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "ignored_count": ignored_count,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load a specific S3 event batch into PostgreSQL."
    )

    parser.add_argument(
        "--s3-key",
        dest="s3_key",
        required=True,
        help="S3 key of the CSV batch to process.",
    )

    parser.add_argument(
        "--batch-id",
        dest="batch_id",
        required=True,
        help="Unique identifier of the ingestion batch.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    args = parse_args()

    from src.ingestion.run_batch import run_batch

    run_batch(
        batch={
            "batch_id": args.batch_id,
            "csv_key": args.s3_key,
        }
    )