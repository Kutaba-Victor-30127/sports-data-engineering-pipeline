import argparse
import logging
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from sqlalchemy import MetaData, Table, func, or_, select
from sqlalchemy.dialects.postgresql import insert

from src.aws.s3_client import read_file_from_s3
from src.database.connection import get_engine
from src.validation.event_validator import validate_events


logger = logging.getLogger(__name__)


def load_events(s3_key, batch_id):
    file_content = read_file_from_s3(s3_key)

    df = pd.read_csv(
        BytesIO(file_content),
        parse_dates=[
            "event_time",
            "source_updated_at",
        ],
    )

    logger.info(
        "Loaded %s events from S3.",
        len(df),
    )

    validate_events(df)

    df["source_file"] = s3_key
    df["batch_id"] = batch_id
    df["ingested_at"] = datetime.now(timezone.utc)

    logger.info(
        "Prepared batch %s from %s.",
        batch_id,
        s3_key,
    )

    return df


def load_to_staging(df):
    engine = get_engine()

    metadata = MetaData()

    raw_events = Table(
        "raw_events",
        metadata,
        schema="staging",
        autoload_with=engine,
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

    with engine.begin() as connection:
        result = connection.execute(
            upsert_stmt,
            records,
        )

        count = connection.execute(
            select(func.count()).select_from(raw_events)
        ).scalar_one()

        logger.info(
            "Rows inserted or updated: %s",
            result.rowcount,
        )

        logger.info(
            "Total rows in staging.raw_events: %s",
            count,
        )

    logger.info("Batch processed successfully.")


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

    events = load_events(
        s3_key=args.s3_key,
        batch_id=args.batch_id,
    )

    load_to_staging(events)