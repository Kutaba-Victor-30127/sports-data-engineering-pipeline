import argparse
import time

import pandas as pd
from sqlalchemy import text

from src.database.connection import get_engine
from src.ingestion.load_events import load_to_staging


def create_dataset(
    row_count,
    batch_id,
    source_updated_at,
):
    return pd.DataFrame({
        "event_id": range(1, row_count + 1),

        "sport": ["football"] * row_count,

        "league": [
            f"League {(i % 10) + 1}"
            for i in range(row_count)
        ],

        "home_team": [
            f"Home Team {i}"
            for i in range(1, row_count + 1)
        ],

        "away_team": [
            f"Away Team {i}"
            for i in range(1, row_count + 1)
        ],

        "event_time": pd.date_range(
            start="2026-01-01 00:00:00",
            periods=row_count,
            freq="min"
        ),

        "home_odds": [1.95] * row_count,
        "draw_odds": [3.40] * row_count,
        "away_odds": [4.20] * row_count,

        "source_file": [
            f"benchmark/{batch_id}/events.csv"
        ] * row_count,

        "batch_id": [batch_id] * row_count,

        "ingested_at": [
            pd.Timestamp.now(tz="UTC")
        ] * row_count,

        "source_updated_at": [
            pd.Timestamp(source_updated_at)
        ] * row_count,
    })


def measure_load(
    name,
    df,
    connection,
):
    start = time.perf_counter()

    metrics = load_to_staging(
        df,
        connection=connection,
    )

    elapsed = time.perf_counter() - start

    rows_per_second = (
        len(df) / elapsed
        if elapsed > 0
        else 0
    )

    result = {
        "scenario": name,
        "rows": len(df),
        "seconds": elapsed,
        "rows_per_second": rows_per_second,
        **metrics,
    }

    return result


def run_benchmark(row_count):
    engine = get_engine()

    initial_df = create_dataset(
        row_count=row_count,
        batch_id=f"benchmark-{row_count}-initial",
        source_updated_at="2026-11-03T08:00:00Z",
    )

    updated_df = initial_df.copy()

    update_count = max(
        1,
        int(row_count * 0.10),
    )

    updated_df.loc[
        updated_df.index[:update_count],
        "home_odds"
    ] = 2.05

    updated_df.loc[
        updated_df.index[:update_count],
        "source_updated_at"
    ] = pd.Timestamp(
        "2026-11-03T09:00:00Z"
    )

    updated_df["batch_id"] = (
        f"benchmark-{row_count}-update"
    )

    updated_df["source_file"] = (
        f"benchmark/"
        f"benchmark-{row_count}-update/"
        f"events.csv"
    )

    results = []

    with engine.connect() as connection:
        transaction = connection.begin()

        try:
            connection.execute(
                text(
                    "TRUNCATE TABLE "
                    "staging.raw_events"
                )
            )

            results.append(
                measure_load(
                    "initial_load",
                    initial_df,
                    connection,
                )
            )

            results.append(
                measure_load(
                    "idempotent_replay",
                    initial_df,
                    connection,
                )
            )

            results.append(
                measure_load(
                    "ten_percent_update",
                    updated_df,
                    connection,
                )
            )

        finally:
            transaction.rollback()

    return results


def print_results(results):
    print()

    print(
        f"{'Scenario':<22}"
        f"{'Rows':>12}"
        f"{'Seconds':>12}"
        f"{'Rows/sec':>14}"
        f"{'Inserted':>12}"
        f"{'Updated':>12}"
        f"{'Ignored':>12}"
    )

    print("-" * 96)

    for result in results:
        print(
            f"{result['scenario']:<22}"
            f"{result['rows']:>12,}"
            f"{result['seconds']:>12.3f}"
            f"{result['rows_per_second']:>14,.0f}"
            f"{result['inserted_count']:>12,}"
            f"{result['updated_count']:>12,}"
            f"{result['ignored_count']:>12,}"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark replay-safe PostgreSQL "
            "incremental ingestion."
        )
    )

    parser.add_argument(
        "--row-count",
        type=int,
        nargs="+",
        default=[10000],
        help=(
            "Dataset sizes to benchmark. "
            "Example: --row-count 10000 100000"
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    for row_count in args.row_count:
        print()
        print(
            f"Benchmarking {row_count:,} rows..."
        )

        try:
            benchmark_results = (
                run_benchmark(row_count)
            )

            print_results(
                benchmark_results
            )

        except Exception as exc:
            print(
                f"Benchmark failed for "
                f"{row_count:,} rows"
            )

            print(
                f"{type(exc).__name__}: {exc}"
            )
        