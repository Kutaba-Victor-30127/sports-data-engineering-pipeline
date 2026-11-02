import pandas as pd
from src.contracts.event_contract import (
    ODDS_COLUMNS,
    REQUIRED_COLUMNS,
)

def validate_schema(df):
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
         raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def partition_events(df):
    validate_schema(df)

    event_times = pd.to_datetime(
        df["event_time"],
        format="ISO8601",
        errors="coerce",
        utc=True,
    )

    source_versions = pd.to_datetime(
        df["source_updated_at"],
        format="ISO8601",
        errors="coerce",
        utc=True,
    )

    numeric_odds = {
        column: pd.to_numeric(
            df[column],
            errors="coerce",
        )
        for column in ODDS_COLUMNS
    }

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="object"
    )

    def add_reason(mask, reason):
        current = reasons.loc[mask]

        reasons.loc[mask] = (
            current.where(
                current.eq(""),
                current + "; "
            )
            + reason
        )

    add_reason(
        df["event_id"].isnull(),
        "event_id cannot be null",
    )

    duplicate_event_ids = (
        df["event_id"].notnull()
        & df["event_id"].duplicated(keep=False)
    )

    add_reason(
        duplicate_event_ids,
        "event_id must be unique within batch",
    )

    add_reason(
        df["sport"].isnull(),
        "sport cannot be null",
    )

    add_reason(
        df["league"].isnull(),
        "league cannot be null",
    )

    add_reason(
        df["home_team"].isnull(),
        "home_team cannot be null",
    )

    add_reason(
        df["away_team"].isnull(),
        "away_team cannot be null",
    )

    same_team = (
        df["home_team"].notnull()
        & df["away_team"].notnull()
        & df["home_team"].eq(df["away_team"])
    )

    add_reason(
        same_team,
        "home_team and away_team cannot be the same",
    )

    add_reason(
        event_times.isna(),
        "event_time cannot be null or invalid",
    )

    for column, values in numeric_odds.items():
        invalid = (
            values.isna()
            | values.le(0)
            | values.isin([
                float("inf"),
                float("-inf"),
            ])
        )

        add_reason(
            invalid,
            f"{column} must be positive",
        )

    add_reason(
        source_versions.isna(),
        "source_updated_at cannot be null or invalid",
    )

    valid_mask = reasons.eq("")

    valid_df = df.loc[valid_mask].copy()

    valid_df["event_time"] = (
        event_times.loc[valid_mask]
        .dt.tz_localize(None)
    )

    valid_df["source_updated_at"] = (
        source_versions.loc[valid_mask]
    )

    for column, values in numeric_odds.items():
        valid_df[column] = values.loc[
            valid_mask
        ]

    rejected_df = df.loc[~valid_mask].copy()
    rejected_df["rejection_reason"] = reasons.loc[
        ~valid_mask
    ]

    return valid_df, rejected_df


def validate_events(df):
    valid_df, rejected_df = partition_events(df)

    if not rejected_df.empty:
        raise ValueError(
            rejected_df.iloc[0]["rejection_reason"]
        )

    print("Data validation passed.")

    return valid_df