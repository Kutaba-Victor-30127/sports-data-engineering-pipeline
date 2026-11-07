import pandas as pd

from src.contracts.event_contract import (
    NON_NULLABLE_COLUMNS,
    NUMBER_COLUMNS,
    ODDS_COLUMNS,
    REQUIRED_COLUMNS,
    TIMESTAMP_COLUMNS,
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

    converted_timestamps = {
        column: pd.to_datetime(
            df[column],
            format="ISO8601",
            errors="coerce",
            utc=True,
        )
        for column in TIMESTAMP_COLUMNS
    }

    converted_numbers = {
        column: pd.to_numeric(
            df[column],
            errors="coerce",
        )
        for column in NUMBER_COLUMNS
    }

    reasons = pd.Series(
        "",
        index=df.index,
        dtype="object",
    )

    def add_reason(mask, reason):
        current = reasons.loc[mask]

        reasons.loc[mask] = (
            current.where(
                current.eq(""),
                current + "; ",
            )
            + reason
        )

    for column in NON_NULLABLE_COLUMNS:
        if column in TIMESTAMP_COLUMNS:
            invalid = (
                converted_timestamps[column].isna()
            )

        elif column in NUMBER_COLUMNS:
            invalid = (
                converted_numbers[column].isna()
            )

        else:
            invalid = df[column].isnull()

        add_reason(
            invalid,
            f"{column} cannot be null or invalid",
        )

    duplicate_event_ids = (
        df["event_id"].notnull()
        & df["event_id"].duplicated(
            keep=False
        )
    )

    add_reason(
        duplicate_event_ids,
        "event_id must be unique within batch",
    )

    same_team = (
        df["home_team"].notnull()
        & df["away_team"].notnull()
        & df["home_team"].eq(
            df["away_team"]
        )
    )

    add_reason(
        same_team,
        "home_team and away_team cannot be the same",
    )

    for column in ODDS_COLUMNS:
        values = converted_numbers[column]

        invalid = (
            values.notna()
            & (
                values.le(0)
                | values.isin([
                    float("inf"),
                    float("-inf"),
                ])
            )
        )

        add_reason(
            invalid,
            f"{column} must be positive",
        )

    valid_mask = reasons.eq("")

    valid_df = df.loc[
        valid_mask
    ].copy()

    rejected_df = df.loc[
        ~valid_mask
    ].copy()

    for column in TIMESTAMP_COLUMNS:
        converted = (
            converted_timestamps[column]
            .loc[valid_mask]
        )

        if column == "event_time":
            converted = (
                converted.dt.tz_localize(None)
            )

        valid_df[column] = converted

    for column in NUMBER_COLUMNS:
        valid_df[column] = (
            converted_numbers[column]
            .loc[valid_mask]
        )

    rejected_df["rejection_reason"] = (
        reasons.loc[~valid_mask]
    )

    return valid_df, rejected_df


def validate_events(df):
    valid_df, rejected_df = (
        partition_events(df)
    )

    if not rejected_df.empty:
        raise ValueError(
            rejected_df.iloc[0][
                "rejection_reason"
            ]
        )

    print("Data validation passed.")

    return valid_df