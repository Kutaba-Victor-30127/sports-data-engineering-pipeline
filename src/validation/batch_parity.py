import pandas as pd

from pandas.testing import assert_frame_equal

from src.contracts.event_contract import (
    REQUIRED_COLUMNS,
    TIMESTAMP_COLUMNS,
)


def canonicalize_events(df):
    canonical_df = (
        df[list(REQUIRED_COLUMNS)]
        .copy()
    )

    for column in TIMESTAMP_COLUMNS:
        canonical_df[column] = pd.to_datetime(
            canonical_df[column],
            errors= "coerce",
            utc=True,
        )

        if column =="event_time":
            canonical_df[column] = (
                canonical_df[column]
                .dt.tz_localize(None)
            )

    return (
        canonical_df
        .sort_values("event_id")
        .reset_index(drop=True)
    )


def validate_batch_parity(
    csv_df,
    json_df,
):
    csv_events = canonicalize_events(csv_df)
    json_events = canonicalize_events(json_df)

    try: 
        assert_frame_equal(
            csv_events,
            json_events,
            check_dtype=False,
            check_exact=False,
            rtol=1e-9,
            atol=1e-9,
        )
    
    except AssertionError as exc:
        raise ValueError(
            "CSV/JSON batch parity check failed: "
            f"{exc}"
        ) from exc