REQUIRED_COLUMNS = [
    "event_id",
    "sport",
    "league",
    "home_team",
    "away_team",
    "event_time",
    "home_odds",
    "draw_odds",
    "away_odds",
    "source_updated_at"
]

def validate_events(df):
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
         raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    if df["event_id"].isnull().any():
        raise ValueError("event_id cannot be null")

    if df["event_id"].duplicated().any():
        raise ValueError("event_id must be unique")

    if df["home_team"].isnull().any():
        raise ValueError("home_team cannot be null")

    if df["away_team"].isnull().any():
        raise ValueError("away_team cannot be null")
    
    if (df["home_odds"] <= 0).any():
        raise ValueError("home_odds must be positive")

    if (df["draw_odds"] <= 0).any():
        raise ValueError("draw_odds must be positive")

    if (df["away_odds"] <= 0).any():
        raise ValueError("away_odds must be positive")

    if df["source_updated_at"].isnull().any():
        raise ValueError("source_updated_at cannot be null")

    print("Data validation passed.")