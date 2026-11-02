EVENT_CONTRACT = {
    "event_id": {
        "type": "integer",
        "nullable": False,
    },
    "sport": {
        "type": "string",
        "nullable": False,
    },
    "league": {
        "type": "string",
        "nullable": False,
    },
    "home_team": {
        "type": "string",
        "nullable": False,
    },
    "away_team": {
        "type": "string",
        "nullable": False,
    },
    "event_time": {
        "type": "timestamp",
        "nullable": False,
    },
    "home_odds": {
        "type": "number",
        "nullable": False,
        "positive": True,
    },
    "draw_odds": {
        "type": "number",
        "nullable": False,
        "positive": True,
    },
    "away_odds": {
        "type": "number",
        "nullable": False,
        "positive": True,
    },
    "source_updated_at": {
        "type": "timestamp",
        "nullable": False,
    },
}


REQUIRED_COLUMNS = tuple(
    EVENT_CONTRACT.keys()
)


ODDS_COLUMNS = tuple(
    column
    for column, rules in EVENT_CONTRACT.items()
    if rules.get("positive") is True
)