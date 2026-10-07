select
    event_id,
    lower(trim(sport)) as sport,
    trim(league) as league,
    trim(home_team) as home_team,
    trim(away_team) as away_team,
    event_time,
    home_odds,
    draw_odds,
    away_odds,
    loaded_at
from {{ source('staging', 'raw_events') }}