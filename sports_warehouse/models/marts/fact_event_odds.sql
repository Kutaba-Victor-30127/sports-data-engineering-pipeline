select
    event_id,

    to_char(event_time::date, 'YYYYMMDD')::integer as date_id,

    md5(league) as league_id,

    md5(home_team) as home_team_id,

    md5(away_team) as away_team_id,

    event_time,

    home_odds,
    draw_odds,
    away_odds,

    source_file,
    batch_id,
    ingested_at,
    source_updated_at,
    loaded_at

from {{ ref('stg_events') }}