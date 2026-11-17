select
    event_id,
    source_updated_at,
    count(*) as version_count

from {{ ref('fact_event_odds_history') }}

group by
    event_id,
    source_updated_at

having count(*) > 1