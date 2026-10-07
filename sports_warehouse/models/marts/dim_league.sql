select distinct
    md5(league) as league_id,
    league as league_name
from {{ ref('stg_events') }}