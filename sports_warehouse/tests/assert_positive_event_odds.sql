select
    event_id
from {{ ref('fact_event_odds') }}
where home_odds is null
   or draw_odds is null
   or away_odds is null
   or home_odds <= 0
   or draw_odds <= 0
   or away_odds <= 0