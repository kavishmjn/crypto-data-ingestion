select 
count(distinct Gain_Lose) as category_count
from {{ ref('coin_top_movers') }}
having count(distinct Gain_Lose) < 2