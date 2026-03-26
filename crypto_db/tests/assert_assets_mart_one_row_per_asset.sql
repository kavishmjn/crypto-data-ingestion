select 
asset_id, count(*) as row_count
from {{ ref('assets_mart') }}
group by asset_id
having count(*) > 1