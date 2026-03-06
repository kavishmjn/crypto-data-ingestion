with previous_price as 
(
select 
    asset_id,
    name,
    price_usd as current_price,
    lag(price_usd) over (partition by asset_id order by ingestion_timestamp) as prev_price,
    data_pull_timestamp,
    ingestion_timestamp
    from {{ref('assets_stg')}}
)
select asset_id,name,current_price,prev_price,
current_price-prev_price as change_in_price,
(current_price-prev_price)*100.0/prev_price as percentage_change_in_price,
data_pull_timestamp,
ingestion_timestamp
from previous_price 

