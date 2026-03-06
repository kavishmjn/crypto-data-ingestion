with per_day_data as
(
    select asset_id,name,price_usd,
    date_trunc('day',ingestion_timestamp)::date as observation_day,
    ingestion_timestamp
    from {{ref('assets_stg')}}
),
data_ranked as
(
    select asset_id,name,price_usd,observation_day,
    row_number() over (partition by asset_id,observation_day order by  ingestion_timestamp asc) as fv,
    row_number() over (partition by asset_id,observation_day order by  ingestion_timestamp desc) as lv
    from per_day_data
),
ohlc as 
(
select asset_id,name,observation_day,
max(case when fv=1 then price_usd end) as open,
max(price_usd) as high,
min(price_usd) as low,
max(case when lv=1 then price_usd end) as close
from data_ranked group by asset_id,name,observation_day
)
select asset_id,name,observation_day
,open,high,low,close,
high-low as range,
round((close-open)*100.0/open,2) as pct_change
from ohlc