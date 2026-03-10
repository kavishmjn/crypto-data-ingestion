with price_data as 
(
    select asset_id,name,ingestion_timestamp::date as day,
    max(ingestion_timestamp) as latest_ingestion_timestamp
    from {{ref('assets_stg')}}
    group by asset_id,name,ingestion_timestamp::date 
),
ohlc as
(
    select asset_id,observation_day,
    open,high,low,close,range,pct_change
    from {{ref('coin_daily_price_range_analysis')}}
),
average as 
(
  select asset_id,ingestion_timestamp,
  avg_price_1_day,avg_price_3_day,avg_price_7_day
  from {{ref('coin_trend_metrics')}}
),
volatility as
(
    select asset_id,ingestion_timestamp,
    volatility_1d,volatility_3d,volatility_7d  
    from {{ref('coin_price_volatility_metric')}} 
)
select t1.asset_id,t1.name,t1.day,t1.latest_ingestion_timestamp,
t2.open,t2.high,t2.low,t2.close,t2.range,t2.pct_change,
t4.avg_price_1_day,t4.avg_price_3_day,t4.avg_price_7_day,
t3.volatility_1d,t3.volatility_3d,t3.volatility_7d 
from price_data t1 
inner join ohlc t2 on t1.asset_id = t2.asset_id and t1.day = t2.observation_day
inner join average t4 on t1.asset_id = t4.asset_id and t1.latest_ingestion_timestamp = t4.ingestion_timestamp
inner join volatility t3 on t1.asset_id=t3.asset_id and t1.latest_ingestion_timestamp = t3.ingestion_timestamp