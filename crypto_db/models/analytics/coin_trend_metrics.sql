
with price_data as
(
    select asset_id,name,
    price_usd as price,
    ingestion_timestamp
    from {{ref('assets_stg')}}
)
select t1.asset_id,t1.name,t1.price,
avg(case when t2.ingestion_timestamp>= t1.ingestion_timestamp - interval '1 day' then t2.price end) as avg_price_1_day,
avg(case when t2.ingestion_timestamp>= t1.ingestion_timestamp - interval '3 days' then t2.price end) as avg_price_3_day,
avg(case when t2.ingestion_timestamp>= t1.ingestion_timestamp - interval '7 days' then t2.price end) as avg_price_7_day,
t1.ingestion_timestamp
from price_data t1 inner join price_data t2
on t1.asset_id = t2.asset_id 
and t2.ingestion_timestamp between t1.ingestion_timestamp - interval '7 days' and t1.ingestion_timestamp 
group by t1.asset_id,t1.name,t1.price,t1.ingestion_timestamp