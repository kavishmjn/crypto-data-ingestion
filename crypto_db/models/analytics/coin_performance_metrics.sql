with closing_price as (
    select 
        asset_id,observation_day,
        close as closing_price
    from {{ ref('coin_daily_price_range_analysis') }}
),
closing_price_compare as (
    select 
        t1.asset_id,
        t1.observation_day,
        t1.closing_price,
        max(case when t2.observation_day = t1.observation_day - interval '1 day'  then t2.closing_price end) as closing_price_1_day,
        max(case when t2.observation_day = t1.observation_day - interval '3 days' then t2.closing_price end) as closing_price_3_day,
        max(case when t2.observation_day = t1.observation_day - interval '7 days' then t2.closing_price end) as closing_price_7_day,
        max(case when t2.observation_day = t1.observation_day - interval '15 days' then t2.closing_price end) as closing_price_15_day,
        max(case when t2.observation_day = t1.observation_day - interval '30 days' then t2.closing_price end) as closing_price_30_day
    from closing_price t1 inner join closing_price t2 
    on  t1.asset_id = t2.asset_id 
    and t1.observation_day - t2.observation_day  in (1, 3, 7, 15, 30)
    group by t1.asset_id,t1.observation_day,t1.closing_price
)
select
    asset_id,observation_day,closing_price,
    closing_price - closing_price_1_day  as abs_return_1_day,
    closing_price - closing_price_3_day  as abs_return_3_day,
    closing_price - closing_price_7_day  as abs_return_7_day,
    closing_price - closing_price_15_day as abs_return_15_day,
    closing_price - closing_price_30_day as abs_return_30_day,
    round((closing_price - closing_price_1_day)  / nullif(closing_price_1_day,  0) * 100, 4) as pct_return_1_day,
    round((closing_price - closing_price_3_day)  / nullif(closing_price_3_day,  0) * 100, 4) as pct_return_3_day,
    round((closing_price - closing_price_7_day)  / nullif(closing_price_7_day,  0) * 100, 4) as pct_return_7_day,
    round((closing_price - closing_price_15_day) / nullif(closing_price_15_day, 0) * 100, 4) as pct_return_15_day,
    round((closing_price - closing_price_30_day) / nullif(closing_price_30_day, 0) * 100, 4) as pct_return_30_day
from closing_price_compare

