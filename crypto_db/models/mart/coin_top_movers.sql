with delta as
(
    select asset_id,name,current_price,prev_price,change_in_price,
    percentage_change_in_price,data_pull_timestamp,ingestion_timestamp
    from {{ref('coin_price_momentum')}}   where change_in_price != 0  
)
  select 
  asset_id,name,current_price,prev_price,change_in_price,percentage_change_in_price,
  case when change_in_price > 0 then 'Gain' else 'Lose' end as Gain_Lose,
  row_number() over
  (partition by case when change_in_price > 0 then 'Gain' else 'Lose' end 
   order by abs(percentage_change_in_price) desc) as Position,
  data_pull_timestamp,ingestion_timestamp
  from delta