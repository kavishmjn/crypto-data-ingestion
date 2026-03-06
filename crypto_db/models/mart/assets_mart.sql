{{ config(schema='mart') }}

select *  from 
(
    select *,row_number() over (partition by asset_id order by ingestion_timestamp desc) row_number
    from {{ref('assets_stg')}}
)t
where row_number = 1
    