SELECT
    asset_id,
    symbol,
    name,
    CAST(price_usd AS NUMERIC) AS price_usd,
    CAST(market_cap_usd AS NUMERIC) AS market_cap_usd,
    CAST(volume_usd_24hr AS NUMERIC) AS volume_usd_24hr,
    CAST(rank AS INTEGER) AS rank,
    CAST(change_percent_24hr AS NUMERIC) AS change_percent_24hr,
    CAST(vwap_24hr AS NUMERIC) AS vwap_24hr,
    DATE_TRUNC('second',TO_TIMESTAMP(data_pull_timestamp::bigint/1000.0)) AT TIME ZONE 'UTC' as  data_pull_timestamp,
    TO_TIMESTAMP(ingestion_timestamp,'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC' as ingestion_timestamp
FROM {{ source('raw', 'assets') }} 
