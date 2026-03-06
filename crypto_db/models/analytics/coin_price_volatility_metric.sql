WITH price_data AS (
    SELECT
        asset_id,name,price_usd,ingestion_timestamp,
        LAG(price_usd) OVER (PARTITION BY asset_id ORDER BY ingestion_timestamp) AS prev_price
    FROM {{ ref('assets_stg') }}
),
returns AS (
    SELECT
        asset_id,name,ingestion_timestamp, price_usd,
        (price_usd - prev_price) / prev_price AS return
    FROM price_data
    WHERE prev_price IS NOT NULL
)
SELECT
    t1.asset_id,t1.name,t1.price_usd,t1.ingestion_timestamp,
    ROUND(STDDEV(CASE WHEN t2.ingestion_timestamp >= t1.ingestion_timestamp - INTERVAL '1 day'THEN t2.return  END)*100.0,2) AS volatility_1d,
    ROUND(STDDEV(CASE WHEN t2.ingestion_timestamp >= t1.ingestion_timestamp - INTERVAL '3 days'THEN t2.return  END)*100.0,2) AS volatility_3d,
    ROUND(STDDEV(t2.return)*100.0,2) AS volatility_7d
FROM returns t1 JOIN returns t2
    ON t1.asset_id = t2.asset_id 
   AND t2.ingestion_timestamp   BETWEEN t1.ingestion_timestamp - INTERVAL '7 days'  AND t1.ingestion_timestamp
GROUP BY  t1.asset_id,t1.name,t1.price_usd,t1.ingestion_timestamp
   