-- Find all bitcoin transaction to an address that are of greater value the 100 bitcoin & map these value to USD value.

{{
  config(
    materialized = 'view'
  )
}}

WITH LARGE_TRANSACTIONS AS (
    SELECT
        flat_address AS receiver_address,
        SUM(flat_value) AS total_bitcoins,
        COUNT(hash_key) AS total_bitcoin_transactions,
    FROM {{ ref('stg3_none_coinbase_transaction_table') }}
    GROUP BY flat_address
    HAVING total_bitcoins > 100
    ORDER BY total_bitcoins DESC
),
PRICE_USD_LATEST AS (
    SELECT
    	price
    FROM {{ ref('btc_usd_max')}}
    WHERE LEFT(snapped_at, 10)::date = DATE('2026-03-08') -- Change to current date current_date()
)

SELECT
    l.receiver_address,
    l.total_bitcoins,
    l.total_bitcoin_transactions,
    (p.price * l.total_bitcoins) AS total_bitcoins_usd
FROM LARGE_TRANSACTIONS l
    CROSS JOIN PRICE_USD_LATEST p