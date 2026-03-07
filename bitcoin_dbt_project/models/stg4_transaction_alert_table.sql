-- Find all bitcoin transaction to an address that are of greater value the 100 bitcoin.

{{
  config(
    materialized = 'view'
  )
}}

SELECT
    flat_address AS receiver_address,
    SUM(flat_value) AS total_bitcoins,
    COUNT(hash_key) AS total_bitcoin_transactions,
FROM {{ ref('stg3_none_coinbase_transaction_table') }}
GROUP BY flat_address
HAVING total_bitcoins > 100
ORDER BY total_bitcoins DESC