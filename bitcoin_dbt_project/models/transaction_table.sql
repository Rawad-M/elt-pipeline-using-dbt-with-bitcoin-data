-- Controls how DBT writes new BITCOIN transaction data to the table
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'HASH_KEY'
  )
}}

SELECT *
FROM {{ source('raw_btc_source', 'BTC_TRANSACTIONS') }}

-- This is needed so dbt knows which rows are new, otherwise it will append everything again from staging source.
-- It tells DBT what data is new from staging source.
-- Outcome is oOnly pull rows from staging that are newer than what’s already in this table.
    {% if is_incremental() %}
WHERE BLOCK_TIMESTAMP
    >
    (select max (BLOCK_TIMESTAMP) FROM {{ this }})
    {% endif %}