-- Explodes json values from transaction table 'outputs' column into it's own rows
-- LATERAL FLATTEN(input => outputs) is a Snowflake function that:
-- a) Explodes the JSON array in the outputs field into multiple rows
-- b) Creates one row per output in the transaction
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append'
  )
}}

WITH flattened_outputs AS (
    SELECT
        tx.hash_key,
        tx.block_number,
        tx.block_timestamp,
        tx.fee,
        tx.input_value,
        tx.output_value,
        tx.is_coinbase,
        f.value:address::STRING as flat_address,
        f.value:value::FLOAT as flat_value
    FROM {{ ref('stg1_transaction_table') }} tx,  --  is a DBT macro that references translation_table model

        LATERAL FLATTEN(input => outputs) f

    WHERE f.value:address is not null

    {% if is_incremental() %}
        AND tx.block_timestamp >= (SELECT MAX(block_timestamp) FROM {{ this }})
    {% endif %}
)
SELECT
    hash_key,
    block_number,
    block_timestamp,
    fee,
    input_value,
    output_value,
    is_coinbase
FROM flattened_outputs

-- Expected output
--{
--  "hash": "1224a6b54b86d5c8868a00037a6ed56a276596707a303fa87e2cbb54370edf1a",
--  "block_number": 932727,
--  "block_timestamp": "2026-01-18 00:31:47.000",
--  "fee" : "0.000454",
--  "input_value": "0.01426153",
--  "output_value", "0.01380753",
--  "outputs": [
--    {
--      "address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
--      "value": 0.00500000
--    },
--    {
--      "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
--      "value": 0.00250000
--    }
--  ]
--}