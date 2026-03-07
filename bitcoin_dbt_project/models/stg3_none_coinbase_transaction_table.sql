-- Creates an ephemeral that doesn't actually exist in your database
-- Get actual transactions as opposed to transactions to bitcoin miner using the is_coinbase flag.

{{
  config(
    materialized = 'ephemeral',
    incremental_strategy = 'append'
  )
}}

SELECT * FROM {{ ref('stg2_transaction_flattened_table') }} WHERE is_coinbase = FALSE

-- Table columns
--    hash_key,
--    block_number,
--    block_timestamp,
--    fee,
--    input_value,
--    output_value,
--    is_coinbase,
--    flat_address,
--    flat_value