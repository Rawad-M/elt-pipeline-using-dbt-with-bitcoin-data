-- Step 1
-- Create the source table for Bitcoin transactions to be used by DBT
CREATE
OR REPLACE TABLE BTC_DATABASE.BTC_SCHEMA.BTC (
  HASH_KEY VARCHAR,
  BLOCK_HASH VARCHAR,
  BLOCK_NUMBER INT,
  BLOCK_TIMESTAMP TIMESTAMP,
  FEE FLOAT,
  INPUT_VALUE FLOAT,
  OUTPUT_VALUE FLOAT,
  FEE_PER_BYTE FLOAT,
  IS_COINBASE BOOLEAN,
  OUTPUTS VARIANT
);

-- Step 2
-- Copy data from stage into the BTC table, with pattern filter for file names for the daily transactions
COPY INTO BTC_DATABASE.BTC_SCHEMA.BTC
    FROM (
    SELECT
    t.$1:hash AS hashkey,
    t.$1:block_hash,
    t.$1:block_number,
    t.$1:block_timestamp,
    t.$1:fee,
    t.$1:input_value,
    t.$1:output_value AS output_btc,
    ROUND(t.$1:fee / t.$1: size, 12) AS fee_per_byte,
    t.$1:is_coinbase,
    t.$1:outputs
    FROM @BTC_DATABASE.BTC_SCHEMA.BTC_STAGE/transactions t
    )
    PATTERN = '.*/[0-9]{6,7}[.]snappy[.]parquet';


-- Step 3
-- Create a Snowflake Task to copy Bitcoin data every 1 hour
CREATE
OR REPLACE TASK BTC_DATABASE.BTC_SCHEMA.BTC_LOAD_TASK
  WAREHOUSE = LARGE_WH
  SCHEDULE = '1 HOUR'
AS
COPY INTO BTC_DATABASE.BTC_SCHEMA.BTC
    FROM (
        SELECT
        t.$1:hash AS hashkey,
        t.$1:block_hash,
        t.$1:block_number,
        t.$1:block_timestamp,
        t.$1:fee,
        t.$1:input_value,
        t.$1:output_value AS output_btc,
        ROUND(t.$1:fee / t.$1: size, 12) AS fee_per_byte,
        t.$1:is_coinbase,
        t.$1:outputs
        FROM @BTC_DATABASE.BTC_SCHEMA.BTC_STAGE/transactions t
    )
    PATTERN = '.*/[0-9]{6,7}[.]snappy[.]parquet';
