-- Query transaction data for a given date from staging table
SELECT *
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2026-01-01 t;

-- Query transaction data for a given date for all columns
SELECT t.$1
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2026-01-01 t;

-- Query transaction data for a given date for a set of columns
SELECT t.$1:hash
    t.$1:block_hash, t.$1:block_number, t.$1:block_timestamp, t.$1:fee,
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2026-01-01 t;

-- Query transaction data for a given date for a set of columns with fee rounded
SELECT t.$1:hash AS hashkey, t.$1:block_hash, t.$1:block_number, t.$1:block_timestamp, t.$1:fee, t.$1:input_value, t.$1:output_value AS output_btc, ROUND(t.$1:fee / t.$1:size, 12) AS fee_per_byte,
       t.$1:is_coinbase, t.$1:outputs
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2026-01-01 t;
