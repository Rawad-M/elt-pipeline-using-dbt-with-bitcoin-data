-- Step 1
-- Create the database
CREATE
DATABASE BTC_DATABASE;

-- Step 2
-- Create the schema within the BTC database
CREATE SCHEMA BTC_SCHEMA;

-- Step 3
-- Create or replace the external stage pointing to the Bitcoin public S3 bucket
-- This will contain the entire dataset
CREATE
OR REPLACE STAGE BTC_DATABASE.BTC_SCHEMA.BTC_STAGE
  URL = 's3://aws-public-blockchain/v1.0/btc/'
  FILE_FORMAT = (TYPE = PARQUET);

-- List files available in the stage
-- Will see files by date & blocks
-- Example: s3://aws-public-blockchain/v1.0/btc/transactions/date=<DATE>/<BLOCK>.snappy.parquet
LIST
@BTC_DATABASE.BTC_SCHEMA.BTC_STAGE;

