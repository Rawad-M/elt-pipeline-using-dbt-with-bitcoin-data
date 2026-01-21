# Test DBT connection to Snowflake
dbt debug

# Test DBT creation of test model
dbt clean
dbt compile --select 1_btc_transaction_table
dbt run --select 1_btc_transaction_table

# Check BTC_TRANSACTIONS table freshness via DBT
dbt source freshness

