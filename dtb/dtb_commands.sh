# Test DBT connection to Snowflake
dbt debug

# Test DBT creation of BTC transaction model
dbt clean
dbt compile --select transaction_table
dbt run --select transaction_table

# Check BTC_TRANSACTIONS table freshness via DBT
dbt source freshness

