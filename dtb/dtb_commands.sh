# DBT Set-up
pip install --upgrade pip
pip install dbt-core==1.9.4
pip install dbt-snowflake==1.9.4

# DTB check version
dbt --version

# DBT Initial project set-up
dbt init

# Test DBT connection to Snowflake
dbt debug

# Test DBT creation of BTC transaction model
dbt clean
dbt compile --select transaction_table
dbt run --select transaction_table

# Check BTC_TRANSACTIONS table freshness via DBT
dbt source freshness
dbt source freshness --debug

# Run first DBT test
dbt test

# Full table without using incremental
dbt run --select transaction_table -- full-referesh

# Run models
dbt run -m stg1_transaction_table
dbt run -m stg2_transaction_payment_table