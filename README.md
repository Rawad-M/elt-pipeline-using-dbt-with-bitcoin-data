# From ETL to ELT: Evaluating Data Integration Approaches and Building a Cloud-Based ELT Pipeline Using DBT

## Project Steps

1. Obtain Bitcoin Source Data from S3 Bucket
2. Ingest Source data into Snowflake
3. Transform via Data Build Tool (DBT)
4. Visualise via PowerBI

## Project Setup

### Git Repo:
- Name: elt-pipeline-using-dbt-with-bitcoin-data
- URL: https://github.com/Rawad-M/elt-pipeline-using-dbt-with-bitcoin-data

### Snowflake Account:

- Snowflake account email: rawad.i.malik@gmail.com
- Database: BTC_DATABASE
- Schema: BTC_SCHEMA
- Role: ACCOUNTADMIN
- Account/Server URL: EYBDADR-QZ09350.snowflakecomputing.com
- Warehouse: COMPUTE_WH

#### Snowflake MFA

### Python Set-up

```shell
python -m venv venv

venv\Scripts\activate
```

### DTB Set-up

````shell
pip install --upgrade pip
pip install dbt-core==1.9.4
pip install dbt-snowflake==1.9.4

dbt --version
dbt init
```
