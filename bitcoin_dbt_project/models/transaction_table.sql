SELECT *
FROM {{ source('raw_btc_source', 'BTC_TRANSACTIONS') }}