CREATE TABLE IF NOT EXISTS tickers(
    ticker TEXT PRIMARY KEY
    , name TEXT
    , market TEXT
    , locale TEXT
    , active INTEGER
    , source_feed TEXT
    , type TEXT
    , composite_figi TEXT
    , share_class_figi TEXT
    , primary_exchange TEXT
    , cik TEXT
    , currency_name TEXT
    , currency_symbol TEXT
    , base_currency_name TEXT
    , base_currency_symbol TEXT
    , last_updated_utc TEXT
)