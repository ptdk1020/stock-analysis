CREATE TABLE IF NOT EXISTS small_daily(
    date TEXT
    , ticker TEXT
    , open_price 
    , close_price NUMERIC
    , highest_price NUMERIC
    , lowest_price NUMERIC
    , number_of_transactions INTEGER
    , trading_volume NUMERIC
    , volume_weighted_average_price NUMERIC
    , otc_sticker INTEGER
    , window_start_timestamp INTEGER
    , PRIMARY KEY (date,ticker)
)