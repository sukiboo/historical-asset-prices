# tickers for price retrieval
STOCK_TICKERS = ["SPY"]  # list of stock tickers to retrieve
CRYPTO_TICKERS = ["BTC-USD"]  # list of crypto tickers to retrieve
FOREX_TICKERS = ["EUR-USD"]  # list of forex tickers to retrieve

# date range for price retrieval
DATE_START = "2024-04-01"  # start date (YYYY-MM-DD)
DATE_END = "2024-04-02"  # end date (YYYY-MM-DD)

# data retrieval settings
MIN_DELAY = 1  # minimum delay between retries (seconds)
MAX_DELAY = 600  # maximum delay between retries (seconds)
MAX_RETRIES = 100  # maximum number of retries
