# asset type retrieval flags
RETRIEVE_STOCKS = True
RETRIEVE_OPTIONS = True
RETRIEVE_CRYPTO = True
RETRIEVE_FOREX = True

# date range for file retrieval
DATE_START = "2025-01-01"  # start date (YYYY-MM-DD)
DATE_END = "2025-01-03"  # end date (YYYY-MM-DD)

# data retrieval settings
MIN_DELAY = 1  # minimum delay between retries (seconds)
MAX_DELAY = 600  # maximum delay between retries (seconds)
MAX_RETRIES = 100  # maximum number of retries
