from __future__ import annotations

import logging
import os

from massive import RESTClient

from src.constants import DATE_END, DATE_START, TICKERS
from src.option_prices import OptionPrices
from src.stock_prices import StockPrices
from src.utils import setup_logging, to_timestamp

logger = setup_logging(console_level=logging.INFO, file_level=logging.DEBUG)

client = RESTClient(os.getenv("MASSIVE_API_KEY"))


# TODO: move client
# TODO: change to daily aggs
# TODO: parse options
logger.info(f"Retrieving prices for {TICKERS} from {DATE_START} to {DATE_END}")
date_start = to_timestamp(DATE_START)
date_end = to_timestamp(DATE_END)
for ticker in TICKERS:
    logger.info(f"Retrieving stock prices for {ticker} from {date_start} to {date_end}")
    stock_prices = StockPrices(client, ticker, date_start, date_end)
    stock_prices.retrieve_prices()
    print(stock_prices)

logger.info(f"Retrieving option prices for {TICKERS} from {date_start} to {date_end}")
option_prices = OptionPrices(TICKERS, date_start, date_end)
option_prices.retrieve_prices()
print(option_prices)
