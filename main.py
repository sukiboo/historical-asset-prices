from __future__ import annotations

import logging
import os

from massive import RESTClient

from src.constants import DATE_END, DATE_START, TICKERS
from src.prices import StockPrices
from src.utils import setup_logging, to_timestamp

logger = setup_logging(level=logging.INFO)

client = RESTClient(os.getenv("MASSIVE_API_KEY"))


logger.info(f"Retrieving prices for {TICKERS} from {DATE_START} to {DATE_END}")
date_start = to_timestamp(DATE_START)
date_end = to_timestamp(DATE_END)
for ticker in TICKERS:
    stock_prices = StockPrices(client, ticker, date_start, date_end)
    stock_prices.retrieve_prices()
    print(stock_prices)
