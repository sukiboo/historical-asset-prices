import logging

from src.constants import (
    DATE_END,
    DATE_START,
    RETRIEVE_CRYPTO,
    RETRIEVE_FOREX,
    RETRIEVE_OPTIONS,
    RETRIEVE_STOCKS,
)
from src.prices import CryptoPrices, ForexPrices, OptionPrices, StockPrices
from src.utils import setup_logging, to_timestamp

if __name__ == "__main__":
    logger = setup_logging(console_level=logging.INFO, file_level=logging.DEBUG)

    date_start = to_timestamp(DATE_START)
    date_end = to_timestamp(DATE_END)

    if RETRIEVE_STOCKS:
        logger.info(f"Retrieving stock files from {date_start.date()} to {date_end.date()}")
        stock_prices = StockPrices(date_start, date_end)
        stock_prices.retrieve_prices()

    if RETRIEVE_OPTIONS:
        logger.info(f"Retrieving option files from {date_start.date()} to {date_end.date()}")
        option_prices = OptionPrices(date_start, date_end)
        option_prices.retrieve_prices()

    if RETRIEVE_CRYPTO:
        logger.info(f"Retrieving crypto files from {date_start.date()} to {date_end.date()}")
        crypto_prices = CryptoPrices(date_start, date_end)
        crypto_prices.retrieve_prices()

    if RETRIEVE_FOREX:
        logger.info(f"Retrieving forex files from {date_start.date()} to {date_end.date()}")
        forex_prices = ForexPrices(date_start, date_end)
        forex_prices.retrieve_prices()
