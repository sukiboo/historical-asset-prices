import logging

from src.constants import DATE_END, DATE_START, TICKERS
from src.option_prices import OptionPrices
from src.stock_prices import StockPrices
from src.utils import setup_logging, to_timestamp

logger = setup_logging(console_level=logging.INFO, file_level=logging.DEBUG)


# TODO: parse stocks/options
# TODO: clean up logging
logger.info(f"Retrieving prices for {TICKERS} from {DATE_START} to {DATE_END}")
date_start = to_timestamp(DATE_START)
date_end = to_timestamp(DATE_END)

logger.info(f"Retrieving stock prices for {TICKERS} from {date_start.date()} to {date_end.date()}")
stock_prices = StockPrices(TICKERS, date_start, date_end)
stock_prices.retrieve_prices()
print(stock_prices)

logger.info(f"Retrieving option prices for {TICKERS} from {date_start.date()} to {date_end.date()}")
option_prices = OptionPrices(TICKERS, date_start, date_end)
option_prices.retrieve_prices()
print(option_prices)
