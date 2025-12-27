import logging

from src.constants import (
    CRYPTO_TICKERS,
    DATE_END,
    DATE_START,
    FOREX_TICKERS,
    STOCK_TICKERS,
)
from src.crypto_prices import CryptoPrices
from src.forex_prices import ForexPrices
from src.option_prices import OptionPrices
from src.stock_prices import StockPrices
from src.utils import setup_logging, to_timestamp

logger = setup_logging(console_level=logging.INFO, file_level=logging.DEBUG)


date_start = to_timestamp(DATE_START)
date_end = to_timestamp(DATE_END)

logger.info(
    f"Retrieving stock prices for {STOCK_TICKERS} from {date_start.date()} to {date_end.date()}"
)
stock_prices = StockPrices(STOCK_TICKERS, date_start, date_end)
stock_prices.retrieve_prices()

logger.info(
    f"Retrieving option prices for {STOCK_TICKERS} from {date_start.date()} to {date_end.date()}"
)
option_prices = OptionPrices(STOCK_TICKERS, date_start, date_end)
option_prices.retrieve_prices()

logger.info(
    f"Retrieving crypto prices for {CRYPTO_TICKERS} from {date_start.date()} to {date_end.date()}"
)
crypto_prices = CryptoPrices(CRYPTO_TICKERS, date_start, date_end)
crypto_prices.retrieve_prices()

logger.info(
    f"Retrieving forex prices for {FOREX_TICKERS} from {date_start.date()} to {date_end.date()}"
)
forex_prices = ForexPrices(FOREX_TICKERS, date_start, date_end)
forex_prices.retrieve_prices()
