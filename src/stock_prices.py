import logging
import os
from typing import cast

import pandas as pd
from massive import RESTClient
from tqdm import tqdm

from src.utils import aggs_to_df, save_daily_prices, with_retry

logger = logging.getLogger(__name__)


class StockPrices:

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        self.client = RESTClient(os.getenv("MASSIVE_API_KEY"))
        self.tickers = [ticker.upper() for ticker in tickers]
        self.date_start = date_start
        self.date_end = date_end
        self.data_dir = f"data/stocks"

    def __str__(self) -> str:
        return (
            f"StockPrices(tickers={self.tickers}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self) -> None:
        self.fetch_aggs = with_retry(aggs_to_df(self.client.list_aggs, logger))

        with tqdm(
            total=(self.date_end - self.date_start).days,
            desc="Retrieving stocks prices",
            unit="day",
        ) as pbar:
            current_day: pd.Timestamp = self.date_start
            while current_day < self.date_end:
                next_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))
                pbar.set_postfix({"date": current_day.strftime("%Y-%m-%d")})

                for ticker in self.tickers:
                    if not self.ticker_has_stock_data(ticker, current_day):
                        stock_prices = self.fetch_aggs(
                            ticker=ticker,
                            multiplier=1,
                            timespan="minute",
                            from_=current_day.strftime("%Y-%m-%d"),
                            to=current_day.strftime("%Y-%m-%d"),
                            adjusted=True,
                            sort="asc",
                            limit=10000,
                        )

                        # Always parse and save, even if empty (for weekends/holidays)
                        self.parse_stock_prices(stock_prices, current_day, ticker)

                pbar.update(1)
                current_day = next_day

    def ticker_has_stock_data(self, ticker: str, current_day: pd.Timestamp) -> bool:
        date_str = current_day.strftime("%Y-%m-%d")
        parquet_file = f"{self.data_dir}/{ticker}/{date_str}.parquet"
        empty_marker = f"{self.data_dir}/{ticker}/{date_str}.empty"

        if os.path.exists(parquet_file) or os.path.exists(empty_marker):
            logger.debug(f"Stock prices: skipping {ticker} records for {current_day.date()}...")
            return True
        else:
            return False

    def parse_stock_prices(
        self, stock_prices: pd.DataFrame, current_day: pd.Timestamp, ticker: str
    ) -> None:
        stock_prices["ticker"] = ticker

        # Handle empty DataFrame (weekends/holidays)
        if stock_prices.empty:
            marker_file = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.empty"
            os.makedirs(os.path.dirname(marker_file), exist_ok=True)
            open(marker_file, "a").close()
            return

        # Convert timestamp from milliseconds to datetime
        # Polygon API returns Unix timestamps in UTC, convert to timezone-aware ET
        stock_prices["timestamp"] = pd.to_datetime(
            stock_prices["timestamp"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")
        # Set timestamp as index and sort
        stock_prices = stock_prices.set_index("timestamp").sort_index()
        stock_prices = stock_prices[["ticker", "open", "close", "low", "high", "volume"]]

        file_path = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"
        save_daily_prices(stock_prices, file_path)
