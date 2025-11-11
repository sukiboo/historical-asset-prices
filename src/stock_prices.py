from __future__ import annotations

import logging
import os
from typing import cast

import pandas as pd
from massive import RESTClient

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

        current_day: pd.Timestamp = self.date_start
        while current_day < self.date_end:
            next_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))

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

                    if stock_prices is not None:
                        self.parse_stock_prices(stock_prices, current_day, ticker)

            current_day = next_day

    def ticker_has_stock_data(self, ticker: str, current_day: pd.Timestamp) -> bool:
        if os.path.exists(f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"):
            logger.info(f"Stock prices: skipping {ticker} records for {current_day.date()}...")
            return True
        else:
            return False

    def parse_stock_prices(
        self, stock_prices: pd.DataFrame, current_day: pd.Timestamp, ticker: str
    ) -> None:

        # Convert timestamp from milliseconds to datetime and make it first column
        # Polygon API returns Unix timestamps in UTC, convert to timezone-aware ET
        stock_prices["timestamp"] = pd.to_datetime(
            stock_prices["timestamp"], unit="ms", utc=True
        ).dt.tz_convert("America/New_York")
        stock_prices = stock_prices.set_index("timestamp").sort_index().reset_index()

        file_path = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"
        save_daily_prices(stock_prices, file_path)
