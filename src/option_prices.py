from __future__ import annotations

import logging
import os
from typing import cast

import pandas as pd

from src.utils import get_file_from_s3, save_daily_prices, with_retry

logger = logging.getLogger(__name__)


class OptionPrices:

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        self.tickers = [ticker.upper() for ticker in tickers]
        self.date_start = date_start
        self.date_end = date_end
        self.data_dir = f"data/options"

    def __str__(self) -> str:
        return (
            f"OptionPrices(tickers={self.tickers}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self) -> None:
        self.fetch_aggs = with_retry(get_file_from_s3)

        current_day: pd.Timestamp = self.date_start
        while current_day < self.date_end:
            if not self.all_tickers_have_option_data(current_day):
                s3_path = (
                    f"us_options_opra/minute_aggs_v1/"
                    f'{current_day.strftime("%Y/%m")}/'
                    f'{current_day.strftime("%Y-%m-%d")}.csv.gz'
                )
                option_contracts = self.fetch_aggs(
                    object_key=s3_path,
                    bucket_name="flatfiles",
                    logger=logger,
                    aws_access_key_id=os.getenv("MASSIVE_AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("MASSIVE_API_KEY"),
                )

                if option_contracts is not None:
                    self.parse_option_contracts(option_contracts, current_day)

            current_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))

    def all_tickers_have_option_data(self, current_day: pd.Timestamp) -> bool:
        if all(
            os.path.exists(f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet")
            for ticker in self.tickers
        ):
            logger.info(f"Option prices: skipping records for {current_day.date()}...")
            return True
        else:
            return False

    def parse_option_contracts(self, option_contracts: pd.DataFrame, current_day: pd.Timestamp):
        # Convert window_start from nanoseconds to timestamp column and make it first column
        # Options flat files use 'window_start' column with Unix timestamps in nanoseconds (UTC)
        option_contracts["timestamp"] = pd.to_datetime(
            option_contracts["window_start"], unit="ns", utc=True
        ).dt.tz_convert("America/New_York")
        option_contracts = option_contracts.drop(columns=["window_start"])
        option_contracts = option_contracts.set_index("timestamp").sort_index().reset_index()

        for ticker in self.tickers:
            ticker_options = cast(
                pd.DataFrame,
                option_contracts[option_contracts["ticker"].str.startswith(f"O:{ticker}")],
            )

            file_path = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"
            save_daily_prices(ticker_options, file_path)
