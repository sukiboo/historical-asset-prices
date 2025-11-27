import logging
import os
from typing import cast

import pandas as pd
from tqdm import tqdm

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

        with tqdm(
            total=(self.date_end - self.date_start).days,
            desc="Retrieving option prices",
            unit="day",
        ) as pbar:
            current_day: pd.Timestamp = self.date_start
            while current_day < self.date_end:
                pbar.set_postfix({"date": current_day.strftime("%Y-%m-%d")})

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

                    # Always parse and save, even if empty (for weekends/holidays)
                    self.parse_option_contracts(option_contracts, current_day)

                pbar.update(1)
                current_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))

    def all_tickers_have_option_data(self, current_day: pd.Timestamp) -> bool:
        date_str = current_day.strftime("%Y-%m-%d")
        if all(
            os.path.exists(f"{self.data_dir}/{ticker}/{date_str}.parquet")
            or os.path.exists(f"{self.data_dir}/{ticker}/{date_str}.empty")
            for ticker in self.tickers
        ):
            logger.debug(f"Option prices: skipping records for {current_day.date()}...")
            return True
        return False

    def parse_option_contracts(self, option_contracts: pd.DataFrame, current_day: pd.Timestamp):
        # Handle empty DataFrame (weekends/holidays)
        if option_contracts.empty:
            for ticker in self.tickers:
                marker_file = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.empty"
                os.makedirs(os.path.dirname(marker_file), exist_ok=True)
                open(marker_file, "a").close()
            return

        # Convert window_start from nanoseconds to timestamp
        # Options flat files use 'window_start' column with Unix timestamps in nanoseconds (UTC)
        option_contracts["timestamp"] = pd.to_datetime(
            option_contracts["window_start"], unit="ns", utc=True
        ).dt.tz_convert("America/New_York")
        option_contracts = option_contracts.drop(columns=["window_start"])
        # Set timestamp as index and sort
        option_contracts = option_contracts.set_index("timestamp").sort_index()

        for ticker in self.tickers:
            ticker_options = option_contracts[
                option_contracts["ticker"].str.startswith(f"O:{ticker}")
            ].copy()
            ticker_options = ticker_options[["ticker", "open", "close", "low", "high", "volume"]]

            file_path = f"{self.data_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"
            save_daily_prices(ticker_options, file_path)
