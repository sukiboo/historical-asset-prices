import logging
import os
from abc import ABC, abstractmethod
from typing import cast

import pandas as pd
from tqdm import tqdm

from src.utils import (
    all_tickers_have_data,
    create_empty_marker,
    get_file_from_s3,
    get_flat_file_path,
    save_daily_prices,
    save_flat_file,
    with_retry,
)

logger = logging.getLogger(__name__)


class BasePrices(ABC):
    """Base class for retrieving and processing price data from flat files."""

    def __init__(
        self,
        tickers: list[str],
        date_start: pd.Timestamp,
        date_end: pd.Timestamp,
        asset_type: str,
        s3_prefix: str,
    ) -> None:
        self.tickers = [ticker.upper() for ticker in tickers]
        self.date_start = date_start
        self.date_end = date_end
        self.asset_type = asset_type
        self.s3_prefix = s3_prefix
        self.prices_dir = f"data/prices/{asset_type}"
        self.files_dir = f"data/files/{asset_type}"

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(tickers={self.tickers}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self) -> None:
        """Download flat files from S3 and process them into per-ticker parquet files."""
        fetch_aggs = with_retry(get_file_from_s3)

        with tqdm(
            total=(self.date_end - self.date_start).days,
            desc=f"Retrieving {self.asset_type} prices",
            unit="day",
        ) as pbar:
            current_day: pd.Timestamp = self.date_start
            while current_day < self.date_end:
                pbar.set_postfix({"date": current_day.strftime("%Y-%m-%d")})

                if not all_tickers_have_data(self.prices_dir, self.tickers, current_day, logger):
                    # Check if flat file exists locally first
                    flat_file_path = get_flat_file_path(self.files_dir, current_day)
                    empty_marker_path = f"{flat_file_path}.empty"

                    if os.path.exists(flat_file_path):
                        logger.debug(f"Loading cached file: {flat_file_path}")
                        data = pd.read_csv(flat_file_path, compression="gzip")
                    elif os.path.exists(empty_marker_path):
                        logger.debug(f"Empty marker exists: {empty_marker_path}")
                        data = pd.DataFrame()
                    else:
                        # Download from S3 and cache locally
                        s3_path = (
                            f"{self.s3_prefix}/minute_aggs_v1/"
                            f'{current_day.strftime("%Y/%m")}/'
                            f'{current_day.strftime("%Y-%m-%d")}.csv.gz'
                        )
                        data = fetch_aggs(
                            object_key=s3_path,
                            bucket_name="flatfiles",
                            logger=logger,
                            aws_access_key_id=os.getenv("MASSIVE_AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("MASSIVE_API_KEY"),
                        )

                        # Save flat file or empty marker for future use
                        if not data.empty:
                            save_flat_file(data, flat_file_path, logger)
                        else:
                            create_empty_marker(flat_file_path, logger)

                    # Always parse and save, even if empty (for weekends/holidays)
                    self.parse_and_save(data, current_day)

                pbar.update(1)
                current_day = cast(pd.Timestamp, current_day + pd.Timedelta(days=1))

    def parse_and_save(self, data: pd.DataFrame, current_day: pd.Timestamp) -> None:
        """Parse raw data and save per-ticker files."""
        # Handle empty DataFrame (weekends/holidays)
        if data.empty:
            for ticker in self.tickers:
                marker_file = (
                    f"{self.prices_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet.empty"
                )
                os.makedirs(os.path.dirname(marker_file), exist_ok=True)
                open(marker_file, "a").close()
            return

        # Convert window_start from nanoseconds to timestamp
        # Flat files use 'window_start' column with Unix timestamps in nanoseconds (UTC)
        data["timestamp"] = pd.to_datetime(data["window_start"], unit="ns", utc=True).dt.tz_convert(
            "America/New_York"
        )
        data = data.drop(columns=["window_start"])
        # Set timestamp as index and sort
        data = data.set_index("timestamp").sort_index()

        for ticker in self.tickers:
            ticker_data = self.filter_ticker_data(data, ticker)
            ticker_data = ticker_data[["ticker", "open", "close", "low", "high", "volume"]]

            file_path = f"{self.prices_dir}/{ticker}/{current_day.strftime('%Y-%m-%d')}.parquet"
            save_daily_prices(ticker_data, file_path)

    @abstractmethod
    def filter_ticker_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter data for a specific ticker. Must be implemented by subclasses."""
        pass
