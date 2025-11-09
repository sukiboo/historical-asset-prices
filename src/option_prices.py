from __future__ import annotations

import logging
import os
from typing import cast

import pandas as pd

from src.utils import get_file_from_s3, with_retry

logger = logging.getLogger(__name__)


class OptionPrices:

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        self.tickers = tickers
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
        os.makedirs(self.data_dir, exist_ok=True)
        self.fetch_aggs = with_retry(get_file_from_s3)

        current_day: pd.Timestamp = self.date_start
        while current_day < self.date_end:
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

    def parse_option_contracts(self, option_contracts: pd.DataFrame, current_day: pd.Timestamp):
        for ticker in self.tickers:
            # TODO: extract tickers from option_contracts
            self.save_daily_prices(option_contracts, current_day, ticker)

    # TODO: move to utils
    def save_daily_prices(self, df: pd.DataFrame, day: pd.Timestamp, ticker: str) -> None:
        df.to_parquet(f"{self.data_dir}/{ticker}/{day.strftime('%Y-%m-%d')}.parquet", index=False)
