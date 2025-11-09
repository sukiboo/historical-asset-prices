from __future__ import annotations

import logging
import os

import pandas as pd
from massive import RESTClient

from src.utils import with_retry

logger = logging.getLogger(__name__)


class StockPrices:

    def __init__(
        self, client: RESTClient, ticker: str, date_start: pd.Timestamp, date_end: pd.Timestamp
    ):
        self.client = client
        self.ticker = ticker.upper()
        self.date_start = date_start
        self.date_end = date_end
        self.data_dir = f"data/stocks/{self.ticker}"

    def __str__(self):
        return (
            f"StockPrices(ticker={self.ticker}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self.fetch_aggs = with_retry(self.client.list_aggs)

        current_date = self.date_start
        while current_date < self.date_end:
            next_month = (current_date.to_period("M") + 1).to_timestamp()

            if os.path.exists(f"{self.data_dir}/{current_date.strftime('%Y-%m')}.parquet"):
                logger.info(
                    f"{self.ticker} stock prices: skipping records for "
                    f"{current_date.date()}--{next_month.date()}"
                )

            else:
                logger.info(
                    f"{self.ticker} stock prices: retrieving records for "
                    f"{current_date.date()}--{next_month.date()}..."
                )

                aggs = self.retrieve_prices_interval(current_date, next_month)
                if aggs:
                    logger.info(
                        f"{self.ticker} stock prices: retrieved {len(aggs)} records "
                        + f"for {current_date.date()}--{next_month.date()}"
                    )
                    self.save_prices_monthly(aggs, current_date)
                else:
                    logger.warning(
                        f"{self.ticker} stock prices: no records returned "
                        + f"for {current_date.date()}--{next_month.date()}"
                    )

            current_date = next_month

    def retrieve_prices_interval(
        self, start_date: pd.Timestamp, end_date: pd.Timestamp
    ) -> list[object]:
        return self.fetch_aggs(
            ticker=self.ticker,
            multiplier=1,
            timespan="minute",
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            adjusted=True,
            sort="asc",
            limit=50000,
        )

    def save_prices_monthly(self, aggs: list[object], start_date: pd.Timestamp):
        df = pd.DataFrame([a.__dict__ for a in aggs])
        if "timestamp" in df.columns and pd.api.types.is_numeric_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.to_parquet(f"{self.data_dir}/{start_date.strftime('%Y-%m')}.parquet", index=False)


class OptionPrices:

    def __init__(
        self, client: RESTClient, ticker: str, date_start: pd.Timestamp, date_end: pd.Timestamp
    ):
        self.client = client
        self.ticker = ticker.upper()
        self.date_start = date_start
        self.date_end = date_end
        self.data_dir = f"data/options/{self.ticker}"

    def __str__(self):
        return (
            f"OptionPrices(ticker={self.ticker}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self.fetch_aggs = with_retry(self.client.list_aggs)
        self.fetch_options_contracts = with_retry(self.client.list_options_contracts)

        current_date = self.date_start
        while current_date < self.date_end:
            next_month = (current_date.to_period("M") + 1).to_timestamp()

            # Get option chains for the current date
            option_contracts = list(
                self.fetch_options_contracts(
                    underlying_ticker=self.ticker,
                    as_of=current_date.strftime("%Y-%m-%d"),
                )
            )
            logger.info(
                f"{self.ticker} option contracts: found {len(option_contracts)} contracts "
                f"for {current_date.date()}"
            )
            print(option_contracts)

            # if os.path.exists(f"{self.data_dir}/{current_date.strftime('%Y-%m')}.parquet"):
            #     logger.info(
            #         f"{self.ticker} option prices: skipping records for "
            #         f"{current_date.date()}--{next_month.date()}"
            #     )

            # else:
            #     logger.info(
            #         f"{self.ticker} option prices: retrieving records for "
            #         f"{current_date.date()}--{next_month.date()}..."
            #     )

            #     aggs = self.retrieve_prices_interval(current_date, next_month)
            #     if aggs:
            #         logger.info(
            #             f"{self.ticker} option prices: retrieved {len(aggs)} records "
            #             + f"for {current_date.date()}--{next_month.date()}"
            #         )
            #         self.save_prices_monthly(aggs, current_date)
            #     else:
            #         logger.warning(
            #             f"{self.ticker} option prices: no records returned "
            #             + f"for {current_date.date()}--{next_month.date()}"
            #         )

            current_date = next_month

    def retrieve_prices_interval(
        self, start_date: pd.Timestamp, end_date: pd.Timestamp
    ) -> list[object]:
        return self.fetch_aggs(
            ticker=self.ticker,
            multiplier=1,
            timespan="minute",
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            adjusted=True,
            sort="asc",
            limit=50000,
        )

    def save_prices_monthly(self, aggs: list[object], start_date: pd.Timestamp):
        df = pd.DataFrame([a.__dict__ for a in aggs])
        if "timestamp" in df.columns and pd.api.types.is_numeric_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.to_parquet(f"{self.data_dir}/{start_date.strftime('%Y-%m')}.parquet", index=False)
