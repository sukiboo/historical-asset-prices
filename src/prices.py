from __future__ import annotations

import logging
import os

import pandas as pd
from polygon import RESTClient

from src.utils import with_retry

logger = logging.getLogger(__name__)


class StockPrices:

    def __init__(
        self, client: RESTClient, ticker: str, date_start: pd.Timestamp, date_end: pd.Timestamp
    ):
        self.client = client
        self.ticker = ticker
        self.date_start = date_start
        self.date_end = date_end

    def __str__(self):
        return (
            f"StockPrices(ticker={self.ticker}, "
            f"date_start={self.date_start.date()}, "
            f"date_end={self.date_end.date()})"
        )

    def retrieve_prices(self):
        os.makedirs(f"data/{self.ticker}", exist_ok=True)
        current_date = self.date_start
        fetch_aggs = with_retry(self.client.list_aggs)

        while current_date <= self.date_end:  # type: ignore
            next_month = (current_date + pd.offsets.MonthEnd(1)) + pd.Timedelta(days=1)
            logger.info(
                f"{self.ticker} prices: retrieving records for "
                f"{current_date.date()}--{next_month.date()}..."
            )

            aggs = fetch_aggs(
                ticker=self.ticker,
                multiplier=1,
                timespan="minute",
                from_=current_date.strftime("%Y-%m-%d"),
                to=next_month.strftime("%Y-%m-%d"),
                adjusted=True,
                sort="asc",
                limit=50000,
            )
            if aggs:
                logger.info(
                    f"{self.ticker} prices: retrieved {len(aggs)} records "
                    + f"for {current_date.date()}--{next_month.date()}"
                )

                df = pd.DataFrame([a.__dict__ for a in aggs])
                if "timestamp" in df.columns and pd.api.types.is_numeric_dtype(df["timestamp"]):
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.to_parquet(
                    f"data/{self.ticker}/{current_date.strftime('%Y-%m')}.parquet", index=False
                )
            else:
                logger.warning(
                    f"{self.ticker} prices: no records returned "
                    + f"for {current_date.date()}--{next_month.date()}"
                )

            current_date = next_month

        logger.info(f"Retrieved {len(df)} rows for {self.ticker}\n")
