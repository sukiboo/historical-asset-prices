from __future__ import annotations

import logging
import os
from typing import cast

import pandas as pd
from polygon import RESTClient
from polygon.exceptions import BadResponse
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.exceptions import MaxRetryError

from src.utils import setup_logging

logger = setup_logging()


API_KEY = os.getenv("POLYGON_API_KEY")
client = RESTClient(API_KEY)


TICKERS = ["AAPL", "MSFT"]
DATE_START = "2024-01-01"
DATE_END = "2024-03-31"

MIN_DELAY = 10
MAX_DELAY = 600
MAX_RETRIES = 10


def with_retry(func):
    """Apply retry decorator to a function. For iterators, converts to list."""

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(min=MIN_DELAY, max=MAX_DELAY),
        retry=retry_if_exception_type((BadResponse, MaxRetryError)),
        before_sleep=before_sleep_log(logger, logging.DEBUG),
        after=after_log(logger, logging.DEBUG),
    )
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
            return list(result)
        return result

    return wrapper


for TICKER in TICKERS:
    os.makedirs(f"data/{TICKER}", exist_ok=True)
    current_date: pd.Timestamp = cast(pd.Timestamp, pd.Timestamp(DATE_START))
    fetch_aggs = with_retry(client.list_aggs)

    while current_date <= pd.Timestamp(DATE_END):  # type: ignore
        next_month: pd.Timestamp = cast(
            pd.Timestamp,
            (current_date + pd.offsets.MonthEnd(1)) + pd.Timedelta(days=1),
        )
        logger.info(
            f"{TICKER} prices: retrieving records for {current_date.date()}--{next_month.date()}..."
        )

        aggs = fetch_aggs(
            ticker=TICKER,
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
                f"{TICKER} prices: retrieved {len(aggs)} records "
                + f"for {current_date.date()}--{next_month.date()}"
            )

            df = pd.DataFrame([a.__dict__ for a in aggs])
            if "timestamp" in df.columns and pd.api.types.is_numeric_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.to_parquet(f"data/{TICKER}/{current_date.strftime('%Y-%m')}.parquet", index=False)
        else:
            logger.warning(
                f"{TICKER} prices: no records returned "
                + f"for {current_date.date()}--{next_month.date()}"
            )

        current_date = next_month

    print(df)
    print(f"Retrieved {len(df)} rows for {TICKER}\n")
