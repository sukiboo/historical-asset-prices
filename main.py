from __future__ import annotations

import os

import pandas as pd
from polygon import RESTClient

from src.constants import DATE_END, DATE_START, TICKERS
from src.utils import setup_logging, with_retry

logger = setup_logging()


API_KEY = os.getenv("POLYGON_API_KEY")
client = RESTClient(API_KEY)

for TICKER in TICKERS:
    os.makedirs(f"data/{TICKER}", exist_ok=True)
    current_date = pd.Timestamp(DATE_START)
    fetch_aggs = with_retry(client.list_aggs)

    while current_date <= pd.Timestamp(DATE_END):  # type: ignore
        next_month = (current_date + pd.offsets.MonthEnd(1)) + pd.Timedelta(days=1)
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
