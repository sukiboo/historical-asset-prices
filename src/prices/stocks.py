import pandas as pd

from .base import BasePrices


class StockPrices(BasePrices):
    """Retrieve raw stock price flat files from S3."""

    def __init__(
        self, date_start: pd.Timestamp, date_end: pd.Timestamp, tickers: list[str] | None = None
    ) -> None:
        super().__init__(
            date_start=date_start,
            date_end=date_end,
            asset_type="stocks",
            s3_prefix="us_stocks_sip",
            available_from="2003-10-01",
            tickers=tickers,
        )
