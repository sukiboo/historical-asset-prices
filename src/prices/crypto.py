import pandas as pd

from .base import BasePrices


class CryptoPrices(BasePrices):
    """Retrieve raw crypto price flat files from S3."""

    def __init__(
        self, date_start: pd.Timestamp, date_end: pd.Timestamp, tickers: list[str] | None = None
    ) -> None:
        super().__init__(
            date_start=date_start,
            date_end=date_end,
            asset_type="crypto",
            s3_prefix="global_crypto",
            available_from="2013-11-01",
            tickers=tickers,
        )
