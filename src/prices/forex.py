import pandas as pd

from .base import BasePrices


class ForexPrices(BasePrices):
    """Retrieve raw forex price flat files from S3."""

    def __init__(self, date_start: pd.Timestamp, date_end: pd.Timestamp) -> None:
        super().__init__(
            date_start=date_start,
            date_end=date_end,
            asset_type="forex",
            s3_prefix="global_forex",
            available_from="2009-10-01",
        )
