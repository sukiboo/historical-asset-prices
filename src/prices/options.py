import pandas as pd

from .base import BasePrices


class OptionPrices(BasePrices):
    """Retrieve raw option price flat files from S3."""

    def __init__(self, date_start: pd.Timestamp, date_end: pd.Timestamp) -> None:
        super().__init__(
            date_start=date_start,
            date_end=date_end,
            asset_type="options",
            s3_prefix="us_options_opra",
            available_from="2014-06-01",
        )
