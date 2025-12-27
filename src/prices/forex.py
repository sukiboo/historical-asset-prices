import pandas as pd

from .base import BasePrices


class ForexPrices(BasePrices):
    """Retrieve and process forex price data from flat files."""

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        super().__init__(
            tickers=tickers,
            date_start=date_start,
            date_end=date_end,
            asset_type="forex",
            s3_prefix="global_forex",
            available_from="2009-10-01",
        )

    def filter_ticker_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter forex prices for a specific ticker, e.g. C:EUR-USD."""
        return data[data["ticker"] == f"C:{ticker}"].copy()
