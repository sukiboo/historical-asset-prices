import pandas as pd

from .base import BasePrices


class OptionPrices(BasePrices):
    """Retrieve and process option price data from flat files."""

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        super().__init__(
            tickers=tickers,
            date_start=date_start,
            date_end=date_end,
            asset_type="options",
            s3_prefix="us_options_opra",
            available_from="2014-06-01",
        )

    def filter_ticker_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter option contracts for a specific underlying ticker, e.g. O:SPY240105C00485000."""
        return data[data["ticker"].str.startswith(f"O:{ticker}")].copy()
