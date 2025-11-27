import pandas as pd

from src.base_prices import BasePrices


class StockPrices(BasePrices):
    """Retrieve and process stock price data from flat files."""

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        super().__init__(
            tickers=tickers,
            date_start=date_start,
            date_end=date_end,
            asset_type="stocks",
            s3_prefix="us_stocks_sip",
        )

    def filter_ticker_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter stock prices for a specific ticker."""
        return data[data["ticker"] == ticker].copy()
