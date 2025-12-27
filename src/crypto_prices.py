import pandas as pd

from src.base_prices import BasePrices


class CryptoPrices(BasePrices):
    """Retrieve and process crypto price data from flat files."""

    def __init__(
        self, tickers: list[str], date_start: pd.Timestamp, date_end: pd.Timestamp
    ) -> None:
        super().__init__(
            tickers=tickers,
            date_start=date_start,
            date_end=date_end,
            asset_type="crypto",
            s3_prefix="global_crypto",
        )

    def filter_ticker_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Filter crypto prices for a specific ticker, e.g. X:BTC-USD."""
        return data[data["ticker"] == f"X:{ticker}"].copy()
