import pandas as pd
from typing import Optional, Literal

class TickerExtractor:
    """
    Extracts and converts financial security tickers from a Parquet dataset.

    This class allows filtering securities by asset type and converting between
    ticker symbols, ISINs, performance IDs, and security IDs.
    """

    def __init__(
        self,
        asset_type: Optional[str] = None,
        ticker: Optional[str] = None,
        isin: Optional[str] = None,
        performance_id: Optional[str] = None
    ):
        """
        Initialize a TickerExtractor.

        Parameters
        ----------
        asset_type : str, optional
            Type of asset to filter ("fund", "etf", "stock"). If None, no filtering is applied.
        ticker : str, optional
            Ticker symbol for conversion.
        isin : str, optional
            ISIN code for conversion.
        performance_id : str, optional
            Morningstar performance identifier for conversion.
        """
        self.asset_type = asset_type
        self.ticker = ticker
        self.isin = isin
        self.performance_id = performance_id
        self.tickers = pd.read_parquet("data/tickers.parquet")

    def get(self) -> pd.DataFrame:
        """
        Retrieve tickers filtered by asset type.

        Returns
        -------
        pd.DataFrame
            DataFrame containing tickers of the specified asset type,
            or all tickers if asset_type is None.
        """
        if self.asset_type is None:
            return self.tickers
        return self.tickers[self.tickers["investment_type"] == self.asset_type]

    def convert_to(self, convert_to: Literal["ticker", "isin", "performance_id", "security_id"]) -> Optional[str]:
        """
        Convert between ticker, ISIN, performance_id, or security_id.

        Parameters
        ----------
        convert_to : {"ticker", "isin", "performance_id", "security_id"}
            The target field to convert to.

        Returns
        -------
        str or None
            The corresponding value in the target column, or None if not found.
        """
        df = self.tickers

        if self.ticker:
            row = df[df["ticker"] == self.ticker]
        elif self.isin:
            row = df[df["isin"] == self.isin]
        elif self.performance_id:
            row = df[df["performance_id"] == self.performance_id]
        else:
            return None

        if row.empty:
            return None

        return row.iloc[0][convert_to]
