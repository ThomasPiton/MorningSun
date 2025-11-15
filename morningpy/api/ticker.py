import pandas as pd
from typing import Optional, Literal

from morningpy.extractor.ticker import TickerExtractor

def get_all_funds() -> pd.DataFrame:
    """
    Get all tickers of type 'fund'.

    Returns
    -------
    pd.DataFrame
        DataFrame containing all fund tickers.
    """
    return TickerExtractor(asset_type="fund").get()


def get_all_etfs() -> pd.DataFrame:
    """
    Get all tickers of type 'etf'.

    Returns
    -------
    pd.DataFrame
        DataFrame containing all ETF tickers.
    """
    return TickerExtractor(asset_type="etf").get()


def get_all_stocks() -> pd.DataFrame:
    """
    Get all tickers of type 'stock'.

    Returns
    -------
    pd.DataFrame
        DataFrame containing all stock tickers.
    """
    return TickerExtractor(asset_type="stock").get()


def get_all_securities() -> pd.DataFrame:
    """
    Get all tickers regardless of asset type.

    Returns
    -------
    pd.DataFrame
        DataFrame containing all securities.
    """
    return TickerExtractor(asset_type=None).get()


def convert(
    ticker: Optional[str] = None,
    isin: Optional[str] = None,
    performance_id: Optional[str] = None,
    convert_to: Literal["ticker", "isin", "performance_id", "security_id"] = None
) -> Optional[str]:
    """
    Convert between ticker, ISIN, performance_id, or security_id.

    Parameters
    ----------
    ticker : str, optional
        Ticker symbol to convert.
    isin : str, optional
        ISIN code to convert.
    performance_id : str, optional
        Morningstar performance identifier to convert.
    convert_to : {"ticker", "isin", "performance_id", "security_id"}
        The target column to convert to.

    Returns
    -------
    str or None
        The corresponding value in the target column, or None if not found.
    """
    return TickerExtractor(ticker=ticker, isin=isin, performance_id=performance_id).convert_to(convert_to)