# import pandas as pd
# from typing import Any, Dict, Optional, List
# from typing import Dict, Any, Union, List, Optional, Tuple

# from .extractors.tickers import AllTickersExtractor
# from .extractors.timeseries import TimeseriesExtractor,IntradayTimeseriesExtractor
# from .extractors.esg import EtfEsgRiskExtractor,StockEsgRiskExtractor
# from .extractors.config import *
# from .extractors.etf import FactorProfileExtractor,InvestmentStrategyExtractor

# def get_all_securities_info() -> pd.DataFrame:
#     """
#     Get all securities tickers and basic info.
    
#     Returns
#     -------
#     pd.DataFrame
#         DataFrame containing the list of securities with their metadata.
#     """
#     return AllTickersExtractor().get()

# def get_timeseries_data(
#     id_sec: str,
#     start_date: str,
#     end_date: str,
#     frequency: str = 'daily',
#     columns: Optional[List[str]] = None
# ) -> pd.DataFrame:
#     """
#     Get timeseries data for a security.
    
#     Parameters
#     ----------
#     id_sec : str
#         Security ID (e.g., '0P0000OQN8')
#     start_date : str
#         Start date in YYYY-MM-DD format
#     end_date : str
#         End date in YYYY-MM-DD format
#     frequency : str, optional
#         Data frequency: 'daily', 'monthly', 'weekly' (default: 'daily')
#     columns : List[str], optional
#         List of data columns to retrieve. Must be from TIMESERIES_COLUMNS.
#         If None, defaults to OHLCV data.
#         Available columns: open, high, low, close, volume, previousClose,
#         marketTotalReturn, earning, split, dividend, PE, PS, PB, PCF,
#         EPSRolling, shortInterestRatio
    
#     Returns
#     -------
#     pd.DataFrame
#         DataFrame containing timeseries data
        
#     Raises
#     ------
#     ValueError
#         If any column is not in TIMESERIES_COLUMNS
        
#     Example
#     -------
#     >>> df = get_timeseries_data('0P0000OQN8', '2024-01-01', '2024-12-31')
#     >>> df = get_timeseries_data('0P0000OQN8', '2024-01-01', '2024-12-31', 
#     ...                          frequency='monthly', columns=['open', 'close', 'PE'])
#     """
#     if columns is None:
#         columns = ['open', 'high', 'low', 'close', 'volume', 'previousClose']
    
#     # Validate columns
#     invalid_columns = [col for col in columns if col not in TIMESERIES_COLUMNS]
#     if invalid_columns:
#         raise ValueError(
#             f"Invalid columns: {invalid_columns}. "
#             f"Must be from: {TIMESERIES_COLUMNS}"
#         )
    
#     return TimeseriesExtractor().get(
#         id_sec=id_sec,
#         columns=columns,
#         frequency=frequency,
#         start_date=start_date,
#         end_date=end_date
#     )

# def get_intraday_timeseries_data(
#     id_sec: str,
#     frequency: str = '5min',
#     columns: Optional[List[str]] = None,
#     pre_after: bool = False,
#     trading_days: int = 1
# ) -> pd.DataFrame:
#     """
#     Get intraday timeseries data for a security.
    
#     Parameters
#     ----------
#     id_sec : str
#         Security ID (e.g., '0P0000OQN8')
#     start_date : str
#         Start date in YYYY-MM-DD format
#     end_date : str
#         End date in YYYY-MM-DD format
#     frequency : str, optional
#         Intraday frequency: '1min', '5min', '15min', '30min', '1hour'/'hourly'
#         (default: '5min')
#     columns : List[str], optional
#         List of data columns to retrieve. Must be from INTRADAY_TIMESERIES_COLUMNS.
#         If None, defaults to OHLCV data.
#         Available columns: open, high, low, close, volume, previousClose
#     pre_after : bool, optional
#         Include pre-market and after-hours data (default: False)
#     trading_days : int, optional
#         Number of trading days to include (default: 1)
    
#     Returns
#     -------
#     pd.DataFrame
#         DataFrame containing intraday timeseries data
        
#     Raises
#     ------
#     ValueError
#         If any column is not in INTRADAY_TIMESERIES_COLUMNS or frequency is invalid
        
#     Example
#     -------
#     >>> df = get_intraday_timeseries_data('0P0000OQN8', '2024-01-01', '2024-01-31')
#     >>> df = get_intraday_timeseries_data('0P0000OQN8', '2024-01-01', '2024-01-31',
#     ...                                   frequency='15min', columns=['open', 'close', 'volume'])
#     """
#     if columns is None:
#         columns = ['open', 'high', 'low', 'close', 'volume', 'previousClose']
    
#     # Validate columns
#     invalid_columns = [col for col in columns if col not in TIMESERIES_COLUMNS]
#     if invalid_columns:
#         raise ValueError(
#             f"Invalid columns: {invalid_columns}. "
#             f"Must be from: {TIMESERIES_COLUMNS}"
#         )
    
#     # Validate frequency
#     if frequency not in FREQUENCY_MAP:
#         raise ValueError(
#             f"Invalid frequency: {frequency}. "
#             f"Must be one of: {list(FREQUENCY_MAP.keys())}"
#         )
    
#     return IntradayTimeseriesExtractor().get(
#         id_sec=id_sec,
#         columns=columns,
#         frequency=frequency,
#         pre_after=pre_after,
#         trading_days=trading_days
#     )
    
# def get_stock_esg_risk(
#     id_sec: str,
# ) -> pd.DataFrame:

#     return StockEsgRiskExtractor().get(
#         id_sec=id_sec,
#     )
    
# def get_etf_esg_risk(
#     id_sec: str,
# ) -> pd.DataFrame:

#     return EtfEsgRiskExtractor().get(
#         id_sec=id_sec,
#     )
    
# def get_factor_profile(
#     id_sec: str,
# ) -> pd.DataFrame:

#     return FactorProfileExtractor().get(
#         id_sec=id_sec,
#     )

# def get_all_tickers(forced_extract: bool = False) -> pd.DataFrame:
#     return AllTickersExtractor(forced_extract=forced_extract).run()