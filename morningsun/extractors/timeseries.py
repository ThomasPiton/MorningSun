import pandas as pd
from typing import Any, Dict, Optional, List

from morningsun.core.client import BaseClient
from morningsun.core.auth import AuthType
from morningsun.extractors.config import *

class TimeseriesExtractor(BaseClient):
    """Extracts timeseries data for securities from Morningstar."""

    def __init__(self):
        super().__init__(auth_type=AuthType.BEARER_TOKEN)

    def get(self, id_sec: str, columns: List[str], frequency: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Retrieve timeseries data for a security.
        
        Parameters
        ----------
        id_sec : str
            Security ID (e.g., '0P0000OQN8')
        columns : List[str]
            List of data columns to retrieve (e.g., ['open', 'high', 'low', 'close', 'volume'])
        frequency : str
            Data frequency: 'daily', 'monthly', 'weekly'
        start_date : str
            Start date in YYYY-MM-DD format
        end_date : str
            End date in YYYY-MM-DD format
            
        Returns
        -------
        pd.DataFrame
            DataFrame containing timeseries data
            
        Example
        -------
        >>> extractor = TimeseriesExtractor()
        >>> df = extractor.get('0P0000OQN8', ['open', 'close', 'volume'], 'daily', '2024-01-01', '2024-12-31')
        """
        url = URLS["TIMESERIES"]
        columns_str = ",".join(columns)
        query = f"{id_sec}:{columns_str}"
        freq_code = FREQUENCY_MAP.get(frequency.lower(), 'd')
        
        params = {
            'query': query,
            'frequency': freq_code,
            'startDate': start_date,
            'endDate': end_date,
            'trackMarketData': '3.6.5',
            'instid': 'DOTCOM'
        }

        response = self.request(url=url, params=params, method="GET")
        
        if not response or len(response) == 0:
            return pd.DataFrame()
        
        data = response[0].get("series", [])
        
        df = pd.DataFrame(data)
       
        return df
    
class IntradayTimeseriesExtractor(BaseClient):
    """Extracts intraday timeseries data for securities from Morningstar."""

    def __init__(self):
        super().__init__(auth_type=AuthType.BEARER_TOKEN)

    def get(
        self,
        id_sec: str,
        columns: List[str],
        frequency: str,
        pre_after: bool = False,
        trading_days: int = 1
    ) -> pd.DataFrame:
        """
        Retrieve intraday timeseries data for a security.
        
        Parameters
        ----------
        id_sec : str
            Security ID (e.g., '0P0000OQN8')
        columns : List[str]
            List of data columns to retrieve (e.g., ['open', 'high', 'low', 'close', 'volume'])
        frequency : str
            Intraday frequency: '1min', '5min', '15min', '30min', '1hour', 'hourly'
        start_date : str
            Start date in YYYY-MM-DD format
        end_date : str
            End date in YYYY-MM-DD format
        pre_after : bool, optional
            Include pre-market and after-hours data (default: False)
        trading_days : int, optional
            Number of trading days to include (default: 1)
            
        Returns
        -------
        pd.DataFrame
            DataFrame containing intraday timeseries data
            
        Example
        -------
        >>> extractor = IntradayTimeseriesExtractor()
        >>> df = extractor.get('0P0000OQN8', ['open', 'close', 'volume'], '5min', '2024-01-01', '2024-01-31')
        """
        url = URLS["TIMESERIES"]
        
        # Join columns with comma
        columns_str = ",".join(columns)
        query = f"{id_sec}:{columns_str}"
        
        # Map frequency to API format
        freq_code = FREQUENCY_MAP.get(frequency.lower(), '5min')
        
        params = {
            'query': query,
            'frequency': freq_code,
            'preAfter': pre_after,
            'tradingDays': trading_days,
            'trackMarketData': '3.6.5',
            'instid': 'DOTCOM'
        }

        response = self.request(url=url, params=params, method="GET")
        
        # response is already parsed as JSON by self.request()
        if not response or len(response) == 0:
            return pd.DataFrame()
        
        data = response[0]["series"][0].get("children", [])
        
        df = pd.DataFrame(data)
        
        return df
    
    