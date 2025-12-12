import pandas as pd
from typing import List, Union

from morningpy.core.security_loader import SecurityLoader
from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.timeseries import *
from morningpy.schema.timeseries import *

    
class IntradayTimeseriesExtractor(BaseExtractor):
    """
    Extracts intraday timeseries data for a single security from Morningstar.

    This extractor handles:
        - Validating inputs (dates, frequency, pre/post market flag)
        - Building API requests (splitting date ranges into 18-business-day chunks)
        - Processing API responses into a standardized pandas DataFrame

    Attributes
    ----------
    ticker : str or None
        Ticker symbol of the security.
    isin : str or None
        ISIN code of the security.
    security_id : str or None
        Morningstar internal security ID.
    performance_id : str or None
        Morningstar performance ID.
    start_date : str
        Start date for extraction (YYYY-MM-DD).
    end_date : str
        End date for extraction (YYYY-MM-DD).
    frequency : str
        Frequency of intraday data (e.g., "5min").
    pre_after : bool
        Include pre/post-market data if True.
    """

    config = IntradayTimeseriesConfig
    schema = IntradayTimeseriesSchema

    def __init__(self,
                 ticker: str = None,
                 isin: str = None,
                 security_id: str = None,
                 performance_id: str = None,
                 start_date: str = "1900-01-01",
                 end_date: str = "1900-01-01",
                 frequency: str = "5min",
                 pre_after: bool = False):
        """
        Initialize the IntradayTimeseriesExtractor.

        Parameters
        ----------
        ticker : str, optional
            Ticker symbol of the security (e.g., 'AAPL'). Mutually exclusive 
            with isin, security_id, and performance_id.
        isin : str, optional
            ISIN code of the security (e.g., 'US0378331005'). Mutually exclusive 
            with ticker, security_id, and performance_id.
        security_id : str, optional
            Morningstar internal security ID. Mutually exclusive with ticker, 
            isin, and performance_id.
        performance_id : str, optional
            Morningstar performance ID. Mutually exclusive with ticker, isin, 
            and security_id.
        start_date : str, optional
            Start date for extraction in YYYY-MM-DD format (e.g., '2024-01-01'). 
            Default is '1900-01-01'.
        end_date : str, optional
            End date for extraction in YYYY-MM-DD format (e.g., '2024-12-31'). 
            Default is '1900-01-01'.
        frequency : str, optional
            Frequency of intraday data. Must be one of the valid frequencies 
            defined in config (e.g., '5min', '15min', '30min', '1hour'). 
            Default is '5min'.
        pre_after : bool, optional
            Include pre-market and post-market data if True. Default is False.

        Notes
        -----
        At least one security identifier (ticker, isin, security_id, or 
        performance_id) must be provided. The extraction period cannot exceed 
        5 years from today.

        Raises
        ------
        ValueError
            If no valid security identifier is provided.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.pre_after = pre_after
        self.url = self.config.API_URL
        self.params = self.config.PARAMS.copy()
        self.mapping_frequency = self.config.MAPPING_FREQUENCY
        self.field_mapping = self.config.FIELD_MAPPING
        self.valid_frequency = self.config.VALID_FREQUENCY
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
        self.metadata = SecurityLoader(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).get(fields=["security_id"])
        

    def _check_inputs(self) -> None:
        """
        Validate user inputs and apply transformations.

        This method validates:
            - Frequency is in the list of valid frequencies
            - pre_after parameter is boolean
            - Dates are in YYYY-MM-DD format
            - start_date is before or equal to end_date
            - Extraction period does not exceed 5 years from today

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If frequency is invalid, dates are malformed, start_date is after 
            end_date, or extraction period exceeds 5 years.
        TypeError
            If pre_after is not a boolean.
        """
        if self.frequency not in self.valid_frequency:
            raise ValueError(
                f"Invalid frequency '{self.frequency}', must be one of {list(self.valid_frequency.keys())}"
            )

        if not isinstance(self.pre_after, bool):
            raise TypeError("Parameter 'pre_after' must be a boolean (True or False).")
        self.pre_after = "true" if self.pre_after else "false"

        try:
            self.start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            self.end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Dates must be in format YYYY-MM-DD (e.g., '2020-01-01').")

        if self.start_dt > self.end_dt:
            raise ValueError("start_date cannot be after end_date.")
        
        today = datetime.now()
        if (today - self.start_dt).days > 5 * 365:
            raise ValueError("Extraction period cannot exceed 5 years from today.")

    def _build_request(self) -> None:
        """
        Build one or several API requests per security ID.

        Morningstar API limits intraday data extraction to 18 business days per 
        call. If the date range exceeds 18 business days, multiple requests 
        (chunks) are created. Each chunk covers up to 18 business days and is 
        stored in self.requests.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If no business days are found in the given date range.

        Notes
        -----
        The method splits the date range into chunks of at most 18 business days.
        For each chunk, a separate API request is prepared with the appropriate
        query parameters including security ID, frequency, date range, and 
        pre/post-market flag.
        """
        max_business_days = 18

        business_days = pd.bdate_range(start=self.start_dt, end=self.end_dt)
        total_days = len(business_days)

        if total_days == 0:
            raise ValueError("No business days found in the given date range.")

        chunks = [
            business_days[i:i + max_business_days]
            for i in range(0, total_days, max_business_days)
        ]
        
        params_list = []
        base_params = self.config.PARAMS
        security_id = self.metadata[0]["security_id"]

        for chunk in chunks:
            params_list.append({
                **base_params, 
                "query": f"{security_id}:open,high,low,close,volume,previousClose",
                "frequency": self.mapping_frequency[self.frequency],
                "preAfter": self.pre_after,
                "startDate": chunk[0].strftime("%Y-%m-%d"),
                "endDate": chunk[-1].strftime("%Y-%m-%d"),
            })

        self.requests = [
            {
                "url": self.url,
                "params": params,
                "metadata": self.metadata[0],
            }
            for params in params_list
        ]
            

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar intraday timeseries response.

        Extracts intraday price data from the API response and transforms it 
        into a standardized DataFrame with proper column types and sorting.

        Parameters
        ----------
        response : dict
            API response containing intraday timeseries data. Expected to be a 
            list of security blocks, each containing series data with intraday 
            price information.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes security_id, date, open, high, low, close, volume, and 
            previous_close. Returns empty DataFrame if response is invalid or empty.
            
            String columns are filled with "N/A" for missing values.
            Numeric columns are filled with 0 for missing values.
            Sorted by security_id and date in ascending order.

        Notes
        -----
        The method iterates through each security block in the response, 
        extracting intraday data points (children) for each trading day (series).
        All data is flattened into rows and then converted to a DataFrame with
        standardized column names and types.
        """
        if not isinstance(response, list) or not response:
            return pd.DataFrame()

        rows = []

        for security_block in response:
            security_id = security_block.get("queryKey")
            series_list = security_block.get("series", [])

            for daily_series in series_list:
                previous_close = daily_series.get("previousClose")
                children = daily_series.get("children", [])

                for child in children:
                    rows.append({
                        "security_id": security_id,
                        "previous_close": previous_close,
                        **{key: child.get(value) for key, value in self.field_mapping.items()}
                    })
                
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
        df["date"] = df["date"].dt.tz_convert(None)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S") 
                
        df.sort_values(by=["security_id", "date"], inplace=True, ignore_index=True)
        return df
        

class HistoricalTimeseriesExtractor(BaseExtractor):
    """
    Extracts historical timeseries data for multiple securities from Morningstar.

    This extractor handles:
        - Validating inputs (dates, frequency, pre/post market flag, security limit)
        - Building API requests per security
        - Processing API responses into a standardized pandas DataFrame

    Attributes
    ----------
    ticker : str or list of str or None
        Ticker symbols of securities.
    isin : str or list of str or None
        ISIN codes of securities.
    security_id : str or list of str or None
        Morningstar internal security IDs.
    performance_id : str or list of str or None
        Morningstar performance IDs.
    start_date : str
        Start date for extraction (YYYY-MM-DD).
    end_date : str
        End date for extraction (YYYY-MM-DD).
    frequency : str
        Frequency of historical data (e.g., "daily", "weekly").
    pre_after : bool
        Include pre/post-market data if True.
    
    Notes
    -----
    Maximum of 100 securities per request is enforced.
    Dates are validated and must follow YYYY-MM-DD format.
    """

    config = HistoricalTimeseriesConfig
    schema = HistoricalTimeseriesSchema

    def __init__(self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,
        start_date: str = "1900-01-01",
        end_date: str = "2025-11-16",
        frequency: str = "daily",
        pre_after: bool = False):
        """
        Initialize the HistoricalTimeseriesExtractor.

        Parameters
        ----------
        ticker : str or list of str, optional
            Single ticker symbol or list of ticker symbols for securities
            (e.g., 'AAPL' or ['AAPL', 'MSFT']). Mutually exclusive with isin, 
            security_id, and performance_id.
        isin : str or list of str, optional
            Single ISIN code or list of ISIN codes for securities
            (e.g., 'US0378331005'). Mutually exclusive with ticker, security_id, 
            and performance_id.
        security_id : str or list of str, optional
            Single Morningstar security ID or list of IDs for securities.
            Mutually exclusive with ticker, isin, and performance_id.
        performance_id : str or list of str, optional
            Single Morningstar performance ID or list of IDs for securities.
            Mutually exclusive with ticker, isin, and security_id.
        start_date : str, optional
            Start date for extraction in YYYY-MM-DD format (e.g., '2020-01-01'). 
            Default is '1900-01-01'.
        end_date : str, optional
            End date for extraction in YYYY-MM-DD format (e.g., '2024-12-31'). 
            Default is '2025-11-16'.
        frequency : str, optional
            Frequency of historical data. Must be one of the valid frequencies 
            defined in config (e.g., 'daily', 'weekly', 'monthly', 'quarterly', 
            'yearly'). Default is 'daily'.
        pre_after : bool, optional
            Include pre-market and post-market data if True. Default is False.

        Notes
        -----
        At least one security identifier (ticker, isin, security_id, or 
        performance_id) must be provided. A maximum of 100 securities can be 
        requested in a single extraction. The extractor will retrieve historical 
        price data including open, high, low, close, volume, previous close, 
        and market total return for each specified security.

        Raises
        ------
        ValueError
            If no valid security identifier is provided.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.pre_after = pre_after
        self.url = self.config.API_URL
        self.params = self.config.PARAMS.copy()
        self.mapping_frequency = self.config.MAPPING_FREQUENCY
        self.field_mapping = self.config.FIELD_MAPPING
        self.valid_frequency = self.config.VALID_FREQUENCY
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        self.requests = []

        self.metadata = SecurityLoader(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).get(fields=["security_id"])

    def _check_inputs(self) -> None:
        """
        Validate user inputs and apply transformations.

        This method validates:
            - Frequency is in the list of valid frequencies
            - pre_after parameter is boolean
            - Number of securities does not exceed 100
            - Dates are in YYYY-MM-DD format
            - start_date is before or equal to end_date

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If frequency is invalid, more than 100 securities are requested,
            dates are malformed, or start_date is after end_date.
        TypeError
            If pre_after is not a boolean.
        """
        if self.frequency not in self.valid_frequency:
            raise ValueError(
                f"Invalid frequency '{self.frequency}', must be one of {list(self.valid_frequency.keys())}"
            )

        if not isinstance(self.pre_after, bool):
            raise TypeError("Parameter 'pre_after' must be a boolean (True or False).")
        self.pre_after = "true" if self.pre_after else "false"

        if len(self.metadata) > 100:
            raise ValueError("A maximum of 100 securities can be requested for historical extraction.")
        
        try:
            self.start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            self.end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Dates must be in format YYYY-MM-DD (e.g., '2020-01-01').")

        if self.start_dt > self.end_dt:
            raise ValueError("start_date cannot be after end_date.")

    def _build_request(self) -> None:
        """
        Build request dictionaries for each security.

        Creates one API request per security with the specified date range and 
        frequency. All requests are stored in self.requests list for batch 
        processing.

        Returns
        -------
        None

        Notes
        -----
        Each request includes the security ID, data fields (open, high, low, 
        close, volume, previous close, market total return), frequency, 
        pre/post-market flag, and date range. Unlike intraday extraction, 
        historical requests are not split into chunks.
        """
        for meta in self.metadata:
            req_params = {
                **self.params,
                "query": f"{meta['security_id']}:open,high,low,close,volume,previousClose,marketTotalReturn",
                "frequency": self.mapping_frequency[self.frequency],
                "preAfter": self.pre_after,
                "startDate": self.start_date,
                "endDate": self.end_date,
            }

            self.requests.append({
                "url": self.url,
                "params": req_params
            })

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar historical timeseries response.

        Extracts historical price data from the API response and transforms it 
        into a standardized DataFrame with proper column types and sorting.

        Parameters
        ----------
        response : dict
            API response containing historical timeseries data. Expected to be a 
            list of security blocks, each containing series data with historical 
            price information.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes security_id, date, open, high, low, close, volume, 
            previous_close, and market_total_return. Returns empty DataFrame if 
            response is invalid or empty.
            
            String columns are filled with "N/A" for missing values.
            Numeric columns are filled with 0 for missing values.
            Sorted by security_id and date in ascending order.

        Notes
        -----
        The method iterates through each security block in the response, 
        extracting historical data points from the series list. All data is 
        flattened into rows and then converted to a DataFrame with standardized 
        column names and types.
        """
        if not isinstance(response, list) or not response:
            return pd.DataFrame()

        rows = []

        for block in response:
            security_id = block.get("queryKey")
            series_list = block.get("series")

            if not series_list or not isinstance(series_list, list):
                continue

            for record in series_list:
                rows.append({
                    "security_id": security_id,
                    **{k: record.get(v) for k, v in self.field_mapping.items()}
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)

        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
        df["date"] = df["date"].dt.tz_convert(None)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d") 
        
        df.sort_values(by=["security_id", "date"], inplace=True, ignore_index=True)
        return df