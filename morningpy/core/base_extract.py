from abc import ABC, abstractmethod
import aiohttp
from typing import Any, List, Tuple, Dict, Optional, Union, Type
import pandas as pd

from morningpy.core.decorator import save_dataframe_mock,save_api_response,save_api_request
from morningpy.core.interchange import DataFrameInterchange
from morningpy.core.config import CoreConfig

class BaseExtractor(ABC):
    """
    Abstract base class for asynchronous data extractors.
    
    Provides a standardized pipeline for API data extraction:
    1. Input validation
    2. Request building
    3. Asynchronous API calls
    4. Response processing
    5. Schema validation and type conversion
    
    Attributes
    ----------
    schema : Type, optional
        Pydantic or custom schema class for DataFrame validation
    client : APIClient
        HTTP client for making API requests
    url : str or List[str]
        API endpoint URL(s)
    params : dict, List[dict], or None
        Query parameters for API requests
    max_requests : int
        Maximum number of concurrent requests allowed
    """

    schema: Optional[Type] = None

    def __init__(self, client):
        """
        Initialize the base extractor.
        
        Parameters
        ----------
        client : APIClient
            Configured API client instance with headers and timeout settings
        """
        self.client = client
        self.url: Union[str, List[str]] = ""
        self.params: Union[Dict[str, Any], List[Dict[str, Any]], None] = None
        self.max_requests: int = CoreConfig.MAX_REQUESTS
        
    @abstractmethod
    def _check_inputs(self) -> None:
        """
        Validate input parameters before request build.
        
        Raises
        ------
        ValueError
            If required parameters are missing or invalid
        """
        raise NotImplementedError

    @abstractmethod
    def _build_request(self) -> None:
        """
        Build API request(s) by setting url, params, and requests list.
        
        Must populate self.requests with tuples of (url, params, metadata).
        """
        raise NotImplementedError

    @abstractmethod
    def _process_response(self, response: Any) -> pd.DataFrame:
        """
        Transform API response into a structured DataFrame.
        
        Parameters
        ----------
        response : Any
            Raw API response data (typically dict or list)
        
        Returns
        -------
        pd.DataFrame
            Processed and normalized data
        """
        raise NotImplementedError
    
    @save_dataframe_mock(activate=False) 
    async def _call_api(self) -> pd.DataFrame:
        """
        Execute asynchronous API calls and aggregate results.
        
        Returns
        -------
        pd.DataFrame
            Concatenated results from all successful API calls,
            empty DataFrame if all requests failed
        """
        timeout = aiohttp.ClientTimeout(total=self.client.DEFAULT_TIMEOUT)

        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=self.client.headers
        ) as session:
            self._check_requests()
            responses = await self._fetch_responses(session, self.requests)

            dfs = []
            for res in responses:
                if isinstance(res, Exception):
                    self.client.logger.error(f"API call failed: {res}")
                    continue
            
                df = self._process_response(res)
                if not isinstance(df, pd.DataFrame):
                    self.client.logger.error(
                        f"_process_response must return DataFrame, got {type(df)}"
                    )
                    continue

                dfs.append(df)
            
            return pd.concat(dfs, ignore_index=True, sort=False) if dfs else pd.DataFrame()

    @save_api_response(activate=False)
    async def _fetch_responses(self, session: aiohttp.ClientSession, 
                               requests: List[Tuple]) -> List[Any]:
        """
        Fetch multiple API responses concurrently.
        
        Parameters
        ----------
        session : aiohttp.ClientSession
            Active HTTP session for making requests
        requests : List[Tuple]
            List of (url, params, metadata) tuples
        
        Returns
        -------
        List[Any]
            API responses or Exception objects for failed requests
        """
        return await self.client.fetch_all(session, requests)

    def _check_requests(self) -> None:
        """
        Validate request count against maximum allowed.
        
        Raises
        ------
        Warning
            If number of requests exceeds max_requests threshold
        """
        if len(self.requests) > self.max_requests:
            raise Warning(
                f"Request count ({len(self.requests)}) exceeds "
                f"maximum allowed ({self.max_requests})"
            )

    def _validate_and_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply schema-based type validation and conversion to DataFrame.
        """
        if self.schema is None:
            return df

        schema_instance = self.schema()
        dtype_map = schema_instance.to_dtype_dict()

        # Colonnes du schéma présentes dans les données
        schema_cols_in_data = set(dtype_map.keys()) & set(df.columns)
        
        # Colonnes du schéma absentes des données
        missing_cols = set(dtype_map.keys()) - set(df.columns)
        if missing_cols:
            self.client.logger.debug(
                f"Schema columns not in data (skipped): {missing_cols}"
            )

        # Conversion uniquement des colonnes présentes
        for col in schema_cols_in_data:
            dtype = dtype_map[col]
            
            try:
                if dtype == "string":
                    df[col] = df[col].astype("string")
                elif dtype == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype in ("float32", "float64"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "boolean":
                    df[col] = df[col].astype("boolean")
                else:
                    df[col] = df[col].astype(dtype)

            except Exception as e:
                self.client.logger.warning(
                    f"Failed to convert column '{col}' to {dtype}: {e}"
                )
        
        # Colonnes supplémentaires dans les données
        extra_cols = set(df.columns) - set(dtype_map.keys())
        if extra_cols:
            self.client.logger.debug(
                f"Extra columns in data (preserved): {extra_cols}"
            )

        return df

    async def run(self) -> DataFrameInterchange:
        """
        Execute the complete data extraction pipeline.
        
        Pipeline steps:
        1. Validate inputs (_check_inputs)
        2. Build requests (_build_request)
        3. Execute API calls (_call_api)
        4. Validate and convert types (_validate_and_convert_types)
        
        Returns
        -------
        DataFrameInterchange
            Wrapper containing the final processed DataFrame
        """
        self._check_inputs()
        self._build_request()

        df = await self._call_api()
        df = self._validate_and_convert_types(df)

        return DataFrameInterchange(df)