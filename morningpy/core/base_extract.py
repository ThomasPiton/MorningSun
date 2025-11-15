from abc import ABC, abstractmethod
import aiohttp
from typing import Any,Type,Optional
import pandas as pd

from morningpy.core.interchange import DataFrameInterchange

class BaseExtractor(ABC):
    """Handles extraction pipeline for single or multiple async calls."""

    def __init__(self, client):
        self.client = client
        self.url: str = ""
        self.params: Any = None 
    
    @abstractmethod
    def _check_inputs(self) -> None:
        pass

    @abstractmethod
    def _build_request(self) -> None:
        pass

    @abstractmethod
    def _process_response(self, response: Any) -> pd.DataFrame:
        pass

    async def _call_api(self) -> pd.DataFrame:
        
        """Handles single or multiple async API calls depending on url/params type."""
        
        timeout = aiohttp.ClientTimeout(total=self.client.DEFAULT_TIMEOUT)
        
        async with aiohttp.ClientSession(timeout=timeout, headers=self.client.headers) as session:

            requests = []

            if isinstance(self.url, list):
                requests = [(url, self.params) for url in self.url]

            elif isinstance(self.params, list):
                requests = [(self.url, p) for p in self.params]

            else:
                requests = [(self.url, self.params)]

            responses = await self.client.fetch_all(session, requests)

            dfs = []
            for res in responses:
                if isinstance(res, Exception):
                    self.client.logger.error(res)
                    continue
                dfs.append(self._process_response(res))

            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _validate_and_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and convert DataFrame types based on schema"""
        if self.schema is None:
            return df
        
        schema_instance = self.schema()
        dtypes = schema_instance.to_dtype_dict()
        
        for col, dtype in dtypes.items():
            if col not in df.columns:
                continue
            
            try:
                if dtype == 'string':
                    df[col] = df[col].astype('string')
                elif dtype == 'Int64':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype == 'float64':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif dtype == 'boolean':
                    df[col] = df[col].astype('boolean')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                self.client.logger.warning(f"Failed to convert {col} to {dtype}: {e}")
        
        return df

    async def run(self) -> DataFrameInterchange:
        """Unified async extraction pipeline."""
        self._check_inputs()
        self._build_request()
        df = await self._call_api()
        df = self._validate_and_convert_types(df)
        return DataFrameInterchange(df)