from abc import ABC, abstractmethod
import aiohttp
from typing import Any,Dict,Optional
import pandas as pd

from morningsun.core.interchange import DataFrameInterchange

class BaseExtractor(ABC):
    """Handles extraction pipeline for single or multiple async calls."""

    def __init__(self, client):
        self.client = client
        self.url: str = ""
        self.params: Any = None  # dict or list of dicts

    @abstractmethod
    def _check_inputs(self) -> None:
        pass

    @abstractmethod
    def _build_request(self) -> None:
        pass

    @abstractmethod
    def _process_response(self, response: Any) -> pd.DataFrame:
        pass

    async def _call_api(self) -> Any:
        """Single or multiple async calls depending on self.params type."""
        timeout = aiohttp.ClientTimeout(total=self.client.DEFAULT_TIMEOUT)
        # headers = self.client._get_headers(self.url)
        
        async with aiohttp.ClientSession(timeout=timeout, headers=self.client.headers) as session:
            if isinstance(self.params, list):
                # multiple concurrent calls
                responses = await self.client.get_async_multi(session, self.url, self.params)
                dfs = []
                for res in responses:
                    if isinstance(res, Exception):
                        self.client.logger.error(res)
                        continue
                    df = self._process_response(res)
                    dfs.append(df)
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            else:
                # single call
                res = await self.client.get_async(session, self.url, params=self.params)
                return self._process_response(res)

    async def run(self) -> DataFrameInterchange:
        """Unified async extraction pipeline."""
        self._check_inputs()
        self._build_request()
        df = await self._call_api()
        return DataFrameInterchange(df)