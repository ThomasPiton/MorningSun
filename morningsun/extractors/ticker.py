import requests
import pandas as pd
from typing import Optional, Union, List,Dict,Any,Tuple
from pathlib import Path
import os
import asyncio
import aiohttp
import os

from morningsun.core.client import BaseClient
from morningsun.core.auth import AuthType
from morningsun.extractors.config import *

class StockScreenExtractor(BaseClient):
    pass

class EtfScreenExtractor(BaseClient):
    pass

class FundScreenExtractor(BaseClient):
    pass

class AllTickersExtractor(BaseClient):
    """Extracts all available tickers from Morningstar security screener."""
    
    BASE_URL = URLS["security_screener"]
    BASE_PARAMS = BASE_PARAMS["security_screener"]
    CACHE_FILE = "data/all_tickers.csv"
    
    def __init__(self, forced_extract: bool = False):
        super().__init__(auth_type=AuthType.NONE)
        self.forced_extract = forced_extract
        self.max_pages = 10
        self.page_size = 5000
        self.params = None
        self.total_records = None
    
    def _check_inputs(self) -> bool:
        """Validate input parameters."""
        if self.max_pages <= 0:
            raise ValueError("max_pages must be greater than 0")
        if self.page_size <= 0:
            raise ValueError("page_size must be greater than 0")
        
        if not self.forced_extract and not os.path.exists(self.CACHE_FILE):
            self.logger.warning(f"Cache file not found: {self.CACHE_FILE}. Will force extract.")
            self.forced_extract = True
        
        return True
    
    def _build_request(self) -> None:
        """Build params for request."""
        if self.forced_extract:
            self.params = self.BASE_PARAMS.copy()
            self.params['pageSize'] = self.page_size
    
    def _call_api(self) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Call API to retrieve all pages of ticker data or read from cache.
        """
        if not self.forced_extract:
            self.logger.info(f"Reading tickers from cache: {self.CACHE_FILE}")
            try:
                df = pd.read_csv(self.CACHE_FILE)
                self.logger.info(f"Loaded {len(df)} tickers from cache")
                return df
            except Exception as e:
                self.logger.error(f"Failed to read cache file: {e}. Forcing extract.")
                self.forced_extract = True
                self._build_request()
        
        # Fetch asynchronously
        return asyncio.run(self._call_api_async())
    
    def _get_total_pages(self) -> int:
        """Fetch first page to determine total number of pages needed."""
        self.logger.info("Fetching first page to determine total records")
        self.params['page'] = 1
        
        try:
            response_data = self.get(url=self.BASE_URL, params=self.params)
            self.total_records = response_data.get("total", 0)
            
            if self.total_records == 0:
                return 0
            
            total_pages = (self.total_records + self.page_size - 1) // self.page_size
            actual_pages = min(total_pages, self.max_pages)
            
            self.logger.info(f"Total records: {self.total_records}, fetching {actual_pages} pages")
            return actual_pages
            
        except Exception as e:
            self.logger.error(f"Failed to fetch first page: {e}")
            return 0
    
    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        page_num: int
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """Fetch a single page asynchronously."""
        params = self.params.copy()
        params['page'] = page_num
        
        try:
            self.logger.info(f"Fetching page {page_num}")
            response_data = await self._get_async_single(
                session=session,
                url=self.BASE_URL,
                params=params
            )
            
            rows = response_data.get("rows", [])
            return page_num, rows
            
        except Exception as e:
            self.logger.error(f"Failed to fetch page {page_num}: {e}")
            return page_num, []
    
    async def _call_api_async(self) -> List[Dict[str, Any]]:
        """Make concurrent async requests for all pages."""

        total_pages = self._get_total_pages()
        
        if total_pages == 0:
            return []
        
        if total_pages == 1:
            self.params['page'] = 1
            response_data = self.get(url=self.BASE_URL, params=self.params)
            return response_data.get("rows", [])
        
        self.logger.info(f"Fetching pages 2-{total_pages} concurrently")
        
        timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=self._get_headers(),
            connector=aiohttp.TCPConnector(limit=10)) as session:

            tasks = [
                self._fetch_page(session, page_num)
                for page_num in range(2, total_pages + 1)]
            
            self.params['page'] = 1
            first_page_data = self.get(url=self.BASE_URL, params=self.params)
            results = first_page_data.get("rows", [])
            
            page_results = await asyncio.gather(*tasks)
            
            page_results = sorted(page_results, key=lambda x: x[0])
            for page_num, rows in page_results:
                if rows:
                    results.extend(rows)
            
            return results
    
    def _process_response(self, response: Union[List[Dict[str, Any]], pd.DataFrame]) -> pd.DataFrame:
        """Process API response and return DataFrame."""
        if isinstance(response, pd.DataFrame):
            return response
        
        if not response:
            return pd.DataFrame()

        return pd.DataFrame(response)
    



