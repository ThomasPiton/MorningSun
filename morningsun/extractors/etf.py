import pandas as pd
from typing import Any, Dict, List, Union,Tuple
import asyncio
import aiohttp

from morningsun.core.client import BaseClient
from morningsun.core.auth import AuthType
from morningsun.extractors.config import *

class FactorProfileExtractor(BaseClient):
    """Extracts factor profile data for securities."""
    
    BASE_PARAMS = BASE_PARAMS["factor_profile"]
    BASE_URL = URLS['factor_profile']
    
    def __init__(self, id_sec: Union[str, List[str]]):
        super().__init__(auth_type=AuthType.API_KEY)
        self.id_sec = id_sec
        self.is_multiple = isinstance(id_sec, list)
        self.url = None
        self.params = None
    
    def _check_inputs(self) -> bool:
        """Validate input parameters."""
        if self.is_multiple:
            if not self.id_sec or not all(isinstance(s, str) and s for s in self.id_sec):
                raise ValueError("id_sec list must contain non-empty strings")
        else:
            if not isinstance(self.id_sec, str) or not self.id_sec:
                raise ValueError("id_sec must be a non-empty string")
        return True
    
    def _build_url(self, id_sec: str) -> str:
        """Build URL for a single security ID."""
        return f"{self.BASE_URL}/{id_sec}/data"
    
    def _build_request(self) -> None:
        """Build URL and params for single request."""
        if not self.is_multiple:
            self.url = self._build_url(self.id_sec)
            self.params = self.BASE_PARAMS.copy()
    
    def _call_api(self) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """Call API - sync for single, async for multiple IDs."""
        if self.is_multiple:
            return asyncio.run(self._call_api_async())
        return self.get(url=self.url, params=self.params)
    
    async def _fetch_single(
        self,
        session: aiohttp.ClientSession,
        id_sec: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Fetch data for a single security asynchronously."""
        try:
            response = await self._get_async_single(
                session=session,
                url=self._build_url(id_sec),
                params=self.BASE_PARAMS
            )
            return id_sec, response
        except Exception as e:
            self.logger.error(f"Failed to fetch {id_sec}: {e}")
            return id_sec, {}
    
    async def _call_api_async(self) -> Dict[str, Dict[str, Any]]:
        """Make concurrent async requests for multiple IDs."""
        timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=self._get_headers(),
            connector=aiohttp.TCPConnector(limit=10)
        ) as session:
            tasks = [self._fetch_single(session, sec_id) for sec_id in self.id_sec]
            results = await asyncio.gather(*tasks)
            return dict(results)
    
    def _process(self, id_sec: str, response: Dict[str, Any]) -> pd.DataFrame:
        """Extract factor profile from response and return as DataFrame."""
        if not response:
            return pd.DataFrame()
        
        data = response.get("factors", {})
        if not data:
            return pd.DataFrame()
        
        rows = []
        for factor, values in data.items():
            historic_range = values.get("historicRange", [])
            
            # Base row with common values
            base_row = {
                "id_sec": id_sec,
                "factor": factor,
                "categoryAvg": values.get("categoryAvg"),
                "indexAvg": values.get("indexAvg"),
                "percentile": values.get("percentile")
            }
            
            # Add historic range data
            if historic_range:
                for hr in historic_range:
                    row = base_row.copy()
                    row.update({
                        "year": hr.get("year"),
                        "min": hr.get("min"),
                        "max": hr.get("max")
                    })
                    rows.append(row)
            else:
                # If no historic range, add base row with null values
                base_row.update({"year": None, "min": None, "max": None})
                rows.append(base_row)
        
        return pd.DataFrame(rows)
    
    def _process_response(
        self,
        response: Union[Dict[str, Any], Dict[str, Dict[str, Any]]]
    ) -> pd.DataFrame:
        """Process API response based on input type and return DataFrame."""
        if self.is_multiple:

            dfs = [self._process(sec_id, resp)
                for sec_id, resp in response.items()]

            dfs = [df for df in dfs if not df.empty]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        return self._process(self.id_sec, response)
    
    
class InvestmentStrategyExtractor(BaseClient):
    """Extracts investment strategy data for securities."""
    
    BASE_PARAMS = BASE_PARAMS["investment_strategy"]
    BASE_URL = URLS['investment_strategy']
    
    def __init__(self, id_sec: Union[str, List[str]]):
        super().__init__(auth_type=AuthType.API_KEY)
        self.id_sec = id_sec
        self.is_multiple = isinstance(id_sec, list)
        self.url = None
        self.params = None
    
    def _check_inputs(self) -> bool:
        """Validate input parameters."""
        if self.is_multiple:
            if not self.id_sec or not all(isinstance(s, str) and s for s in self.id_sec):
                raise ValueError("id_sec list must contain non-empty strings")
        else:
            if not isinstance(self.id_sec, str) or not self.id_sec:
                raise ValueError("id_sec must be a non-empty string")
        return True
    
    def _build_url(self, id_sec: str) -> str:
        """Build URL for a single security ID."""
        return f"{self.BASE_URL}/{id_sec}/data"
    
    def _build_request(self) -> None:
        """Build URL and params for single request."""
        if not self.is_multiple:
            self.url = self._build_url(self.id_sec)
            self.params = self.BASE_PARAMS.copy()
            
    def _call_api(self) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """Call API - sync for single, async for multiple IDs."""
        if self.is_multiple:
            return asyncio.run(self._call_api_async())
        return self.get(url=self.url, params=self.params)
    
    async def _fetch_single(
        self,
        session: aiohttp.ClientSession,
        id_sec: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Fetch data for a single security asynchronously."""
        try:
            response = await self._get_async_single(
                session=session,
                url=self._build_url(id_sec),
                params=self.BASE_PARAMS
            )
            return id_sec, response
        except Exception as e:
            self.logger.error(f"Failed to fetch {id_sec}: {e}")
            return id_sec, {}
    
    async def _call_api_async(self) -> Dict[str, Dict[str, Any]]:
        """Make concurrent async requests for multiple IDs."""
        timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=self._get_headers(),
            connector=aiohttp.TCPConnector(limit=10)
        ) as session:
            tasks = [self._fetch_single(session, sec_id) for sec_id in self.id_sec]
            results = await asyncio.gather(*tasks)
            return dict(results)
    
    def _process(self, id_sec: str, response: Dict[str, Any]) -> pd.DataFrame:
        """Extract investment strategy from response and return as DataFrame."""
        investment_strategy = response.get("investmentStrategy", "") if response else ""
        data = [{"id_sec": id_sec, "investment_strategy": investment_strategy}]
        return pd.DataFrame(data)
    
    def _process_response(
        self,
        response: Union[Dict[str, Any], Dict[str, Dict[str, Any]]]
    ) -> pd.DataFrame:
        """Process API response based on input type and return DataFrame."""
        if self.is_multiple:
            # Create list of dataframes for each security
            dfs = [
                self._process(sec_id, resp)
                for sec_id, resp in response.items()
            ]
            # Concatenate all dataframes
            return pd.concat(dfs, ignore_index=True)
        
        # Single response
        return self._process(self.id_sec, response)
    
    
class EtfEsgRiskExtractor(BaseClient):
    """Extracts timeseries data for securities from Morningstar."""

    def __init__(self):
        super().__init__(auth_type=AuthType.API_KEY)

    def get(self, id_sec: str) -> str:

        url = f"{URLS["etf_esg_risk"]}/{id_sec}/data"

        params =  {
            "reportType":"A",
            "languageId": "eg",
            "locale": "eg",
            "clientId": "MDC",
            "benchmarkId": "prospectus_primary",
            "component": "sal-mip-esg-risk",
            "version": "4.69.0"
        }

        response = self.request(url=url, params=params, method="GET")
        
        if not response or len(response) == 0:
            return pd.DataFrame()
        
        # Flatten nested dictionary
        flat_data = {}
        for key, value in response.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    flat_data[f"{subkey}"] = subvalue
            else:
                flat_data[key] = value

        # Convert to vertical DataFrame
        df = pd.DataFrame(list(flat_data.items()), columns=["metric", "value"])
        df.set_index("metric", inplace=True)
        return df