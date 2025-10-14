import aiohttp
import requests
import logging
import asyncio
from typing import Any, Dict, List

from morningsun.core.auth import AuthManager
from morningsun.core.utils import retry


class BaseClient:
    """Handles low-level network communication (sync + async) with retry."""

    DEFAULT_TIMEOUT = 20
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2

    def __init__(self, auth_type, url: str = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth_type = auth_type
        self.url = url
        self.auth_manager = AuthManager()
        self.session = requests.Session()
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        """Return headers, with optional URL for WAF token extraction."""
        return self.auth_manager.get_headers(self.auth_type, self.url)

    @retry(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
    def get(self, url: str, **kwargs) -> Dict[str, Any]:
        """Sync GET request with retry decorator."""
        # headers = kwargs.pop("headers", None) or self._get_headers()
        response = self.session.get(url, headers=self.headers, timeout=self.DEFAULT_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response.json()

    @retry(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
    async def get_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Async GET request with retry decorator."""
        # headers = kwargs.pop("headers", None) or self._get_headers()
        async with session.get(url, headers=self.headers, timeout=self.DEFAULT_TIMEOUT, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    async def get_async_multi(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params_list: List[Dict[str, Any]]
    ) -> List[Any]:
        """Fetch multiple requests concurrently."""
        tasks = [self.get_async(session, url, params=p) for p in params_list]
        return await asyncio.gather(*tasks, return_exceptions=True)

    def close_session(self):
        self.session.close()

    def __del__(self):
        self.close_session()
