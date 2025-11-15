import aiohttp
import requests
import logging
import asyncio
from typing import Any, Dict, List

from morningpy.core.auth import AuthManager
from morningpy.core.utils import retry


class BaseClient:
    """Handles low-level network communication (sync + async) with retry."""

    DEFAULT_TIMEOUT = 20
    MAX_RETRIES = 1
    BACKOFF_FACTOR = 2

    def __init__(self, auth_type, url: str = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth_type = auth_type
        self.url = url
        self.auth_manager = AuthManager()
        self.session = requests.Session()
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        return self.auth_manager.get_headers(self.auth_type, self.url)

    @retry(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
    async def get_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict | None = None,
    ) -> Dict[str, Any]:
        """Async GET request with retry decorator."""
        async with session.get(url, headers=self.headers, timeout=self.DEFAULT_TIMEOUT, params=params) as response:
            response.raise_for_status()
            return await response.json()

    async def fetch_all(self, session: aiohttp.ClientSession, requests: List[tuple[str, dict]]) -> List[Any]:
        """Fetch multiple (url, params) pairs concurrently."""
        tasks = [self.get_async(session, url, params=params) for url, params in requests]
        return await asyncio.gather(*tasks, return_exceptions=True)

# Cogedim - generale / Gasparini 
# dispo samedi ou dimanche ? 
