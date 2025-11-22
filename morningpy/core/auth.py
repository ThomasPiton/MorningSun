import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from enum import Enum
from typing import Dict, Optional, Any
import brotli

from .config import DEFAULT_HEADERS, URLS
from .cache import Cache


class AuthType(Enum):
    """
    Authentication types supported by the Morningstar API.

    - API_KEY: Standard Apigee API key
    - BEARER_TOKEN: MAAS bearer token
    - WAF_TOKEN: AWS WAF browser token (requires Selenium)
    - NONE: No authentication required
    """
    API_KEY = "apikey"
    BEARER_TOKEN = "bearer"
    WAF_TOKEN = "waf"
    NONE = "none"


class AuthManager:
    """
    Handles authentication for all Morningstar API interactions.

    Responsibilities:
    - Retrieve and cache API keys (API key, MAAS token, WAF token)
    - Fallback to persisted cache if live retrieval fails
    - Provide properly authenticated headers based on AuthType
    """

    def __init__(self):
        self._headers = DEFAULT_HEADERS
        self._maas_token: Optional[str] = None
        self._api_key: Optional[str] = None
        self._token_real_time: Optional[str] = None
        self._waf_token: Optional[str] = None
        self.cache = Cache()

    def get_maas_token(self, force_refresh: bool = False) -> str:
        """
        Retrieve the MAAS bearer token.

        Strategy:
        1. Use in-memory token unless force_refresh=True
        2. Attempt live HTTP retrieval
        3. Fallback to cached token if server returns empty
        4. Raise if no valid token is available

        Parameters
        ----------
        force_refresh : bool
            If True, forces retrieval from the live endpoint.

        Returns
        -------
        str
            The MAAS bearer token.
        """
        cached = self.cache.get("maas_token")

        if self._maas_token and not force_refresh:
            return self._maas_token

        url = URLS["maas_token"]
        try:
            response = self._fetch_url(url)
            token = response.text.strip()
        except Exception:
            token = ""

        if not token:
            if cached:
                print("⚠️ Empty MAAS token response, using cached token.")
                return cached
            raise ValueError("Empty MAAS token and no cached token available.")

        self._maas_token = token
        self.cache.set("maas_token", token)
        return token

    def get_api_key(self, force_refresh: bool = False) -> str:
        """
        Retrieve the Apigee API key.

        Extracts the token from a JavaScript snippet:
            keyApigee: "XXXX"

        Falls back to cached value on failure.

        Parameters
        ----------
        force_refresh : bool
            If True, forces live retrieval.

        Returns
        -------
        str
            The Morningstar API key.
        """

        cached = self.cache.get("apikey")
        if self._api_key and not force_refresh:
            return self._api_key

        url = URLS["key_api"]

        try:
            resp = self._fetch_url(url)
            content = resp.text
            pattern = r'keyApigee\s*[:=]\s*["\']([^"\']+)["\']'
            match = re.search(pattern, content)
            api_key = match.group(1) if match else ""

        except Exception as e:
            print(f"⚠️ Error while retrieving API key: {e}")
            api_key = ""

        if not api_key:
            if cached:
                print("⚠️ Empty API key, using cached value.")
                return cached
            raise ValueError("API key not found in response or cache.")

        self._api_key = api_key
        self.cache.set("apikey", api_key)
        return api_key

    def get_token_real_time(self, force_refresh: bool = False) -> str:
        """
        Retrieve the real-time data token (tokenRealtime).

        Extracts from JS payload:
            tokenRealtime: "XXXX"

        Uses cache if the token cannot be retrieved.

        Returns
        -------
        str
            The real-time token.
        """
        cached = self.cache.get("token_real_time")

        if self._token_real_time and not force_refresh:
            return self._token_real_time

        url = URLS["key_api"]
        try:
            response = self._fetch_url(url)
            match = re.search(r'tokenRealtime\s*[:=]\s*"([^"]+)"', response.text)
            token = match.group(1) if match else ""
        except Exception:
            token = ""

        if not token:
            if cached:
                print("⚠️ Empty real-time token, using cached value.")
                return cached
            raise ValueError("Real-time token not found in response or cache.")

        self._token_real_time = token
        self.cache.set("token_real_time", token)
        return token

    def get_waf_token(
        self,
        url: str = "https://www.morningstar.com/markets/calendar",
        force_refresh: bool = False
    ) -> Optional[str]:
        """
        Retrieve AWS WAF token using a Selenium-driven Chrome session.

        Strategy:
        1. Load page headlessly
        2. Extract all cookies
        3. Identify a cookie whose name contains "waf" or "token"
        4. Fallback to cache on failure

        Parameters
        ----------
        url : str
            Target webpage to trigger WAF token generation.
        force_refresh : bool
            Whether to ignore in-memory and force Selenium extraction.

        Returns
        -------
        Optional[str]
            The extracted WAF token.
        """
        cached = self.cache.get("waf_token")

        if self._waf_token and not force_refresh:
            return self._waf_token

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
        )

        waf_token = ""
        driver = None

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)

            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            cookies = driver.get_cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}

            for name, value in cookies_dict.items():
                if "waf" in name.lower() or "token" in name.lower():
                    waf_token = value
                    break

        except Exception as e:
            print(f"⚠️ Selenium WAF token fetch failed: {e}")
            waf_token = ""
        finally:
            if driver:
                driver.quit()

        if not waf_token:
            if cached:
                print("⚠️ Empty WAF token, using cached value.")
                return cached
            raise ValueError("WAF token not found in response or cache.")

        self._waf_token = waf_token
        self.cache.set("waf_token", waf_token)
        return waf_token

    def _fetch_url(self, url: str) -> requests.Response:
        """
        Perform a GET request with default headers and error handling.

        Returns
        -------
        requests.Response
            The validated HTTP response.

        Raises
        ------
        requests.HTTPError
            When the response status is not 200.
        """
        response = requests.get(url, headers=self._headers, timeout=20)
        response.raise_for_status()
        return response

    def get_headers(self, auth_type: AuthType, url: str = None) -> Dict[str, Any]:
        """
        Build headers required for a specific API authentication method.

        Parameters
        ----------
        auth_type : AuthType
            Determines which token is injected into the headers.
        url : str, optional
            URL needed to obtain some tokens (e.g., WAF token).

        Returns
        -------
        Dict[str, Any]
            The full headers dict, including authentication fields.
        """
        headers = DEFAULT_HEADERS.copy()

        if auth_type == AuthType.API_KEY:
            headers["Apikey"] = self.get_api_key()

        elif auth_type == AuthType.BEARER_TOKEN:
            headers["authorization"] = f"Bearer {self.get_maas_token()}"

        elif auth_type == AuthType.WAF_TOKEN:
            headers["x-aws-waf-token"] = self.get_waf_token(url)

        return headers
