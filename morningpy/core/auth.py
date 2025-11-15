import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from enum import Enum
from typing import Dict, Optional,Any

from .config import DEFAULT_HEADERS, URLS
from .cache import Cache 

class AuthType(Enum):
    """Enumeration of authentication types."""
    API_KEY = "apikey"
    BEARER_TOKEN = "bearer"
    WAF_TOKEN = "waf"
    NONE = "none"


class AuthManager:
    """Manages authentication tokens and API keys for Morningstar API."""

    def __init__(self):
        self._headers = DEFAULT_HEADERS
        self._maas_token: Optional[str] = None
        self._api_key: Optional[str] = None
        self._token_real_time: Optional[str] = None
        self._waf_token: Optional[str] = None
        self.cache = Cache()  # persistent cache instance

    # ---------------------------------------------------------------------
    # MAIN TOKEN METHODS
    # ---------------------------------------------------------------------

    def get_maas_token(self, force_refresh: bool = False) -> str:
        """Get MAAS token (Bearer token). Try API first, else fallback to cache."""
        cached = self.cache.get("maas_token")

        if self._maas_token and not force_refresh:
            return self._maas_token

        url = URLS["maas_token"]
        try:
            response = self._fetch_url(url)
            token = response.text.strip()
        except Exception:
            token = ""

        # if live fetch failed or empty -> fallback
        if not token:
            if cached:
                print("⚠️ Empty MAAS token response, using cached token.")
                return cached
            raise ValueError("Empty MAAS token and no cached token available.")

        # success → save & return
        self._maas_token = token
        self.cache.set("maas_token", token)
        return token

    def get_api_key(self, force_refresh: bool = False) -> str:
        """Get Apigee API key. Try live first, else fallback to cache."""
        cached = self.cache.get("apikey")

        if self._api_key and not force_refresh:
            return self._api_key

        url = URLS["key_api"]
        try:
            response = self._fetch_url(url)
            match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', response.text)
            api_key = match.group(1) if match else ""
        except Exception:
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
        """Get real-time token. Try live first, else fallback to cache."""
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
        """Extract AWS WAF token using Selenium. Try live first, else fallback to cache."""
        cached = self.cache.get("waf_token")

        if self._waf_token and not force_refresh:
            return self._waf_token

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
        )

        waf_token = ""
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            cookies = driver.get_cookies()
            cookies_dict = {c['name']: c['value'] for c in cookies}

            for name, value in cookies_dict.items():
                if 'waf' in name.lower() or 'token' in name.lower():
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

    # ---------------------------------------------------------------------
    # INTERNAL HELPERS
    # ---------------------------------------------------------------------

    def _fetch_url(self, url: str) -> requests.Response:
        """Fetch URL with error handling."""
        response = requests.get(url, headers=self._headers, timeout=20)
        response.raise_for_status()
        return response

    def get_headers(self, auth_type: AuthType, url: str = None) -> Dict[str, Any]:
        """Get headers with appropriate authentication."""
        headers = DEFAULT_HEADERS.copy()

        if auth_type == AuthType.API_KEY:
            headers["Apikey"] = self.get_api_key()

        elif auth_type == AuthType.BEARER_TOKEN:
            headers["authorization"] = f"Bearer {self.get_maas_token()}"

        elif auth_type == AuthType.WAF_TOKEN:
            waf_token = self.get_waf_token(url)
            headers["x-aws-waf-token"] = waf_token

        return headers


# class AuthManager:
#     """Manages authentication tokens and API keys for Morningstar API."""

#     def __init__(self):

#         self._headers = DEFAULT_HEADERS
#         self._maas_token: Optional[str] = None
#         self._api_key: Optional[str] = None
#         self._token_real_time: Optional[str] = None
#         self._waf_token: Optional[str] = None

#     def get_maas_token(self, force_refresh: bool = False) -> str:
#         """Get MAAS token (Bearer token), cached unless force_refresh is True."""
#         if self._maas_token and not force_refresh:
#             return self._maas_token
        
#         url = URLS["maas_token"]
#         response = self._fetch_url(url)
#         self._maas_token = response.text
#         return self._maas_token

#     def get_api_key(self, force_refresh: bool = False) -> str:
#         """Get Apigee API key, cached unless force_refresh is True."""
#         if self._api_key and not force_refresh:
#             return self._api_key
        
#         url = URLS["key_api"]
#         response = self._fetch_url(url)
#         match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', response.text)
#         if not match:
#             raise ValueError("API key not found in response")
#         self._api_key = match.group(1)
#         return self._api_key
    
#     def get_token_real_time(self, force_refresh: bool = False) -> str:
#         """Get real-time token, cached unless force_refresh is True."""
#         if self._token_real_time and not force_refresh:
#             return self._token_real_time
        
#         url = URLS["key_api"]
#         response = self._fetch_url(url)
#         match = re.search(r'tokenRealtime\s*[:=]\s*"([^"]+)"', response.text)
#         if not match:
#             raise ValueError("Real-time token not found in response")
#         self._token_real_time = match.group(1)
#         return self._token_real_time
    
#     def get_waf_token(self, url: str = "https://www.morningstar.com/markets/calendar", 
#                   force_refresh: bool = False) -> Optional[str]:
#         """
#         Extract AWS WAF token using shared Selenium driver.
        
#         Args:
#             url: Base URL to visit for token generation
#             force_refresh: Force refresh even if token exists
            
#         Returns:
#             tuple: (waf_token, cookies_dict)
#         """
#         chrome_options = Options()
#         chrome_options.add_argument('--headless')
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
#         chrome_options.add_argument('--disable-blink-features=AutomationControlled')
#         chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
#         chrome_options.add_experimental_option('useAutomationExtension', False)
#         chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
        
#         driver = None
#         driver = webdriver.Chrome(options=chrome_options)
#         driver.get(url)
        
#         WebDriverWait(driver, 10).until(
#             lambda d: d.execute_script('return document.readyState') == 'complete'
#         )
        
#         cookies = driver.get_cookies()
#         cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

#         for name, value in cookies_dict.items():
#             if 'waf' in name.lower() or 'token' in name.lower():
#                 self._waf_token = value
        
#         return self._waf_token
        

#     def _fetch_url(self, url: str) -> requests.Response:
#         """Fetch URL with error handling."""
#         try:
#             response = requests.get(url, headers=self._headers, timeout=20)
#             response.raise_for_status()
#             return response
#         except requests.RequestException as e:
#             self.logger.error(f"Failed to fetch {url}: {e}")
#             raise
        
#     def get_headers(self, auth_type: AuthType, url: str = None) -> Dict[str, Any]:
#         """Get headers with appropriate authentication."""
#         headers = DEFAULT_HEADERS.copy()

#         if auth_type == AuthType.API_KEY:
#             headers["Apikey"] = self.get_api_key()

#         elif auth_type == AuthType.BEARER_TOKEN:
#             headers["authorization"] = f"Bearer {self.get_maas_token()}"
#             # headers["authorization"] = f"Bearer {self.get_token_real_time()}"

#         elif auth_type == AuthType.WAF_TOKEN:
#             waf_token = self.get_waf_token(url)
#             headers["x-aws-waf-token"] = waf_token

#         return headers
