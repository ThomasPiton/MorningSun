import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from enum import Enum
from typing import Dict, Optional,Any

from .config import DEFAULT_HEADERS, URLS


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

    def get_maas_token(self, force_refresh: bool = False) -> str:
        """Get MAAS token (Bearer token), cached unless force_refresh is True."""
        if self._maas_token and not force_refresh:
            return self._maas_token
        
        url = URLS["maas_token"]
        response = self._fetch_url(url)
        match = re.search(r'maasToken\s*[:=]\s*"([^"]+)"', response.text)
        if not match:
            raise ValueError("MAAS token not found in response")
        self._maas_token = match.group(1)
        return self._maas_token

    def get_api_key(self, force_refresh: bool = False) -> str:
        """Get Apigee API key, cached unless force_refresh is True."""
        if self._api_key and not force_refresh:
            return self._api_key
        
        url = URLS["key_api"]
        response = self._fetch_url(url)
        match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', response.text)
        if not match:
            raise ValueError("API key not found in response")
        self._api_key = match.group(1)
        return self._api_key
    
    def get_token_real_time(self, force_refresh: bool = False) -> str:
        """Get real-time token, cached unless force_refresh is True."""
        if self._token_real_time and not force_refresh:
            return self._token_real_time
        
        url = URLS["api_key"]
        response = self._fetch_url(url)
        match = re.search(r'tokenRealtime\s*[:=]\s*"([^"]+)"', response.text)
        if not match:
            raise ValueError("Real-time token not found in response")
        self._token_real_time = match.group(1)
        return self._token_real_time
    
    def get_waf_token(self, url: str = "https://www.morningstar.com/markets/calendar", 
                  force_refresh: bool = False) -> Optional[str]:
        """
        Extract AWS WAF token using shared Selenium driver.
        
        Args:
            url: Base URL to visit for token generation
            force_refresh: Force refresh even if token exists
            
        Returns:
            tuple: (waf_token, cookies_dict)
        """
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
        
        driver = None
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        
        cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

        for name, value in cookies_dict.items():
            if 'waf' in name.lower() or 'token' in name.lower():
                self._waf_token = value
        
        return self._waf_token
        

    def _fetch_url(self, url: str) -> requests.Response:
        """Fetch URL with error handling."""
        try:
            response = requests.get(url, headers=self._headers, timeout=20)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise
        
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
