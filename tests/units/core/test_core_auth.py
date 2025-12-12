
"""Tests for authentication module."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from selenium.common.exceptions import WebDriverException

from morningpy.core.auth import AuthType, AuthManager
from morningpy.core.cache import Cache


@pytest.fixture
def auth_manager():
    """Provide a fresh AuthManager instance for each test."""
    with patch.object(Cache, '__init__', return_value=None):
        with patch.object(Cache, 'get', return_value=None):
            with patch.object(Cache, 'set', return_value=None):
                manager = AuthManager()
                manager.cache = Mock(spec=Cache)
                return manager


@pytest.fixture
def mock_requests_get():
    """Mock requests.get for HTTP calls."""
    with patch('morningpy.core.auth.requests.get') as mock_get:
        yield mock_get


@pytest.fixture
def mock_webdriver():
    """Mock Selenium WebDriver."""
    with patch('morningpy.core.auth.webdriver.Chrome') as mock_chrome:
        yield mock_chrome


class TestAuthType:
    """Test suite for AuthType enum."""
    
    def test_auth_type_values(self):
        """Test that AuthType has correct values."""
        assert AuthType.API_KEY.value == "apikey"
        assert AuthType.BEARER_TOKEN.value == "bearer"
        assert AuthType.WAF_TOKEN.value == "waf"
        assert AuthType.NONE.value == "none"
    
    def test_auth_type_members(self):
        """Test that all expected members exist."""
        members = [e.name for e in AuthType]
        assert "API_KEY" in members
        assert "BEARER_TOKEN" in members
        assert "WAF_TOKEN" in members
        assert "NONE" in members
    
    def test_auth_type_count(self):
        """Test that AuthType has exactly 4 members."""
        assert len(list(AuthType)) == 4


class TestAuthManagerInit:
    """Test suite for AuthManager initialization."""
    
    def test_init_creates_cache(self):
        """Test that initialization creates a Cache instance."""
        with patch.object(Cache, '__init__', return_value=None):
            manager = AuthManager()
            assert hasattr(manager, 'cache')
            assert isinstance(manager.cache, Cache)
    
    def test_init_sets_default_values(self, auth_manager):
        """Test that initialization sets all tokens to None."""
        assert auth_manager._maas_token is None
        assert auth_manager._api_key is None
        assert auth_manager._token_real_time is None
        assert auth_manager._waf_token is None
    
    def test_init_loads_config(self, auth_manager):
        """Test that initialization loads headers and URLs from config."""
        assert hasattr(auth_manager, '_headers')
        assert hasattr(auth_manager, '_urls')
        assert isinstance(auth_manager._headers, dict)
        assert isinstance(auth_manager._urls, dict)


class TestGetMaasToken:
    """Test suite for get_maas_token method."""
    
    def test_returns_cached_token_if_available(self, auth_manager):
        """Test that cached in-memory token is returned."""
        auth_manager._maas_token = "cached_token_123"
        
        result = auth_manager.get_maas_token()
        
        assert result == "cached_token_123"
    
    def test_fetches_new_token_when_force_refresh(self, auth_manager, mock_requests_get):
        """Test that force_refresh bypasses cache and fetches new token."""
        auth_manager._maas_token = "old_token"
        mock_response = Mock()
        mock_response.text = "new_token_456"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_maas_token(force_refresh=True)
        
        assert result == "new_token_456"
        assert auth_manager._maas_token == "new_token_456"
        auth_manager.cache.set.assert_called_once_with("maas_token", "new_token_456")
    
    def test_fetches_new_token_when_no_cache(self, auth_manager, mock_requests_get):
        """Test that new token is fetched when no cached value exists."""
        mock_response = Mock()
        mock_response.text = "fresh_token_789"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_maas_token()
        
        assert result == "fresh_token_789"
        mock_requests_get.assert_called_once()
    
    def test_uses_persistent_cache_on_empty_response(self, auth_manager, mock_requests_get):
        """Test fallback to persistent cache when server returns empty."""
        auth_manager.cache.get.return_value = "persistent_cached_token"
        mock_response = Mock()
        mock_response.text = ""
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_maas_token()
        
        assert result == "persistent_cached_token"
        auth_manager.cache.get.assert_called_once_with("maas_token")
    
    def test_raises_error_when_no_token_available(self, auth_manager, mock_requests_get):
        """Test that ValueError is raised when no token can be obtained."""
        auth_manager.cache.get.return_value = None
        mock_response = Mock()
        mock_response.text = ""
        mock_requests_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="Empty MAAS token and no cached token available"):
            auth_manager.get_maas_token()
    
    def test_handles_network_error_with_cache(self, auth_manager, mock_requests_get):
        """Test that network errors fall back to cached value."""
        auth_manager.cache.get.return_value = "backup_token"
        mock_requests_get.side_effect = requests.RequestException("Network error")
        
        result = auth_manager.get_maas_token()
        
        assert result == "backup_token"
    
    def test_strips_whitespace_from_token(self, auth_manager, mock_requests_get):
        """Test that whitespace is stripped from retrieved token."""
        mock_response = Mock()
        mock_response.text = "  token_with_spaces  \n"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_maas_token()
        
        assert result == "token_with_spaces"


class TestGetApiKey:
    """Test suite for get_api_key method."""
    
    def test_returns_cached_api_key(self, auth_manager):
        """Test that cached API key is returned."""
        auth_manager._api_key = "cached_key_abc"
        
        result = auth_manager.get_api_key()
        
        assert result == "cached_key_abc"
    
    def test_extracts_api_key_with_colon_separator(self, auth_manager, mock_requests_get):
        """Test extraction of API key with colon separator."""
        mock_response = Mock()
        mock_response.text = 'keyApigee: "extracted_key_123"'
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key()
        
        assert result == "extracted_key_123"
    
    def test_extracts_api_key_with_equals_separator(self, auth_manager, mock_requests_get):
        """Test extraction of API key with equals separator."""
        mock_response = Mock()
        mock_response.text = 'keyApigee = "extracted_key_456"'
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key()
        
        assert result == "extracted_key_456"
    
    def test_extracts_api_key_with_single_quotes(self, auth_manager, mock_requests_get):
        """Test extraction of API key with single quotes."""
        mock_response = Mock()
        mock_response.text = "keyApigee: 'single_quote_key'"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key()
        
        assert result == "single_quote_key"
    
    def test_uses_persistent_cache_on_extraction_failure(self, auth_manager, mock_requests_get):
        """Test fallback to persistent cache when extraction fails."""
        auth_manager.cache.get.return_value = "cached_key_backup"
        mock_response = Mock()
        mock_response.text = "no key here"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key()
        
        assert result == "cached_key_backup"
    
    def test_raises_error_when_no_key_found(self, auth_manager, mock_requests_get):
        """Test that ValueError is raised when no key can be found."""
        auth_manager.cache.get.return_value = None
        mock_response = Mock()
        mock_response.text = "no key here"
        mock_requests_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="API key not found in response or cache"):
            auth_manager.get_api_key()
    
    def test_caches_successfully_extracted_key(self, auth_manager, mock_requests_get):
        """Test that successfully extracted key is cached."""
        mock_response = Mock()
        mock_response.text = 'keyApigee: "new_key_789"'
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key()
        
        auth_manager.cache.set.assert_called_once_with("apikey", "new_key_789")
    
    def test_force_refresh_bypasses_cache(self, auth_manager, mock_requests_get):
        """Test that force_refresh fetches new key."""
        auth_manager._api_key = "old_key"
        mock_response = Mock()
        mock_response.text = 'keyApigee: "fresh_key"'
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_api_key(force_refresh=True)
        
        assert result == "fresh_key"
        assert auth_manager._api_key == "fresh_key"


class TestGetTokenRealTime:
    """Test suite for get_token_real_time method."""
    
    def test_returns_cached_token(self, auth_manager):
        """Test that cached real-time token is returned."""
        auth_manager._token_real_time = "cached_rt_token"
        
        result = auth_manager.get_token_real_time()
        
        assert result == "cached_rt_token"
    
    def test_extracts_token_from_response(self, auth_manager, mock_requests_get):
        """Test extraction of real-time token."""
        mock_response = Mock()
        mock_response.text = 'tokenRealtime: "realtime_token_123"'
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_token_real_time()
        
        assert result == "realtime_token_123"
    
    def test_uses_persistent_cache_on_failure(self, auth_manager, mock_requests_get):
        """Test fallback to persistent cache."""
        auth_manager.cache.get.return_value = "cached_rt_backup"
        mock_response = Mock()
        mock_response.text = "no token here"
        mock_requests_get.return_value = mock_response
        
        result = auth_manager.get_token_real_time()
        
        assert result == "cached_rt_backup"
    
    def test_raises_error_when_no_token_available(self, auth_manager, mock_requests_get):
        """Test that ValueError is raised when no token found."""
        auth_manager.cache.get.return_value = None
        mock_response = Mock()
        mock_response.text = "no token"
        mock_requests_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="Real-time token not found"):
            auth_manager.get_token_real_time()


class TestGetWafToken:
    """Test suite for get_waf_token method."""
    
    def test_returns_cached_waf_token(self, auth_manager):
        """Test that cached WAF token is returned."""
        auth_manager._waf_token = "cached_waf_token"
        
        result = auth_manager.get_waf_token()
        
        assert result == "cached_waf_token"
    
    def test_uses_persistent_cache_on_selenium_failure(self, auth_manager, mock_webdriver):
        """Test fallback to cache when Selenium fails."""
        auth_manager.cache.get.return_value = "cached_waf_backup"
        mock_webdriver.side_effect = WebDriverException("Browser error")
        
        result = auth_manager.get_waf_token()
        
        assert result == "cached_waf_backup"
    
    def test_raises_error_when_no_token_found(self, auth_manager, mock_webdriver):
        """Test that ValueError is raised when no token found."""
        auth_manager.cache.get.return_value = None
        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [
            {"name": "session", "value": "no_waf_here"}
        ]
        mock_webdriver.return_value = mock_driver
        
        with pytest.raises(ValueError, match="WAF token not found"):
            auth_manager.get_waf_token()
    
    def test_cleans_up_driver_on_success(self, auth_manager, mock_webdriver):
        """Test that WebDriver is properly closed after success."""
        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [
            {"name": "waf-token", "value": "token_123"}
        ]
        mock_webdriver.return_value = mock_driver
        
        auth_manager.get_waf_token()
        
        mock_driver.quit.assert_called_once()
    
    def test_cleans_up_driver_on_failure(self, auth_manager, mock_webdriver):
        """Test that WebDriver is closed even on failure."""
        auth_manager.cache.get.return_value = "cached_token"
        mock_driver = MagicMock()
        mock_driver.get_cookies.side_effect = Exception("Cookie error")
        mock_webdriver.return_value = mock_driver
        
        auth_manager.get_waf_token()
        
        mock_driver.quit.assert_called_once()
    
    def test_accepts_custom_url(self, auth_manager, mock_webdriver):
        """Test that custom URL is used."""
        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [
            {"name": "waf", "value": "token"}
        ]
        mock_webdriver.return_value = mock_driver
        custom_url = "https://custom.example.com"
        
        auth_manager.get_waf_token(url=custom_url)
        
        mock_driver.get.assert_called_once_with(custom_url)
        

class TestFetchUrl:
    """Test suite for _fetch_url method."""
    
    def test_successful_request(self, auth_manager, mock_requests_get):
        """Test successful HTTP request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        
        result = auth_manager._fetch_url("https://example.com")
        
        assert result == mock_response
        mock_requests_get.assert_called_once()
    
    def test_uses_default_headers(self, auth_manager, mock_requests_get):
        """Test that default headers are used."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        
        auth_manager._fetch_url("https://example.com")
        
        call_kwargs = mock_requests_get.call_args[1]
        assert 'headers' in call_kwargs
        assert call_kwargs['headers'] == auth_manager._headers
    
    def test_sets_timeout(self, auth_manager, mock_requests_get):
        """Test that 20 second timeout is set."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        
        auth_manager._fetch_url("https://example.com")
        
        call_kwargs = mock_requests_get.call_args[1]
        assert call_kwargs['timeout'] == 20
    
    def test_raises_on_http_error(self, auth_manager, mock_requests_get):
        """Test that HTTPError is raised on error status."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("Not found")
        mock_requests_get.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            auth_manager._fetch_url("https://example.com")
    
    def test_raises_on_timeout(self, auth_manager, mock_requests_get):
        """Test that Timeout is raised on timeout."""
        mock_requests_get.side_effect = requests.Timeout("Timeout")
        
        with pytest.raises(requests.Timeout):
            auth_manager._fetch_url("https://example.com")


class TestGetHeaders:
    """Test suite for get_headers method."""
    
    def test_returns_headers_for_none_auth(self, auth_manager):
        """Test that NONE auth returns default headers unchanged."""
        result = auth_manager.get_headers(AuthType.NONE)
        
        assert result == auth_manager._headers
    
    def test_adds_api_key_header(self, auth_manager):
        """Test that API_KEY auth adds Apikey header."""
        auth_manager._api_key = "test_api_key"
        
        result = auth_manager.get_headers(AuthType.API_KEY)
        
        assert "Apikey" in result
        assert result["Apikey"] == "test_api_key"
    
    def test_adds_bearer_token_header(self, auth_manager):
        """Test that BEARER_TOKEN auth adds authorization header."""
        auth_manager._maas_token = "test_maas_token"
        
        result = auth_manager.get_headers(AuthType.BEARER_TOKEN)
        
        assert "authorization" in result
        assert result["authorization"] == "Bearer test_maas_token"
    
    def test_adds_waf_token_header(self, auth_manager, mock_webdriver):
        """Test that WAF_TOKEN auth adds x-aws-waf-token header."""
        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [
            {"name": "waf-token", "value": "test_waf_token"}
        ]
        mock_webdriver.return_value = mock_driver
        
        result = auth_manager.get_headers(
            AuthType.WAF_TOKEN,
            url="https://www.morningstar.com/markets/calendar"
        )
        
        assert "x-aws-waf-token" in result
    
    def test_does_not_modify_original_headers(self, auth_manager):
        """Test that original headers dict is not modified."""
        original_headers = auth_manager._headers.copy()
        auth_manager._api_key = "test_key"
        
        result = auth_manager.get_headers(AuthType.API_KEY)
        
        assert auth_manager._headers == original_headers
        assert result != original_headers
    
    def test_includes_default_headers(self, auth_manager):
        """Test that default headers are included in result."""
        auth_manager._api_key = "test_key"
        
        result = auth_manager.get_headers(AuthType.API_KEY)
        
        for key, value in auth_manager._headers.items():
            assert key in result
            assert result[key] == value
    
    @pytest.mark.parametrize("auth_type", [
        AuthType.API_KEY,
        AuthType.BEARER_TOKEN,
        AuthType.NONE
    ])
    def test_handles_all_auth_types(self, auth_manager, auth_type):
        """Test that all auth types can be handled."""
        auth_manager._api_key = "key"
        auth_manager._maas_token = "token"
        
        result = auth_manager.get_headers(auth_type)
        
        assert isinstance(result, dict)
        assert len(result) > 0


# Integration-style tests
@pytest.mark.integration
class TestAuthManagerIntegration:
    """Integration tests for AuthManager workflows."""
    
    def test_token_caching_workflow(self, auth_manager, mock_requests_get):
        """Test complete token caching workflow."""
        # First call fetches token
        mock_response = Mock()
        mock_response.text = "token_123"
        mock_requests_get.return_value = mock_response
        
        token1 = auth_manager.get_maas_token()
        assert token1 == "token_123"
        
        # Second call uses cached token (no HTTP call)
        mock_requests_get.reset_mock()
        token2 = auth_manager.get_maas_token()
        assert token2 == "token_123"
        mock_requests_get.assert_not_called()
        
        # Force refresh fetches new token
        mock_response.text = "token_456"
        token3 = auth_manager.get_maas_token(force_refresh=True)
        assert token3 == "token_456"
        mock_requests_get.assert_called_once()
    
    def test_fallback_chain(self, auth_manager, mock_requests_get):
        """Test complete fallback chain: memory → http → persistent cache."""
        # No memory cache, HTTP fails, persistent cache exists
        auth_manager.cache.get.return_value = "persistent_token"
        mock_requests_get.side_effect = Exception("Network error")
        
        result = auth_manager.get_maas_token()
        
        assert result == "persistent_token"
        auth_manager.cache.get.assert_called_once()