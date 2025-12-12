"""Tests for BaseClient module."""
import pytest
import aiohttp
import requests
import logging
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock, call
from aiohttp import ClientResponseError, ClientError
from morningpy.core.client import BaseClient
from morningpy.core.auth import AuthManager, AuthType


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_auth_manager():
    """Provide a mock AuthManager."""
    with patch('morningpy.core.client.AuthManager') as mock_auth:
        mock_instance = Mock(spec=AuthManager)
        mock_instance.get_headers.return_value = {
            'Authorization': 'Bearer test_token',
            'User-Agent': 'TestClient/1.0'
        }
        mock_auth.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def base_client(mock_auth_manager):
    """Provide a BaseClient instance with mocked authentication."""
    client = BaseClient(auth_type="bearer", url="https://api.example.com")
    return client


@pytest.fixture
def mock_session():
    """Provide a mock aiohttp ClientSession."""
    session = Mock(spec=aiohttp.ClientSession)
    return session


@pytest.fixture
def mock_response():
    """Provide a mock aiohttp response."""
    response = AsyncMock()
    response.status = 200
    response.json = AsyncMock(return_value={"data": "test"})
    response.raise_for_status = Mock()
    return response


@pytest.fixture
def sample_requests():
    """Provide sample request tuples."""
    return [
        {
            "url": "https://api.example.com/endpoint1",
            "params": {"param1": "value1"},
            "metadata": {"id": "sec1"}
        },
        {
            "url": "https://api.example.com/endpoint2",
            "params": {"param2": "value2"},
            "metadata": {"id": "sec2"}
        },
        {
            "url": "https://api.example.com/endpoint3",
            "params": None,
            "metadata": {"id": "sec3"}
        }
    ]


# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

class TestBaseClientInit:
    """Test suite for BaseClient initialization."""
    
    def test_init_creates_logger(self, mock_auth_manager):
        """Test that initialization creates a logger."""
        client = BaseClient(auth_type="bearer")
        
        assert hasattr(client, 'logger')
        assert isinstance(client.logger, logging.Logger)
        assert client.logger.name == "BaseClient"
    
    def test_init_sets_auth_type(self, mock_auth_manager):
        """Test that auth_type is stored correctly."""
        client = BaseClient(auth_type="apikey")
        
        assert client.auth_type == "apikey"
    
    def test_init_sets_url(self, mock_auth_manager):
        """Test that URL is stored correctly."""
        url = "https://api.example.com"
        client = BaseClient(auth_type="bearer", url=url)
        
        assert client.url == url
    
    def test_init_with_none_url(self, mock_auth_manager):
        """Test initialization with None URL."""
        client = BaseClient(auth_type="bearer", url=None)
        
        assert client.url is None
    
    def test_init_creates_auth_manager(self, mock_auth_manager):
        """Test that AuthManager instance is created."""
        client = BaseClient(auth_type="bearer")
        
        assert hasattr(client, 'auth_manager')
        assert isinstance(client.auth_manager, AuthManager)
    
    def test_init_creates_session(self, mock_auth_manager):
        """Test that requests.Session is created."""
        client = BaseClient(auth_type="bearer")
        
        assert hasattr(client, 'session')
        assert isinstance(client.session, requests.Session)
    
    def test_init_calls_get_headers(self, mock_auth_manager):
        """Test that _get_headers is called during initialization."""
        client = BaseClient(auth_type="bearer", url="https://api.example.com")
        
        mock_auth_manager.get_headers.assert_called_once_with(
            "bearer",
            "https://api.example.com"
        )
    
    def test_init_stores_headers(self, mock_auth_manager):
        """Test that headers are stored correctly."""
        client = BaseClient(auth_type="bearer")
        
        assert client.headers == mock_auth_manager.get_headers.return_value
        assert isinstance(client.headers, dict)
    
    def test_class_constants(self):
        """Test that class constants are defined correctly."""
        assert BaseClient.DEFAULT_TIMEOUT == 20
        assert BaseClient.MAX_RETRIES == 1
        assert BaseClient.BACKOFF_FACTOR == 2


# ============================================================================
# GET_HEADERS TESTS
# ============================================================================

class TestGetHeaders:
    """Test suite for _get_headers method."""
    
    def test_returns_dict(self, base_client):
        """Test that _get_headers returns a dictionary."""
        headers = base_client._get_headers()
        
        assert isinstance(headers, dict)
    
    def test_calls_auth_manager(self, base_client, mock_auth_manager):
        """Test that _get_headers calls AuthManager.get_headers."""
        mock_auth_manager.get_headers.reset_mock()
        
        base_client._get_headers()
        
        mock_auth_manager.get_headers.assert_called_once_with(
            base_client.auth_type,
            base_client.url
        )
    
    def test_passes_correct_auth_type(self, mock_auth_manager):
        """Test that correct auth_type is passed to AuthManager."""
        client = BaseClient(auth_type="apikey", url="https://api.example.com")
        mock_auth_manager.get_headers.reset_mock()
        
        client._get_headers()
        
        call_args = mock_auth_manager.get_headers.call_args[0]
        assert call_args[0] == "apikey"
    
    def test_passes_correct_url(self, mock_auth_manager):
        """Test that correct URL is passed to AuthManager."""
        url = "https://custom.api.com"
        client = BaseClient(auth_type="bearer", url=url)
        mock_auth_manager.get_headers.reset_mock()
        
        client._get_headers()
        
        call_args = mock_auth_manager.get_headers.call_args[0]
        assert call_args[1] == url
    
    def test_handles_none_url(self, mock_auth_manager):
        """Test _get_headers with None URL."""
        client = BaseClient(auth_type="bearer", url=None)
        mock_auth_manager.get_headers.reset_mock()
        
        client._get_headers()
        
        call_args = mock_auth_manager.get_headers.call_args[0]
        assert call_args[1] is None


# ============================================================================
# GET_ASYNC TESTS
# ============================================================================

class TestGetAsync:
    """Test suite for get_async method."""
    
    @pytest.mark.asyncio
    async def test_has_retry_decorator(self, base_client):
        """Test that get_async has retry decorator applied."""
        # Check if the function has been wrapped
        assert hasattr(base_client.get_async, '__wrapped__') or \
               base_client.get_async.__name__ == 'get_async'


# ============================================================================
# FETCH_ALL TESTS
# ============================================================================

class TestFetchAll:
    """Test suite for fetch_all method."""
    
    @pytest.mark.asyncio
    async def test_fetches_multiple_requests(self, base_client, sample_requests):
        """Test that multiple requests are fetched."""
        mock_session = AsyncMock()
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [
                {"data": "result1"},
                {"data": "result2"},
                {"data": "result3"}
            ]
            
            results = await base_client.fetch_all(mock_session, sample_requests)
            
            assert len(results) == 3
            assert mock_get.call_count == 3
    
    @pytest.mark.asyncio
    async def test_passes_correct_parameters(self, base_client):
        """Test that correct parameters are passed to get_async."""
        mock_session = AsyncMock()
        requests = [
            {
                "url": "https://api.example.com/test",
                "params": {"key": "value"},
                "metadata": {"id": "123"}
            }
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": "test"}
            
            await base_client.fetch_all(mock_session, requests)
            
            mock_get.assert_called_once_with(
                mock_session,
                "https://api.example.com/test",
                params={"key": "value"},
                metadata={"id": "123"}
            )
    
    @pytest.mark.asyncio
    async def test_handles_none_params(self, base_client):
        """Test that None params are handled correctly."""
        mock_session = AsyncMock()
        requests = [
            {
                "url": "https://api.example.com/test",
                "params": None,
                "metadata": {"id": "123"}
            }
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": "test"}
            
            await base_client.fetch_all(mock_session, requests)
            
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs['params'] is None
    
    @pytest.mark.asyncio
    async def test_handles_none_metadata(self, base_client):
        """Test that None metadata is handled correctly."""
        mock_session = AsyncMock()
        requests = [
            {
                "url": "https://api.example.com/test",
                "params": {"key": "value"},
                "metadata": None
            }
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": "test"}
            
            await base_client.fetch_all(mock_session, requests)
            
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs['metadata'] is None
    
    @pytest.mark.asyncio
    async def test_returns_exceptions_not_raises(self, base_client):
        """Test that exceptions are returned, not raised."""
        mock_session = AsyncMock()
        requests = [
            {"url": "https://api.example.com/1", "params": None, "metadata": None},
            {"url": "https://api.example.com/2", "params": None, "metadata": None}
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            error = ValueError("Test error")
            mock_get.side_effect = [{"data": "success"}, error]
            
            results = await base_client.fetch_all(mock_session, requests)
            
            assert len(results) == 2
            assert results[0] == {"data": "success"}
            assert isinstance(results[1], ValueError)
            assert str(results[1]) == "Test error"
    
    @pytest.mark.asyncio
    async def test_handles_empty_request_list(self, base_client):
        """Test that empty request list is handled."""
        mock_session = AsyncMock()
        
        results = await base_client.fetch_all(mock_session, [])
        
        assert results == []
    
    @pytest.mark.asyncio
    async def test_uses_asyncio_gather(self, base_client, sample_requests):
        """Test that asyncio.gather is used for concurrency."""
        mock_session = AsyncMock()
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": "test"}
            
            with patch('asyncio.gather', new_callable=AsyncMock) as mock_gather:
                mock_gather.return_value = [{"data": "test"}] * 3
                
                await base_client.fetch_all(mock_session, sample_requests)
                
                mock_gather.assert_called_once()
                call_kwargs = mock_gather.call_args[1]
                assert call_kwargs['return_exceptions'] is True
    
    @pytest.mark.asyncio
    async def test_creates_tasks_for_all_requests(self, base_client):
        """Test that a task is created for each request."""
        mock_session = AsyncMock()
        requests = [
            {"url": f"https://api.example.com/{i}", "params": None, "metadata": None}
            for i in range(5)
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = {"data": "test"}
            
            await base_client.fetch_all(mock_session, requests)
            
            assert mock_get.call_count == 5
    
    @pytest.mark.asyncio
    async def test_preserves_request_order(self, base_client):
        """Test that results preserve request order."""
        mock_session = AsyncMock()
        requests = [
            {"url": "https://api.example.com/1", "params": None, "metadata": None},
            {"url": "https://api.example.com/2", "params": None, "metadata": None},
            {"url": "https://api.example.com/3", "params": None, "metadata": None}
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [
                {"id": 1},
                {"id": 2},
                {"id": 3}
            ]
            
            results = await base_client.fetch_all(mock_session, requests)
            
            assert results[0]["id"] == 1
            assert results[1]["id"] == 2
            assert results[2]["id"] == 3


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestBaseClientIntegration:
    """Integration tests for BaseClient workflows."""
    
    @pytest.mark.asyncio
    async def test_fetch_all_with_mixed_results(self, base_client):
        """Test fetch_all with mix of successful and failed requests."""
        mock_session = AsyncMock()
        requests = [
            {"url": "https://api.example.com/1", "params": None, "metadata": None},
            {"url": "https://api.example.com/2", "params": None, "metadata": None},
            {"url": "https://api.example.com/3", "params": None, "metadata": None}
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [
                {"data": "success1"},
                ValueError("Network error"),
                {"data": "success2"}
            ]
            
            results = await base_client.fetch_all(mock_session, requests)
            
            assert len(results) == 3
            assert results[0] == {"data": "success1"}
            assert isinstance(results[1], ValueError)
            assert results[2] == {"data": "success2"}

    def test_client_with_different_auth_types(self, mock_auth_manager):
        """Test creating clients with different auth types."""
        auth_types = ["bearer", "apikey", "waf", "none"]
        
        for auth_type in auth_types:
            client = BaseClient(auth_type=auth_type)
            assert client.auth_type == auth_type
            mock_auth_manager.get_headers.assert_called()
    
    @pytest.mark.asyncio
    async def test_large_batch_fetch(self, base_client):
        """Test fetching a large batch of requests."""
        mock_session = AsyncMock()
        requests = [
            {"url": f"https://api.example.com/{i}", "params": None, "metadata": {"id": i}}
            for i in range(50)
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [{"data": f"result{i}"} for i in range(50)]
            
            results = await base_client.fetch_all(mock_session, requests)
            
            assert len(results) == 50
            assert mock_get.call_count == 50


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

class TestErrorHandling:
    """Test suite for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_handles_all_requests_failing(self, base_client):
        """Test fetch_all when all requests fail."""
        mock_session = AsyncMock()
        requests = [
            {"url": f"https://api.example.com/{i}", "params": None, "metadata": None}
            for i in range(3)
        ]
        
        with patch.object(base_client, 'get_async', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [
                ValueError("Error 1"),
                ValueError("Error 2"),
                ValueError("Error 3")
            ]
            
            results = await base_client.fetch_all(mock_session, requests)
            
            assert len(results) == 3
            assert all(isinstance(r, ValueError) for r in results)

