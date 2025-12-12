"""Tests for decorators module."""
import pytest
import asyncio
import time
import json
import logging
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open, call
from morningpy.core.decorator import (
    retry,
    save_api_response,
    save_dataframe_mock,
    save_api_request
)
from morningpy.core.config import CoreConfig
import pandas as pd


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provide a mock logger for testing."""
    return Mock(spec=logging.Logger)


@pytest.fixture
def temp_fixture_dir(tmp_path):
    """Provide a temporary fixture directory."""
    fixture_dir = tmp_path / "data" / "fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    return fixture_dir


@pytest.fixture
def temp_mock_dir(tmp_path):
    """Provide a temporary mock directory."""
    mock_dir = tmp_path / "data" / "mock"
    mock_dir.mkdir(parents=True, exist_ok=True)
    return mock_dir


@pytest.fixture
def temp_request_dir(tmp_path):
    """Provide a temporary request directory."""
    request_dir = tmp_path / "data" / "request"
    request_dir.mkdir(parents=True, exist_ok=True)
    return request_dir


@pytest.fixture
def sample_dataframe():
    """Provide a sample DataFrame for testing."""
    return pd.DataFrame({
        'col1': [1, 2, 3, 4, 5, 6, 7],
        'col2': ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    })


@pytest.fixture
def mock_extractor_class():
    """Provide a mock extractor class."""
    class MockExtractor:
        def __init__(self):
            self.client = Mock()
            self.client.logger = Mock(spec=logging.Logger)
            self.logger = Mock(spec=logging.Logger)
            self.__class__.__name__ = "TestExtractor"
    
    return MockExtractor


# ============================================================================
# RETRY DECORATOR TESTS - SYNC
# ============================================================================

class TestRetrySyncFunctions:
    """Test suite for retry decorator with synchronous functions."""
    
    def test_successful_execution_no_retry(self, mock_logger):
        """Test that successful function executes once without retry."""
        @retry(max_retries=3)
        def successful_func():
            return "success"
        
        with patch('logging.getLogger', return_value=mock_logger):
            result = successful_func()
        
        assert result == "success"
        mock_logger.warning.assert_not_called()
    
    def test_retries_on_exception(self, mock_logger):
        """Test that function retries on exception."""
        attempt_count = [0]
        
        @retry(max_retries=3, backoff_factor=0.01)
        def failing_func():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Test error")
            return "success"
        
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('time.sleep'):
                result = failing_func()
        
        assert result == "success"
        assert attempt_count[0] == 3
        assert mock_logger.warning.call_count == 2
    
    def test_specific_exception_types(self, mock_logger):
        """Test that only specified exceptions trigger retry."""
        @retry(max_retries=3, backoff_factor=0.01, exceptions=(ValueError,))
        def func_with_specific_exception():
            raise TypeError("Wrong exception type")
        
        with patch('logging.getLogger', return_value=mock_logger):
            with pytest.raises(TypeError):
                func_with_specific_exception()
        
        mock_logger.warning.assert_not_called()
    
    def test_multiple_exception_types(self, mock_logger):
        """Test retry with multiple exception types."""
        attempt_count = [0]
        
        @retry(max_retries=3, backoff_factor=0.01, exceptions=(ValueError, TypeError))
        def func_with_multiple_exceptions():
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise ValueError("First error")
            elif attempt_count[0] == 2:
                raise TypeError("Second error")
            return "success"
        
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('time.sleep'):
                result = func_with_multiple_exceptions()
        
        assert result == "success"
        assert mock_logger.warning.call_count == 2
    
    def test_preserves_function_metadata(self):
        """Test that decorator preserves function metadata."""
        @retry(max_retries=3)
        def documented_func():
            """This is a docstring."""
            pass
        
        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "This is a docstring."
    
    def test_function_with_arguments(self, mock_logger):
        """Test retry with function arguments."""
        @retry(max_retries=2, backoff_factor=0.01)
        def func_with_args(a, b, c=3):
            if a < 5:
                raise ValueError("a too small")
            return a + b + c
        
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('time.sleep'):
                with pytest.raises(ValueError):
                    func_with_args(1, 2, c=4)
    
    def test_function_with_return_value(self, mock_logger):
        """Test that return value is preserved."""
        @retry(max_retries=3)
        def func_with_return():
            return {"key": "value", "number": 42}
        
        with patch('logging.getLogger', return_value=mock_logger):
            result = func_with_return()
        
        assert result == {"key": "value", "number": 42}


# ============================================================================
# RETRY DECORATOR TESTS - ASYNC
# ============================================================================

class TestRetryAsyncFunctions:
    """Test suite for retry decorator with asynchronous functions."""
    
    @pytest.mark.asyncio
    async def test_async_successful_execution(self, mock_logger):
        """Test that successful async function executes once."""
        @retry(max_retries=3)
        async def async_successful_func():
            return "async success"
        
        with patch('logging.getLogger', return_value=mock_logger):
            result = await async_successful_func()
        
        assert result == "async success"
        mock_logger.warning.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_async_retries_on_exception(self, mock_logger):
        """Test that async function retries on exception."""
        attempt_count = [0]
        
        @retry(max_retries=3, backoff_factor=0.01)
        async def async_failing_func():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Async test error")
            return "async success"
        
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('asyncio.sleep'):
                result = await async_failing_func()
        
        assert result == "async success"
        assert attempt_count[0] == 3
        assert mock_logger.warning.call_count == 2

    
    @pytest.mark.asyncio
    async def test_async_with_arguments(self, mock_logger):
        """Test async retry with function arguments."""
        @retry(max_retries=2, backoff_factor=0.01)
        async def async_func_with_args(x, y, z=10):
            if x < 5:
                raise ValueError("x too small")
            return x + y + z
        
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('asyncio.sleep'):
                result = await async_func_with_args(10, 20, z=30)
        
        assert result == 60
    
    @pytest.mark.asyncio
    async def test_async_preserves_metadata(self):
        """Test that decorator preserves async function metadata."""
        @retry(max_retries=3)
        async def async_documented_func():
            """This is an async docstring."""
            pass
        
        assert async_documented_func.__name__ == "async_documented_func"
        assert async_documented_func.__doc__ == "This is an async docstring."
    
    @pytest.mark.asyncio
    async def test_async_specific_exception_types(self, mock_logger):
        """Test that only specified exceptions trigger async retry."""
        @retry(max_retries=3, backoff_factor=0.01, exceptions=(ValueError,))
        async def async_func_with_type_error():
            raise TypeError("Wrong exception")
        
        with patch('logging.getLogger', return_value=mock_logger):
            with pytest.raises(TypeError):
                await async_func_with_type_error()
        
        mock_logger.warning.assert_not_called()


# ============================================================================
# SAVE_API_RESPONSE DECORATOR TESTS
# ============================================================================

class TestSaveApiResponse:
    """Test suite for save_api_response decorator."""
    
    @pytest.mark.asyncio
    async def test_noop_when_not_activated(self):
        """Test that decorator does nothing when activate=False."""
        @save_api_response(activate=False)
        async def mock_fetch_all(self, session, requests):
            return [{"data": "test"}]
        
        mock_self = Mock()
        result = await mock_fetch_all(mock_self, None, [])
        
        assert result == [{"data": "test"}]
    
    @pytest.mark.asyncio
    async def test_handles_empty_responses(self):
        """Test that empty response list is handled gracefully."""
        mock_self = Mock()
        mock_self.__class__.__name__ = "TestExtractor"
        mock_self.logger = Mock()
        
        @save_api_response(activate=True)
        async def mock_fetch_all(self, session, requests):
            return []
        
        result = await mock_fetch_all(mock_self, None, [])
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_preserves_function_signature(self):
        """Test that decorator preserves function signature."""
        @save_api_response(activate=True)
        async def mock_fetch_all(self, session, requests):
            """Fetch all data."""
            return []
        
        assert mock_fetch_all.__name__ == "mock_fetch_all"
        assert "Fetch all data" in mock_fetch_all.__doc__
    
   

# ============================================================================
# SAVE_DATAFRAME_MOCK DECORATOR TESTS
# ============================================================================

class TestSaveDataFrameMock:
    """Test suite for save_dataframe_mock decorator."""
    
    @pytest.mark.asyncio
    async def test_noop_when_not_activated(self, sample_dataframe):
        """Test that decorator does nothing when activate=False."""
        @save_dataframe_mock(activate=False)
        async def mock_call_api(self):
            return sample_dataframe
        
        mock_self = Mock()
        result = await mock_call_api(mock_self)
        
        pd.testing.assert_frame_equal(result, sample_dataframe)
    
    @pytest.mark.asyncio
    async def test_handles_empty_dataframe(self):
        """Test that empty DataFrame is handled gracefully."""
        mock_self = Mock()
        mock_self.__class__.__name__ = "TestExtractor"
        mock_self.client = Mock()
        
        empty_df = pd.DataFrame()
        
        @save_dataframe_mock(activate=True)
        async def mock_call_api(self):
            return empty_df
        
        result = await mock_call_api(mock_self)
        
        assert result.empty
    
    @pytest.mark.asyncio
    async def test_handles_none_dataframe(self):
        """Test that None DataFrame is handled gracefully."""
        mock_self = Mock()
        mock_self.__class__.__name__ = "TestExtractor"
        
        @save_dataframe_mock(activate=True)
        async def mock_call_api(self):
            return None
        
        result = await mock_call_api(mock_self)
        
        assert result is None

    
    @pytest.mark.asyncio
    async def test_preserves_function_signature(self):
        """Test that decorator preserves function signature."""
        @save_dataframe_mock(activate=True)
        async def mock_call_api(self):
            """Call API and return DataFrame."""
            return pd.DataFrame()
        
        assert mock_call_api.__name__ == "mock_call_api"
        assert "Call API" in mock_call_api.__doc__


# ============================================================================
# SAVE_API_REQUEST DECORATOR TESTS
# ============================================================================

class TestSaveApiRequest:
    """Test suite for save_api_request decorator."""
    
    def test_noop_when_not_activated(self):
        """Test that decorator does nothing when activate=False."""
        @save_api_request(activate=False)
        def mock_build_request(self):
            return [("https://api.example.com", {"param": "value"})]
        
        mock_self = Mock()
        result = mock_build_request(mock_self)
        
        assert result == [("https://api.example.com", {"param": "value"})]
    
    def test_handles_empty_requests_list(self):
        """Test that empty requests list is handled gracefully."""
        mock_self = Mock()
        mock_self.__class__.__name__ = "TestExtractor"
        
        @save_api_request(activate=True)
        def mock_build_request(self):
            return []
        
        result = mock_build_request(mock_self)
        
        assert result == []
    
    def test_preserves_function_signature(self):
        """Test that decorator preserves function signature."""
        @save_api_request(activate=True)
        def mock_build_request(self):
            """Build API request."""
            return []
        
        assert mock_build_request.__name__ == "mock_build_request"
        assert "Build API request" in mock_build_request.__doc__


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestDecoratorsIntegration:
    """Integration tests for decorators workflows."""
    
    @pytest.mark.asyncio
    async def test_retry_with_real_async_sleep(self):
        """Test retry with actual async sleep (short duration)."""
        attempt_count = [0]
        
        @retry(max_retries=3, backoff_factor=0.01)
        async def failing_then_success():
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise ValueError("Not yet")
            return "success"
        
        result = await failing_then_success()
        
        assert result == "success"
        assert attempt_count[0] == 2
    
    def test_retry_with_real_sleep(self):
        """Test retry with actual sleep (short duration)."""
        attempt_count = [0]
        
        @retry(max_retries=3, backoff_factor=0.01)
        def failing_then_success():
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise ValueError("Not yet")
            return "success"
        
        result = failing_then_success()
        
        assert result == "success"
        assert attempt_count[0] == 2
    
    @pytest.mark.asyncio
    async def test_multiple_decorators_on_same_function(self, sample_dataframe):
        """Test that multiple decorators can be stacked."""
        mock_self = Mock()
        mock_self.__class__.__name__ = "TestExtractor"
        mock_self.client = Mock()
        mock_self.client.logger = Mock()
        
        @save_dataframe_mock(activate=False)
        @retry(max_retries=2, backoff_factor=0.01)
        async def decorated_function(self):
            return sample_dataframe
        
        result = await decorated_function(mock_self)
        
        pd.testing.assert_frame_equal(result, sample_dataframe)
    
    def test_decorator_activation_toggle(self):
        """Test that decorator can be toggled on and off."""
        mock_self = Mock()
        
        @save_api_request(activate=False)
        def build_request_inactive(self):
            return [("url", {})]
        
        @save_api_request(activate=True)
        def build_request_active(self):
            return [("url", {})]
        
        # Both should return the same result
        result1 = build_request_inactive(mock_self)
        result2 = build_request_active(mock_self)
        
        assert result1 == result2
