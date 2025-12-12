"""
Comprehensive pytest suite for BaseExtractor module.

Tests cover:
- Initialization and configuration
- Abstract method enforcement
- Input validation
- Request building and validation
- API call execution and error handling
- Response processing
- Schema validation and type conversion
- Complete pipeline execution
"""

import pytest
import pandas as pd
import aiohttp
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Any, List, Tuple, Dict
from abc import ABC

from morningpy.core.base_extract import BaseExtractor
from morningpy.core.interchange import DataFrameInterchange
from morningpy.core.config import CoreConfig


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_client():
    """Create a mock API client with standard attributes."""
    client = Mock()
    client.DEFAULT_TIMEOUT = 30
    client.headers = {"User-Agent": "TestClient"}
    client.logger = Mock()
    client.logger.error = Mock()
    client.logger.warning = Mock()
    client.logger.debug = Mock()
    client.fetch_all = AsyncMock(return_value=[])
    return client


@pytest.fixture
def mock_schema():
    """Create a mock schema class for type validation."""
    class MockSchema:
        def to_dtype_dict(self):
            return {
                "id": "Int64",
                "name": "string",
                "price": "float64",
                "active": "boolean"
            }
    return MockSchema


@pytest.fixture
def concrete_extractor(mock_client):
    """Create a concrete implementation of BaseExtractor for testing."""
    class ConcreteExtractor(BaseExtractor):
        def _check_inputs(self):
            if not hasattr(self, 'test_input') or self.test_input is None:
                raise ValueError("test_input is required")
        
        def _build_request(self):
            self.url = "https://api.example.com/data"
            self.params = {"param1": "value1"}
            self.requests = [
                (self.url, self.params, {"metadata": "test"})
            ]
        
        def _process_response(self, response: Any) -> pd.DataFrame:
            if isinstance(response, dict):
                return pd.DataFrame([response])
            elif isinstance(response, list):
                return pd.DataFrame(response)
            return pd.DataFrame()
    
    return ConcreteExtractor(mock_client)


# ============================================================================
# Test BaseExtractor Initialization
# ============================================================================

class TestBaseExtractorInit:
    """Test initialization and basic configuration."""
    
    def test_init_sets_client(self, mock_client):
        """Test that client is properly set during initialization."""
        class TestExtractor(BaseExtractor):
            def _check_inputs(self): pass
            def _build_request(self): pass
            def _process_response(self, response): return pd.DataFrame()
        
        extractor = TestExtractor(mock_client)
        assert extractor.client is mock_client
    
    def test_init_sets_default_attributes(self, mock_client):
        """Test that default attributes are initialized correctly."""
        class TestExtractor(BaseExtractor):
            def _check_inputs(self): pass
            def _build_request(self): pass
            def _process_response(self, response): return pd.DataFrame()
        
        extractor = TestExtractor(mock_client)
        assert extractor.url == ""
        assert extractor.params is None
        assert extractor.max_requests == CoreConfig.MAX_REQUESTS
    
    def test_schema_is_optional(self, mock_client):
        """Test that schema attribute is None by default."""
        class TestExtractor(BaseExtractor):
            def _check_inputs(self): pass
            def _build_request(self): pass
            def _process_response(self, response): return pd.DataFrame()
        
        extractor = TestExtractor(mock_client)
        assert extractor.schema is None


# ============================================================================
# Test Abstract Methods
# ============================================================================

class TestAbstractMethods:
    """Test that abstract methods must be implemented."""
    
    def test_cannot_instantiate_base_extractor(self, mock_client):
        """Test that BaseExtractor cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseExtractor(mock_client)
    
    def test_missing_check_inputs_raises_error(self, mock_client):
        """Test that missing _check_inputs implementation raises error."""
        with pytest.raises(TypeError):
            class IncompleteExtractor(BaseExtractor):
                def _build_request(self): pass
                def _process_response(self, response): return pd.DataFrame()
            
            IncompleteExtractor(mock_client)
    
    def test_missing_build_request_raises_error(self, mock_client):
        """Test that missing _build_request implementation raises error."""
        with pytest.raises(TypeError):
            class IncompleteExtractor(BaseExtractor):
                def _check_inputs(self): pass
                def _process_response(self, response): return pd.DataFrame()
            
            IncompleteExtractor(mock_client)
    
    def test_missing_process_response_raises_error(self, mock_client):
        """Test that missing _process_response implementation raises error."""
        with pytest.raises(TypeError):
            class IncompleteExtractor(BaseExtractor):
                def _check_inputs(self): pass
                def _build_request(self): pass
            
            IncompleteExtractor(mock_client)


# ============================================================================
# Test Input Validation
# ============================================================================

class TestCheckInputs:
    """Test input validation functionality."""
    
    def test_check_inputs_called_in_run(self, concrete_extractor):
        """Test that _check_inputs is called during run execution."""
        with patch.object(concrete_extractor, '_check_inputs') as mock_check:
            with patch.object(concrete_extractor, '_build_request'):
                with patch.object(concrete_extractor, '_call_api', 
                                return_value=AsyncMock(return_value=pd.DataFrame())):
                    try:
                        import asyncio
                        asyncio.run(concrete_extractor.run())
                    except:
                        pass
                    
                    mock_check.assert_called_once()
    
    def test_check_inputs_validation_error(self, concrete_extractor):
        """Test that validation errors are properly raised."""
        with pytest.raises(ValueError, match="test_input is required"):
            import asyncio
            asyncio.run(concrete_extractor.run())


# ============================================================================
# Test Request Building
# ============================================================================

class TestBuildRequest:
    """Test request building functionality."""
    
    def test_build_request_sets_url(self, concrete_extractor):
        """Test that _build_request sets the URL."""
        concrete_extractor.test_input = "valid"
        concrete_extractor._build_request()
        assert concrete_extractor.url == "https://api.example.com/data"
    
    def test_build_request_sets_params(self, concrete_extractor):
        """Test that _build_request sets parameters."""
        concrete_extractor.test_input = "valid"
        concrete_extractor._build_request()
        assert concrete_extractor.params == {"param1": "value1"}
    
    def test_build_request_creates_request_list(self, concrete_extractor):
        """Test that _build_request creates requests list."""
        concrete_extractor.test_input = "valid"
        concrete_extractor._build_request()
        assert len(concrete_extractor.requests) == 1
        assert concrete_extractor.requests[0][0] == "https://api.example.com/data"


# ============================================================================
# Test Request Validation
# ============================================================================

class TestCheckRequests:
    """Test request count validation."""
    
    def test_check_requests_passes_within_limit(self, concrete_extractor):
        """Test that check passes when request count is within limit."""
        concrete_extractor.requests = [(f"url_{i}", {}, {}) for i in range(5)]
        concrete_extractor.max_requests = 10
        # Should not raise
        concrete_extractor._check_requests()
    
    def test_check_requests_warns_exceeds_limit(self, concrete_extractor):
        """Test that warning is raised when request count exceeds limit."""
        concrete_extractor.requests = [(f"url_{i}", {}, {}) for i in range(15)]
        concrete_extractor.max_requests = 10
        
        with pytest.raises(Warning, match="Request count .* exceeds maximum"):
            concrete_extractor._check_requests()
    
    def test_check_requests_exact_limit(self, concrete_extractor):
        """Test that check passes when request count equals limit."""
        concrete_extractor.requests = [(f"url_{i}", {}, {}) for i in range(10)]
        concrete_extractor.max_requests = 10
        # Should not raise
        concrete_extractor._check_requests()


# ============================================================================
# Test API Calls
# ============================================================================

class TestCallApi:
    """Test asynchronous API call execution."""
    
    @pytest.mark.asyncio
    async def test_call_api_successful_request(self, concrete_extractor, mock_client):
        """Test successful API call returns DataFrame."""
        concrete_extractor.requests = [("https://api.example.com", {}, {})]
        mock_client.fetch_all.return_value = [{"id": 1, "name": "test"}]
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["name"].iloc[0] == "test"
    
    @pytest.mark.asyncio
    async def test_call_api_multiple_requests(self, concrete_extractor, mock_client):
        """Test multiple API calls are concatenated correctly."""
        concrete_extractor.requests = [
            ("https://api.example.com/1", {}, {}),
            ("https://api.example.com/2", {}, {})
        ]
        mock_client.fetch_all.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_call_api_handles_exceptions(self, concrete_extractor, mock_client):
        """Test that API exceptions are logged and handled."""
        concrete_extractor.requests = [("https://api.example.com", {}, {})]
        mock_client.fetch_all.return_value = [Exception("API Error")]
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        mock_client.logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_call_api_mixed_success_failure(self, concrete_extractor, mock_client):
        """Test that successful requests are processed despite failures."""
        concrete_extractor.requests = [
            ("https://api.example.com/1", {}, {}),
            ("https://api.example.com/2", {}, {})
        ]
        mock_client.fetch_all.return_value = [
            {"id": 1, "name": "success"},
            Exception("API Error")
        ]
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["name"].iloc[0] == "success"
    
    @pytest.mark.asyncio
    async def test_call_api_invalid_process_response_return(self, concrete_extractor, mock_client):
        """Test that non-DataFrame returns from _process_response are logged."""
        concrete_extractor.requests = [("https://api.example.com", {}, {})]
        mock_client.fetch_all.return_value = [{"id": 1}]
        
        # Override _process_response to return invalid type
        original_process = concrete_extractor._process_response
        concrete_extractor._process_response = lambda x: "not a dataframe"
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        mock_client.logger.error.assert_called()
        
        # Restore original
        concrete_extractor._process_response = original_process
    
    @pytest.mark.asyncio
    async def test_call_api_empty_response_list(self, concrete_extractor, mock_client):
        """Test that empty response list returns empty DataFrame."""
        concrete_extractor.requests = [("https://api.example.com", {}, {})]
        mock_client.fetch_all.return_value = []
        
        result = await concrete_extractor._call_api()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ============================================================================
# Test Response Processing
# ============================================================================

class TestProcessResponse:
    """Test response processing functionality."""
    
    def test_process_response_dict_input(self, concrete_extractor):
        """Test processing a dictionary response."""
        response = {"id": 1, "name": "test", "value": 100.5}
        result = concrete_extractor._process_response(response)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["name"].iloc[0] == "test"
    
    def test_process_response_list_input(self, concrete_extractor):
        """Test processing a list response."""
        response = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        result = concrete_extractor._process_response(response)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    def test_process_response_empty_input(self, concrete_extractor):
        """Test processing empty input returns empty DataFrame."""
        result = concrete_extractor._process_response(None)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ============================================================================
# Test Type Validation and Conversion
# ============================================================================

class TestValidateAndConvertTypes:
    """Test schema-based type validation and conversion."""
    
    def test_no_schema_returns_unchanged(self, concrete_extractor):
        """Test that DataFrame is returned unchanged when no schema is set."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        result = concrete_extractor._validate_and_convert_types(df)
        
        pd.testing.assert_frame_equal(result, df)
    
    def test_converts_to_string_type(self, concrete_extractor, mock_schema):
        """Test conversion to string type."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"name": [1, 2, 3]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert result["name"].dtype == "string"
    
    def test_converts_to_int64_type(self, concrete_extractor, mock_schema):
        """Test conversion to Int64 type."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"id": ["1", "2", "3"]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert result["id"].dtype == "Int64"
    
    def test_converts_to_float_type(self, concrete_extractor, mock_schema):
        """Test conversion to float type."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"price": ["10.5", "20.3", "30.1"]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert result["price"].dtype in ["float64", "float32"]
    
    def test_converts_to_boolean_type(self, concrete_extractor, mock_schema):
        """Test conversion to boolean type."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"active": [1, 0, 1]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert result["active"].dtype == "boolean"
    
    def test_handles_missing_columns(self, concrete_extractor, mock_schema, mock_client):
        """Test that missing schema columns are logged but don't cause errors."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"name": ["test"]})  # Missing id, price, active
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert "name" in result.columns
        mock_client.logger.debug.assert_called()
    
    def test_handles_extra_columns(self, concrete_extractor, mock_schema, mock_client):
        """Test that extra columns are preserved and logged."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({
            "id": [1],
            "name": ["test"],
            "extra_col": ["value"]
        })
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert "extra_col" in result.columns
        mock_client.logger.debug.assert_called()
    
    def test_handles_conversion_errors(self, concrete_extractor, mock_schema, mock_client):
        """Test that conversion errors are logged as warnings."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"id": ["not", "a", "number"]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        # Should still return DataFrame even with conversion errors
        assert isinstance(result, pd.DataFrame)
    
    def test_coerces_invalid_numeric_values(self, concrete_extractor, mock_schema):
        """Test that invalid numeric values are coerced to NaN."""
        concrete_extractor.schema = mock_schema
        df = pd.DataFrame({"id": ["1", "invalid", "3"]})
        
        result = concrete_extractor._validate_and_convert_types(df)
        
        assert pd.isna(result["id"].iloc[1])


# ============================================================================
# Test Complete Pipeline
# ============================================================================

class TestRunPipeline:
    """Test the complete extraction pipeline."""
    
    
    @pytest.mark.asyncio
    async def test_run_pipeline_order(self, concrete_extractor, mock_client):
        """Test that pipeline methods are called in correct order."""
        concrete_extractor.test_input = "valid"
        mock_client.fetch_all.return_value = [{"id": 1}]
        
        call_order = []
        
        original_check = concrete_extractor._check_inputs
        original_build = concrete_extractor._build_request
        original_call = concrete_extractor._call_api
        original_validate = concrete_extractor._validate_and_convert_types
        
        def track_check():
            call_order.append("check")
            return original_check()
        
        def track_build():
            call_order.append("build")
            return original_build()
        
        async def track_call():
            call_order.append("call")
            return await original_call()
        
        def track_validate(df):
            call_order.append("validate")
            return original_validate(df)
        
        concrete_extractor._check_inputs = track_check
        concrete_extractor._build_request = track_build
        concrete_extractor._call_api = track_call
        concrete_extractor._validate_and_convert_types = track_validate
        
        await concrete_extractor.run()
        
        assert call_order == ["check", "build", "call", "validate"]
    
    @pytest.mark.asyncio
    async def test_run_fails_on_invalid_input(self, concrete_extractor):
        """Test that run fails when input validation fails."""
        # Don't set test_input, should fail validation
        with pytest.raises(ValueError, match="test_input is required"):
            await concrete_extractor.run()


# ============================================================================
# Test Fetch Responses
# ============================================================================

class TestFetchResponses:
    """Test the response fetching mechanism."""
    
    @pytest.mark.asyncio
    async def test_fetch_responses_calls_client_fetch_all(self, concrete_extractor, mock_client):
        """Test that _fetch_responses delegates to client.fetch_all."""
        mock_session = Mock()
        requests = [("url1", {}, {}), ("url2", {}, {})]
        expected_response = [{"data": "test"}]
        mock_client.fetch_all.return_value = expected_response
        
        result = await concrete_extractor._fetch_responses(mock_session, requests)
        
        mock_client.fetch_all.assert_called_once_with(mock_session, requests)
        assert result == expected_response
