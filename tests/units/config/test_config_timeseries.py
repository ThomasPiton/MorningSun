"""Tests for timeseries configuration classes."""
import pytest
import re
from morningpy.core.auth import AuthType
from morningpy.config.timeseries import (
    IntradayTimeseriesConfig,
    HistoricalTimeseriesConfig
)


@pytest.mark.config
class TestIntradayTimeseriesConfig:
    """Test suite for IntradayTimeseriesConfig."""
    
    def test_required_auth_is_bearer_token(self):
        """Test that REQUIRED_AUTH is BEARER_TOKEN."""
        assert hasattr(IntradayTimeseriesConfig, 'REQUIRED_AUTH')
        assert IntradayTimeseriesConfig.REQUIRED_AUTH == AuthType.BEARER_TOKEN
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, IntradayTimeseriesConfig.API_URL)
        assert "morningstar.com" in IntradayTimeseriesConfig.API_URL
    
    def test_params_structure(self):
        """Test PARAMS structure."""
        assert isinstance(IntradayTimeseriesConfig.PARAMS, dict)
        required_keys = ['query', 'frequency', 'preAfter', 'trackMarketData', 'instid']
        for key in required_keys:
            assert key in IntradayTimeseriesConfig.PARAMS
    
    def test_valid_frequency_structure(self):
        """Test VALID_FREQUENCY structure."""
        assert isinstance(IntradayTimeseriesConfig.VALID_FREQUENCY, set)
        expected = {"1min", "5min", "10min", "15min", "30min", "60min"}
        assert IntradayTimeseriesConfig.VALID_FREQUENCY == expected
    
    def test_mapping_frequency_consistency(self):
        """Test MAPPING_FREQUENCY keys match VALID_FREQUENCY."""
        assert set(IntradayTimeseriesConfig.MAPPING_FREQUENCY.keys()) == \
               IntradayTimeseriesConfig.VALID_FREQUENCY
    
    def test_mapping_frequency_values_are_integers(self):
        """Test that MAPPING_FREQUENCY values are integers."""
        for value in IntradayTimeseriesConfig.MAPPING_FREQUENCY.values():
            assert isinstance(value, int)
            assert value > 0
    
    def test_field_mapping_structure(self):
        """Test FIELD_MAPPING structure."""
        assert isinstance(IntradayTimeseriesConfig.FIELD_MAPPING, dict)
        
        # Should have OHLCV fields
        expected_fields = {"date", "open", "high", "low", "close", "volume"}
        assert set(IntradayTimeseriesConfig.FIELD_MAPPING.keys()) == expected_fields
    
    def test_string_columns_structure(self):
        """Test STRING_COLUMNS structure."""
        assert isinstance(IntradayTimeseriesConfig.STRING_COLUMNS, list)
        assert "security_id" in IntradayTimeseriesConfig.STRING_COLUMNS
        assert "date" in IntradayTimeseriesConfig.STRING_COLUMNS
    
    def test_numeric_columns_structure(self):
        """Test NUMERIC_COLUMNS structure."""
        assert isinstance(IntradayTimeseriesConfig.NUMERIC_COLUMNS, list)
        
        # Should have OHLCV fields
        required_numeric = ["open", "high", "low", "close", "volume"]
        for field in required_numeric:
            assert field in IntradayTimeseriesConfig.NUMERIC_COLUMNS
    
    def test_final_columns_composition(self):
        """Test FINAL_COLUMNS equals STRING + NUMERIC."""
        expected = (IntradayTimeseriesConfig.STRING_COLUMNS +
                   IntradayTimeseriesConfig.NUMERIC_COLUMNS)
        assert IntradayTimeseriesConfig.FINAL_COLUMNS == expected
    
    def test_all_columns_snake_case(self):
        """Test that all column names are snake_case."""
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for col in IntradayTimeseriesConfig.FINAL_COLUMNS:
            assert re.match(snake_case_pattern, col), \
                f"Column '{col}' should be snake_case"


@pytest.mark.config
class TestHistoricalTimeseriesConfig:
    """Test suite for HistoricalTimeseriesConfig."""
    
    def test_required_auth_is_bearer_token(self):
        """Test that REQUIRED_AUTH is BEARER_TOKEN."""
        assert hasattr(HistoricalTimeseriesConfig, 'REQUIRED_AUTH')
        assert HistoricalTimeseriesConfig.REQUIRED_AUTH == AuthType.BEARER_TOKEN
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, HistoricalTimeseriesConfig.API_URL)
    
    def test_params_structure(self):
        """Test PARAMS structure."""
        assert isinstance(HistoricalTimeseriesConfig.PARAMS, dict)
        required_keys = ['query', 'frequency', 'preAfter', 'trackMarketData', 'instid']
        for key in required_keys:
            assert key in HistoricalTimeseriesConfig.PARAMS
    
    def test_valid_frequency_structure(self):
        """Test VALID_FREQUENCY structure."""
        assert isinstance(HistoricalTimeseriesConfig.VALID_FREQUENCY, set)
        expected = {"daily", "weekly", "monthly"}
        assert HistoricalTimeseriesConfig.VALID_FREQUENCY == expected
    
    def test_mapping_frequency_consistency(self):
        """Test MAPPING_FREQUENCY keys match VALID_FREQUENCY."""
        assert set(HistoricalTimeseriesConfig.MAPPING_FREQUENCY.keys()) == \
               HistoricalTimeseriesConfig.VALID_FREQUENCY
    
    def test_mapping_frequency_values_are_strings(self):
        """Test that MAPPING_FREQUENCY values are single characters."""
        for value in HistoricalTimeseriesConfig.MAPPING_FREQUENCY.values():
            assert isinstance(value, str)
            assert len(value) == 1
            assert value in ['d', 'w', 'm']
    
    def test_field_mapping_has_previous_close(self):
        """Test that FIELD_MAPPING includes previous_close."""
        assert "previous_close" in HistoricalTimeseriesConfig.FIELD_MAPPING
    
    def test_final_columns_composition(self):
        """Test FINAL_COLUMNS equals STRING + NUMERIC."""
        expected = (HistoricalTimeseriesConfig.STRING_COLUMNS +
                   HistoricalTimeseriesConfig.NUMERIC_COLUMNS)
        assert HistoricalTimeseriesConfig.FINAL_COLUMNS == expected
    
    def test_previous_close_in_final_columns(self):
        """Test that previous_close is in FINAL_COLUMNS."""
        assert "previous_close" in HistoricalTimeseriesConfig.FINAL_COLUMNS


# Parametrized tests for both timeseries configs
@pytest.mark.config
@pytest.mark.parametrize("config_class", [
    IntradayTimeseriesConfig,
    HistoricalTimeseriesConfig,
])
class TestAllTimeseriesConfigs:
    """Parametrized tests for all timeseries configuration classes."""
    
    def test_has_required_auth(self, config_class):
        """Test that all configs have REQUIRED_AUTH."""
        assert hasattr(config_class, 'REQUIRED_AUTH')
        assert config_class.REQUIRED_AUTH == AuthType.BEARER_TOKEN
    
    def test_has_api_url(self, config_class):
        """Test that all configs have API_URL."""
        assert hasattr(config_class, 'API_URL')
        assert "morningstar.com" in config_class.API_URL
    
    def test_has_valid_frequency(self, config_class):
        """Test that all configs have VALID_FREQUENCY."""
        assert hasattr(config_class, 'VALID_FREQUENCY')
        assert isinstance(config_class.VALID_FREQUENCY, set)
        assert len(config_class.VALID_FREQUENCY) > 0
    
    def test_has_mapping_frequency(self, config_class):
        """Test that all configs have MAPPING_FREQUENCY."""
        assert hasattr(config_class, 'MAPPING_FREQUENCY')
        assert isinstance(config_class.MAPPING_FREQUENCY, dict)
    
    def test_has_ohlcv_columns(self, config_class):
        """Test that all configs have OHLCV columns."""
        required = ["open", "high", "low", "close", "volume"]
        for col in required:
            assert col in config_class.FINAL_COLUMNS