"""Tests for security configuration classes."""
import pytest
import re
from morningpy.core.auth import AuthType
from morningpy.config.security import (
    FinancialStatementConfig,
    HoldingConfig,
    HoldingInfoConfig
)


@pytest.mark.config
class TestFinancialStatementConfig:
    """Test suite for FinancialStatementConfig."""
    
    def test_required_auth_is_api_key(self):
        """Test that REQUIRED_AUTH is API_KEY."""
        assert hasattr(FinancialStatementConfig, 'REQUIRED_AUTH')
        assert FinancialStatementConfig.REQUIRED_AUTH == AuthType.API_KEY
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, FinancialStatementConfig.API_URL)
        assert "api-global.morningstar.com" in FinancialStatementConfig.API_URL
    
    def test_params_structure(self):
        """Test PARAMS structure."""
        assert isinstance(FinancialStatementConfig.PARAMS, dict)
        required_keys = ["dataType", "reportType", "languageId", "locale", "clientId"]
        for key in required_keys:
            assert key in FinancialStatementConfig.PARAMS
    
    def test_valid_frequency_structure(self):
        """Test VALID_FREQUENCY structure."""
        assert isinstance(FinancialStatementConfig.VALID_FREQUENCY, set)
        assert FinancialStatementConfig.VALID_FREQUENCY == {"Annualy", "Quarterly"}
    
    def test_mapping_frequency_consistency(self):
        """Test MAPPING_FREQUENCY consistency."""
        assert set(FinancialStatementConfig.MAPPING_FREQUENCY.keys()) == \
               FinancialStatementConfig.VALID_FREQUENCY
    
    def test_endpoint_structure(self):
        """Test ENDPOINT structure."""
        assert isinstance(FinancialStatementConfig.ENDPOINT, dict)
        expected_statements = {
            "Balance Sheet", "Cash Flow Statement", "Income Statement"
        }
        assert set(FinancialStatementConfig.ENDPOINT.keys()) == expected_statements
    
    def test_filter_value_structure(self):
        """Test FILTER_VALUE structure."""
        assert isinstance(FinancialStatementConfig.FILTER_VALUE, dict)
        expected_filters = {"balance-sheet", "cash-flow", "income-statement"}
        assert set(FinancialStatementConfig.FILTER_VALUE.keys()) == expected_filters


@pytest.mark.config
class TestHoldingConfig:
    """Test suite for HoldingConfig."""
    
    def test_required_auth_is_api_key(self):
        """Test that REQUIRED_AUTH is API_KEY."""
        assert HoldingConfig.REQUIRED_AUTH == AuthType.API_KEY
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, HoldingConfig.API_URL)
    
    def test_params_structure(self):
        """Test PARAMS structure."""
        assert isinstance(HoldingConfig.PARAMS, dict)
        assert "premiumNum" in HoldingConfig.PARAMS
        assert "freeNum" in HoldingConfig.PARAMS
    
    def test_field_mapping_structure(self):
        """Test FIELD_MAPPING structure."""
        assert isinstance(HoldingConfig.FIELD_MAPPING, dict)
        assert len(HoldingConfig.FIELD_MAPPING) > 0
        
        # Keys should be snake_case
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for key in HoldingConfig.FIELD_MAPPING.keys():
            assert re.match(snake_case_pattern, key), \
                f"FIELD_MAPPING key '{key}' should be snake_case"
    
    def test_rename_columns_structure(self):
        """Test RENAME_COLUMNS structure."""
        assert isinstance(HoldingConfig.RENAME_COLUMNS, dict)
        
        # Values should be snake_case
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for value in HoldingConfig.RENAME_COLUMNS.values():
            assert re.match(snake_case_pattern, value), \
                f"RENAME_COLUMNS value '{value}' should be snake_case"
    
    def test_columns_structure(self):
        """Test COLUMNS structure."""
        assert isinstance(HoldingConfig.COLUMNS, list)
        assert len(HoldingConfig.COLUMNS) > 0
        
        # All should be snake_case
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for col in HoldingConfig.COLUMNS:
            assert re.match(snake_case_pattern, col), \
                f"COLUMNS '{col}' should be snake_case"
    
    def test_parent_security_id_in_columns(self):
        """Test that parent_security_id is in COLUMNS."""
        assert "parent_security_id" in HoldingConfig.COLUMNS
    
    def test_essential_holding_fields_present(self):
        """Test that essential holding fields are present."""
        essential = [
            "child_security_id", "security_name", "weighting",
            "ticker", "sector", "currency"
        ]
        for field in essential:
            assert field in HoldingConfig.COLUMNS


@pytest.mark.config
class TestHoldingInfoConfig:
    """Test suite for HoldingInfoConfig."""
    
    def test_required_auth_is_api_key(self):
        """Test that REQUIRED_AUTH is API_KEY."""
        assert HoldingInfoConfig.REQUIRED_AUTH == AuthType.API_KEY
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, HoldingInfoConfig.API_URL)
    
    def test_params_structure(self):
        """Test PARAMS structure."""
        assert isinstance(HoldingInfoConfig.PARAMS, dict)
        assert "premiumNum" in HoldingInfoConfig.PARAMS
        assert "freeNum" in HoldingInfoConfig.PARAMS
    
    def test_field_mapping_structure(self):
        """Test FIELD_MAPPING structure."""
        assert isinstance(HoldingInfoConfig.FIELD_MAPPING, dict)
        
        # Keys should be snake_case
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for key in HoldingInfoConfig.FIELD_MAPPING.keys():
            assert re.match(snake_case_pattern, key)
    
    def test_holding_summary_mapping_structure(self):
        """Test HOLDING_SUMMARY_MAPPING structure."""
        assert isinstance(HoldingInfoConfig.HOLDING_SUMMARY_MAPPING, dict)
        
        # Check special case: last_turnover_date with list of alternatives
        assert "last_turnover_date" in HoldingInfoConfig.HOLDING_SUMMARY_MAPPING
        turnover_mapping = HoldingInfoConfig.HOLDING_SUMMARY_MAPPING["last_turnover_date"]
        assert isinstance(turnover_mapping, list)
        assert len(turnover_mapping) == 2
    
    def test_columns_structure(self):
        """Test COLUMNS structure."""
        assert isinstance(HoldingInfoConfig.COLUMNS, list)
        assert len(HoldingInfoConfig.COLUMNS) > 0
        
        # All should be snake_case
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for col in HoldingInfoConfig.COLUMNS:
            assert re.match(snake_case_pattern, col)
    
    def test_essential_info_fields_present(self):
        """Test that essential info fields are present."""
        essential = [
            "security_id", "master_portfolio_id", "number_of_holding",
            "top_holding_weighting"
        ]
        for field in essential:
            assert field in HoldingInfoConfig.COLUMNS


# Parametrized tests for all security configs
@pytest.mark.config
@pytest.mark.parametrize("config_class", [
    FinancialStatementConfig,
    HoldingConfig,
    HoldingInfoConfig,
])
class TestAllSecurityConfigs:
    """Parametrized tests for all security configuration classes."""
    
    def test_has_required_auth(self, config_class):
        """Test that all configs have REQUIRED_AUTH."""
        assert hasattr(config_class, 'REQUIRED_AUTH')
        assert config_class.REQUIRED_AUTH == AuthType.API_KEY
    
    def test_has_api_url(self, config_class):
        """Test that all configs have API_URL."""
        assert hasattr(config_class, 'API_URL')
        assert "api-global.morningstar.com" in config_class.API_URL
    
    def test_has_params(self, config_class):
        """Test that all configs have PARAMS."""
        assert hasattr(config_class, 'PARAMS')
        assert isinstance(config_class.PARAMS, dict)