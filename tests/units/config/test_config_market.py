"""Tests for market configuration classes."""
import pytest
import re
from morningpy.core.auth import AuthType
from morningpy.config.market import (
    MarketCalendarUsInfoConfig,
    MarketFairValueConfig,
    MarketIndexesConfig,
    MarketMoversConfig,
    MarketCommoditiesConfig,
    MarketCurrenciesConfig
)


class ConfigTestMixin:
    """Mixin class with common test methods for all configs."""
    
    @staticmethod
    def assert_valid_auth(config_class):
        """Test that REQUIRED_AUTH is a valid AuthType."""
        assert hasattr(config_class, 'REQUIRED_AUTH')
        assert isinstance(config_class.REQUIRED_AUTH, AuthType)
    
    @staticmethod
    def assert_valid_urls(config_class):
        """Test that URLs are valid HTTPS URLs."""
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        
        if hasattr(config_class, 'PAGE_URL'):
            assert re.match(url_pattern, config_class.PAGE_URL)
            assert "morningstar.com" in config_class.PAGE_URL
        
        if hasattr(config_class, 'API_URL'):
            assert re.match(url_pattern, config_class.API_URL)
            assert "morningstar.com" in config_class.API_URL
    
    @staticmethod
    def assert_snake_case(value, field_name):
        """Assert that a value follows snake_case pattern."""
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        assert re.match(snake_case_pattern, value), \
            f"{field_name} '{value}' should be snake_case"


@pytest.mark.config
class TestMarketCalendarUsInfoConfig(ConfigTestMixin):
    """Test suite for MarketCalendarUsInfoConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketCalendarUsInfoConfig)
        assert MarketCalendarUsInfoConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketCalendarUsInfoConfig)
    
    def test_params_structure(self):
        """Test that PARAMS is a dictionary with expected keys."""
        assert isinstance(MarketCalendarUsInfoConfig.PARAMS, dict)
        assert "date" in MarketCalendarUsInfoConfig.PARAMS
        assert "category" in MarketCalendarUsInfoConfig.PARAMS
    
    def test_valid_inputs_exists(self):
        """Test that VALID_INPUTS contains expected categories."""
        assert isinstance(MarketCalendarUsInfoConfig.VALID_INPUTS, set)
        expected_inputs = {"earnings", "economic-releases", "ipos", "splits"}
        assert MarketCalendarUsInfoConfig.VALID_INPUTS == expected_inputs
    
    def test_field_mapping_structure(self):
        """Test that FIELD_MAPPING has correct structure."""
        assert isinstance(MarketCalendarUsInfoConfig.FIELD_MAPPING, dict)
        assert "base" in MarketCalendarUsInfoConfig.FIELD_MAPPING
        assert "earnings" in MarketCalendarUsInfoConfig.FIELD_MAPPING
        assert "economic-releases" in MarketCalendarUsInfoConfig.FIELD_MAPPING
        assert "ipos" in MarketCalendarUsInfoConfig.FIELD_MAPPING
        assert "splits" in MarketCalendarUsInfoConfig.FIELD_MAPPING
    
    def test_field_mapping_keys_are_snake_case(self):
        """Test that all field mapping keys are snake_case."""
        for category, mappings in MarketCalendarUsInfoConfig.FIELD_MAPPING.items():
            for key in mappings.keys():
                self.assert_snake_case(key, f"FIELD_MAPPING[{category}] key")
    
    def test_field_mapping_values_are_strings(self):
        """Test that all field mapping values are strings."""
        for category, mappings in MarketCalendarUsInfoConfig.FIELD_MAPPING.items():
            for value in mappings.values():
                assert isinstance(value, str), \
                    f"FIELD_MAPPING[{category}] value should be string"
    
    @pytest.mark.parametrize("category", ["earnings", "economic-releases", "ipos", "splits"])
    def test_valid_input_categories_have_mappings(self, category):
        """Test that all valid input categories have field mappings."""
        assert category in MarketCalendarUsInfoConfig.FIELD_MAPPING


@pytest.mark.config
class TestMarketFairValueConfig(ConfigTestMixin):
    """Test suite for MarketFairValueConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketFairValueConfig)
        assert MarketFairValueConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketFairValueConfig)
    
    def test_valid_inputs_structure(self):
        """Test VALID_INPUTS structure."""
        assert isinstance(MarketFairValueConfig.VALID_INPUTS, set)
        assert MarketFairValueConfig.VALID_INPUTS == {"undervaluated", "overvaluated"}
    
    def test_mapping_inputs_consistency(self):
        """Test that MAPPING_INPUTS keys match VALID_INPUTS."""
        assert isinstance(MarketFairValueConfig.MAPPING_INPUTS, dict)
        assert set(MarketFairValueConfig.MAPPING_INPUTS.keys()) == \
               MarketFairValueConfig.VALID_INPUTS
    
    def test_rename_columns_snake_case(self):
        """Test that RENAME_COLUMNS values are snake_case."""
        for key, value in MarketFairValueConfig.RENAME_COLUMNS.items():
            self.assert_snake_case(value, "RENAME_COLUMNS value")
    
    def test_string_columns_are_snake_case(self):
        """Test that STRING_COLUMNS are snake_case."""
        for col in MarketFairValueConfig.STRING_COLUMNS:
            self.assert_snake_case(col, "STRING_COLUMNS")
    
    def test_numeric_columns_are_snake_case(self):
        """Test that NUMERIC_COLUMNS are snake_case."""
        for col in MarketFairValueConfig.NUMERIC_COLUMNS:
            self.assert_snake_case(col, "NUMERIC_COLUMNS")
    
    def test_final_columns_composition(self):
        """Test that FINAL_COLUMNS equals STRING_COLUMNS + NUMERIC_COLUMNS."""
        expected = MarketFairValueConfig.STRING_COLUMNS + MarketFairValueConfig.NUMERIC_COLUMNS
        assert MarketFairValueConfig.FINAL_COLUMNS == expected
    
    def test_no_duplicate_columns(self):
        """Test that FINAL_COLUMNS has no duplicates."""
        final_cols = MarketFairValueConfig.FINAL_COLUMNS
        assert len(final_cols) == len(set(final_cols))
    
    def test_renamed_columns_in_final_columns(self):
        """Test that renamed columns appear in final columns."""
        renamed_values = set(MarketFairValueConfig.RENAME_COLUMNS.values())
        final_columns = set(MarketFairValueConfig.FINAL_COLUMNS)
        
        # Most renamed values should be in final columns
        common = renamed_values & final_columns
        assert len(common) > 0, "Some renamed columns should appear in FINAL_COLUMNS"


@pytest.mark.config
class TestMarketIndexesConfig(ConfigTestMixin):
    """Test suite for MarketIndexesConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketIndexesConfig)
        assert MarketIndexesConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketIndexesConfig)
    
    def test_valid_inputs_structure(self):
        """Test VALID_INPUTS contains expected regions."""
        assert isinstance(MarketIndexesConfig.VALID_INPUTS, set)
        expected = {"americas", "asia", "europe", "private", "sector", "us"}
        assert MarketIndexesConfig.VALID_INPUTS == expected
    
    def test_mapping_inputs_consistency(self):
        """Test that MAPPING_INPUTS keys match VALID_INPUTS."""
        assert set(MarketIndexesConfig.MAPPING_INPUTS.keys()) == \
               MarketIndexesConfig.VALID_INPUTS
    
    def test_final_columns_composition(self):
        """Test FINAL_COLUMNS structure."""
        expected = MarketIndexesConfig.STRING_COLUMNS + MarketIndexesConfig.NUMERIC_COLUMNS
        assert MarketIndexesConfig.FINAL_COLUMNS == expected
    
    def test_all_columns_snake_case(self):
        """Test that all column names are snake_case."""
        for col in MarketIndexesConfig.FINAL_COLUMNS:
            self.assert_snake_case(col, "FINAL_COLUMNS")


@pytest.mark.config
class TestMarketMoversConfig(ConfigTestMixin):
    """Test suite for MarketMoversConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketMoversConfig)
        assert MarketMoversConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketMoversConfig)
    
    def test_valid_inputs_structure(self):
        """Test VALID_INPUTS contains market movers categories."""
        assert isinstance(MarketMoversConfig.VALID_INPUTS, set)
        assert MarketMoversConfig.VALID_INPUTS == {"gainers", "losers", "actives"}
    
    def test_rename_columns_values_snake_case(self):
        """Test that RENAME_COLUMNS values are snake_case."""
        for value in MarketMoversConfig.RENAME_COLUMNS.values():
            self.assert_snake_case(value, "RENAME_COLUMNS value")
    
    def test_final_columns_composition(self):
        """Test FINAL_COLUMNS equals STRING + NUMERIC columns."""
        expected = MarketMoversConfig.STRING_COLUMNS + MarketMoversConfig.NUMERIC_COLUMNS
        assert MarketMoversConfig.FINAL_COLUMNS == expected


@pytest.mark.config
class TestMarketCommoditiesConfig(ConfigTestMixin):
    """Test suite for MarketCommoditiesConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketCommoditiesConfig)
        assert MarketCommoditiesConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketCommoditiesConfig)
    
    def test_rename_columns_structure(self):
        """Test RENAME_COLUMNS structure."""
        assert isinstance(MarketCommoditiesConfig.RENAME_COLUMNS, dict)
        assert len(MarketCommoditiesConfig.RENAME_COLUMNS) > 0
    
    def test_rename_columns_values_snake_case(self):
        """Test that RENAME_COLUMNS values are snake_case."""
        for value in MarketCommoditiesConfig.RENAME_COLUMNS.values():
            self.assert_snake_case(value, "RENAME_COLUMNS value")
    
    def test_final_columns_structure(self):
        """Test FINAL_COLUMNS structure."""
        assert isinstance(MarketCommoditiesConfig.FINAL_COLUMNS, list)
        assert len(MarketCommoditiesConfig.FINAL_COLUMNS) > 0
    
    def test_final_columns_snake_case(self):
        """Test that FINAL_COLUMNS are snake_case."""
        for col in MarketCommoditiesConfig.FINAL_COLUMNS:
            self.assert_snake_case(col, "FINAL_COLUMNS")
    
    def test_essential_columns_present(self):
        """Test that essential columns are present."""
        essential = ["category", "name", "instrument_id", "exchange"]
        for col in essential:
            assert col in MarketCommoditiesConfig.FINAL_COLUMNS


@pytest.mark.config
class TestMarketCurrenciesConfig(ConfigTestMixin):
    """Test suite for MarketCurrenciesConfig."""
    
    def test_required_auth_is_valid(self):
        """Test that REQUIRED_AUTH is WAF_TOKEN."""
        self.assert_valid_auth(MarketCurrenciesConfig)
        assert MarketCurrenciesConfig.REQUIRED_AUTH == AuthType.WAF_TOKEN
    
    def test_urls_are_valid(self):
        """Test that all URLs are valid."""
        self.assert_valid_urls(MarketCurrenciesConfig)
    
    def test_rename_columns_structure(self):
        """Test RENAME_COLUMNS structure."""
        assert isinstance(MarketCurrenciesConfig.RENAME_COLUMNS, dict)
        assert len(MarketCurrenciesConfig.RENAME_COLUMNS) > 0
    
    def test_rename_columns_values_snake_case(self):
        """Test that RENAME_COLUMNS values are snake_case."""
        for value in MarketCurrenciesConfig.RENAME_COLUMNS.values():
            self.assert_snake_case(value, "RENAME_COLUMNS value")
    
    def test_final_columns_structure(self):
        """Test FINAL_COLUMNS structure."""
        assert isinstance(MarketCurrenciesConfig.FINAL_COLUMNS, list)
        assert len(MarketCurrenciesConfig.FINAL_COLUMNS) == 9
    
    def test_final_columns_snake_case(self):
        """Test that FINAL_COLUMNS are snake_case."""
        for col in MarketCurrenciesConfig.FINAL_COLUMNS:
            self.assert_snake_case(col, "FINAL_COLUMNS")
    
    def test_renamed_values_in_final_columns(self):
        """Test that renamed column values appear in FINAL_COLUMNS."""
        renamed_values = set(MarketCurrenciesConfig.RENAME_COLUMNS.values())
        final_columns = set(MarketCurrenciesConfig.FINAL_COLUMNS)
        
        assert renamed_values.issubset(final_columns), \
            f"Missing columns: {renamed_values - final_columns}"


# Parametrized tests for all market configs
@pytest.mark.config
@pytest.mark.parametrize("config_class", [
    MarketCalendarUsInfoConfig,
    MarketFairValueConfig,
    MarketIndexesConfig,
    MarketMoversConfig,
    MarketCommoditiesConfig,
    MarketCurrenciesConfig,
])
class TestAllMarketConfigs:
    """Parametrized tests for all market configuration classes."""
    
    def test_has_required_auth(self, config_class):
        """Test that all configs have REQUIRED_AUTH."""
        assert hasattr(config_class, 'REQUIRED_AUTH')
        assert isinstance(config_class.REQUIRED_AUTH, AuthType)
    
    def test_has_api_url(self, config_class):
        """Test that all configs have API_URL."""
        assert hasattr(config_class, 'API_URL')
        assert isinstance(config_class.API_URL, str)
        assert config_class.API_URL.startswith("https://")
    
    def test_has_page_url(self, config_class):
        """Test that all configs have PAGE_URL."""
        assert hasattr(config_class, 'PAGE_URL')
        assert isinstance(config_class.PAGE_URL, str)
        assert config_class.PAGE_URL.startswith("https://")