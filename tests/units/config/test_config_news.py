"""Tests for news configuration classes."""
import pytest
import re
from morningpy.core.auth import AuthType
from morningpy.config.news import HeadlineNewsConfig


@pytest.mark.config
class TestHeadlineNewsConfig:
    """Test suite for HeadlineNewsConfig."""
    
    def test_required_auth_is_bearer_token(self):
        """Test that REQUIRED_AUTH is BEARER_TOKEN."""
        assert hasattr(HeadlineNewsConfig, 'REQUIRED_AUTH')
        assert isinstance(HeadlineNewsConfig.REQUIRED_AUTH, AuthType)
        assert HeadlineNewsConfig.REQUIRED_AUTH == AuthType.BEARER_TOKEN
    
    def test_api_url_is_valid(self):
        """Test that API_URL is valid."""
        assert hasattr(HeadlineNewsConfig, 'API_URL')
        url_pattern = r'^https://[^\s/$.?#].[^\s]*$'
        assert re.match(url_pattern, HeadlineNewsConfig.API_URL)
        assert "morningstar.com" in HeadlineNewsConfig.API_URL
    
    def test_params_structure(self):
        """Test that PARAMS has correct structure."""
        assert isinstance(HeadlineNewsConfig.PARAMS, dict)
        assert "marketID" in HeadlineNewsConfig.PARAMS
        assert "sectionFallBack" in HeadlineNewsConfig.PARAMS
    
    def test_columns_structure(self):
        """Test that COLUMNS is a list."""
        assert isinstance(HeadlineNewsConfig.COLUMNS, list)
        assert len(HeadlineNewsConfig.COLUMNS) == 8
    
    def test_columns_are_snake_case(self):
        """Test that column names are snake_case."""
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        for col in HeadlineNewsConfig.COLUMNS:
            assert re.match(snake_case_pattern, col), \
                f"Column '{col}' should be snake_case"
    
    def test_market_id_structure(self):
        """Test MARKET_ID dictionary structure."""
        assert isinstance(HeadlineNewsConfig.MARKET_ID, dict)
        assert len(HeadlineNewsConfig.MARKET_ID) > 0
        
        # All values should be 2-4 letter codes
        for key, value in HeadlineNewsConfig.MARKET_ID.items():
            assert isinstance(key, str)
            assert isinstance(value, str)
            assert 2 <= len(value) <= 4, \
                f"Market ID '{value}' should be 2-4 characters"
    
    def test_edition_id_structure(self):
        """Test EDITION_ID dictionary structure."""
        assert isinstance(HeadlineNewsConfig.EDITION_ID, dict)
        assert len(HeadlineNewsConfig.EDITION_ID) > 0
        
        # All values should be locale codes
        for key, value in HeadlineNewsConfig.EDITION_ID.items():
            assert isinstance(key, str)
            assert isinstance(value, str)
            assert "-" in value or len(value) == 2, \
                f"Edition ID '{value}' should be locale code"
    
    def test_english_endpoint_structure(self):
        """Test ENGLISH_ENDPOINT dictionary structure."""
        assert isinstance(HeadlineNewsConfig.ENGLISH_ENDPOINT, dict)
        
        expected_keys = {
            "economy", "personal-finance", "sustainable-investing",
            "bonds", "etfs", "funds", "stocks", "markets"
        }
        assert set(HeadlineNewsConfig.ENGLISH_ENDPOINT.keys()) == expected_keys
        
        # All values should start with "sections/"
        for value in HeadlineNewsConfig.ENGLISH_ENDPOINT.values():
            assert value.startswith("sections/"), \
                f"Endpoint '{value}' should start with 'sections/'"
    
    def test_french_endpoint_structure(self):
        """Test FRENCH_ENDPOINT dictionary structure."""
        assert isinstance(HeadlineNewsConfig.FRENCH_ENDPOINT, dict)
        
        # Should have same keys as ENGLISH_ENDPOINT
        assert set(HeadlineNewsConfig.FRENCH_ENDPOINT.keys()) == \
               set(HeadlineNewsConfig.ENGLISH_ENDPOINT.keys())
        
        # All values should start with "sections/"
        for value in HeadlineNewsConfig.FRENCH_ENDPOINT.values():
            assert value.startswith("sections/"), \
                f"Endpoint '{value}' should start with 'sections/'"
    
    def test_endpoint_structure(self):
        """Test ENDPOINT dictionary structure."""
        assert isinstance(HeadlineNewsConfig.ENDPOINT, dict)
        
        # All editions should have endpoint mappings
        for edition, endpoints in HeadlineNewsConfig.ENDPOINT.items():
            assert isinstance(endpoints, dict)
            assert len(endpoints) > 0
            
            # Should be either ENGLISH_ENDPOINT or FRENCH_ENDPOINT
            assert endpoints in [
                HeadlineNewsConfig.ENGLISH_ENDPOINT,
                HeadlineNewsConfig.FRENCH_ENDPOINT
            ]
    
    def test_endpoint_edition_consistency(self):
        """Test that ENDPOINT keys match EDITION_ID keys."""
        endpoint_editions = set(HeadlineNewsConfig.ENDPOINT.keys())
        edition_id_editions = set(HeadlineNewsConfig.EDITION_ID.keys())
        
        assert endpoint_editions == edition_id_editions, \
            f"Mismatch: {endpoint_editions ^ edition_id_editions}"
    
    @pytest.mark.parametrize("edition,expected_lang", [
        ("France", "FRENCH"),
        ("Canada French", "FRENCH"),
        ("Canada English", "ENGLISH"),
        ("United Kingdom", "ENGLISH"),
    ])
    def test_edition_language_mapping(self, edition, expected_lang):
        """Test that editions map to correct language endpoints."""
        endpoint = HeadlineNewsConfig.ENDPOINT[edition]
        if expected_lang == "FRENCH":
            assert endpoint == HeadlineNewsConfig.FRENCH_ENDPOINT
        else:
            assert endpoint == HeadlineNewsConfig.ENGLISH_ENDPOINT