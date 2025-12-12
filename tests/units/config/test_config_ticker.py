"""Tests for ticker configuration classes."""
import pytest
from typing import get_args
from morningpy.config.ticker import TickerConfig


@pytest.mark.config
class TestTickerConfig:
    """Test suite for TickerConfig."""
    
    def test_id_literal_exists(self):
        """Test that IdLiteral type exists."""
        assert hasattr(TickerConfig, 'IdLiteral')
        
        # Get valid values from Literal
        valid_ids = get_args(TickerConfig.IdLiteral)
        assert "ticker" in valid_ids
        assert "isin" in valid_ids
        assert "performance_id" in valid_ids
        assert "security_id" in valid_ids
    
    def test_security_type_literal_exists(self):
        """Test that SecurityTypeLiteral type exists."""
        assert hasattr(TickerConfig, 'SecurityTypeLiteral')
        
        valid_types = get_args(TickerConfig.SecurityTypeLiteral)
        assert "fund" in valid_types
        assert "index" in valid_types
        assert "etf" in valid_types
        assert "stock" in valid_types
    
    def test_boolean_literal_exists(self):
        """Test that BooleanLiteral type exists."""
        assert hasattr(TickerConfig, 'BooleanLiteral')
        
        valid_bools = get_args(TickerConfig.BooleanLiteral)
        assert True in valid_bools
        assert False in valid_bools
    
    def test_country_literal_exists(self):
        """Test that CountryLiteral type exists and has countries."""
        assert hasattr(TickerConfig, 'CountryLiteral')
        
        countries = get_args(TickerConfig.CountryLiteral)
        assert len(countries) > 50, "Should have many countries"
        
        # Check for major countries
        assert "United States" in countries
        assert "United Kingdom" in countries
        assert "Canada" in countries
        assert "Germany" in countries
        assert "France" in countries
    
    def test_country_id_literal_exists(self):
        """Test that CountryIdLiteral type exists and has country codes."""
        assert hasattr(TickerConfig, 'CountryIdLiteral')
        
        country_ids = get_args(TickerConfig.CountryIdLiteral)
        assert len(country_ids) > 50, "Should have many country codes"
        
        # Check for major country codes
        assert "USA" in country_ids
        assert "GBR" in country_ids
        assert "CAN" in country_ids
        assert "DEU" in country_ids
        assert "FRA" in country_ids
    
    def test_country_and_id_count_match(self):
        """Test that CountryLiteral and CountryIdLiteral have same count."""
        countries = get_args(TickerConfig.CountryLiteral)
        country_ids = get_args(TickerConfig.CountryIdLiteral)
        
        # Should have roughly the same number (accounting for potential differences)
        assert abs(len(countries) - len(country_ids)) <= 5, \
            "Country names and IDs should have similar counts"
    
    def test_exchange_id_literal_exists(self):
        """Test that ExchangeIdLiteral type exists."""
        assert hasattr(TickerConfig, 'ExchangeIdLiteral')
        
        exchanges = get_args(TickerConfig.ExchangeIdLiteral)
        assert len(exchanges) > 50, "Should have many exchange IDs"
        
        # Check for major exchanges
        assert "XNYS" in exchanges  # NYSE
        assert "XNAS" in exchanges  # NASDAQ
        assert "XLON" in exchanges  # London
        assert "XTKS" in exchanges  # Tokyo
    
    def test_exchange_name_literal_exists(self):
        """Test that ExchangeNameLiteral type exists."""
        assert hasattr(TickerConfig, 'ExchangeNameLiteral')
        
        exchange_names = get_args(TickerConfig.ExchangeNameLiteral)
        assert len(exchange_names) > 50, "Should have many exchange names"
        
        # Check for major exchange names
        major_exchanges = [
            "New York Stock Exchange",
            "NASDAQ",
            "London Stock Exchange",
            "Tokyo Stock Exchange"
        ]
        
        # At least some major exchanges should be present
        found = sum(1 for ex in major_exchanges if ex in exchange_names)
        assert found >= 2, "Should contain several major exchange names"
    
    def test_sector_literal_exists(self):
        """Test that SectorLiteral type exists."""
        assert hasattr(TickerConfig, 'SectorLiteral')
        
        sectors = get_args(TickerConfig.SectorLiteral)
        expected_sectors = {
            "Consumer Cyclical",
            "Consumer Defensive",
            "Real Estate",
            "Basic Materials",
            "Communication Services",
            "Financial Services",
            "Utilities",
            "Healthcare",
            "Technology",
            "Industrials",
            "Energy"
        }
        
        assert set(sectors) == expected_sectors
    
    def test_all_literals_are_type_hints(self):
        """Test that all Literal attributes are proper type hints."""
        literal_attrs = [
            'IdLiteral',
            'SecurityTypeLiteral',
            'BooleanLiteral',
            'CountryLiteral',
            'CountryIdLiteral',
            'ExchangeIdLiteral',
            'ExchangeNameLiteral',
            'SectorLiteral'
        ]
        
        for attr in literal_attrs:
            assert hasattr(TickerConfig, attr), f"Missing {attr}"
            # Can get args means it's a proper Literal type
            args = get_args(getattr(TickerConfig, attr))
            assert len(args) > 0, f"{attr} should have values"
    
    def test_no_duplicate_values_in_literals(self):
        """Test that Literal types have no duplicate values."""
        literal_attrs = [
            'IdLiteral',
            'SecurityTypeLiteral',
            'CountryLiteral',
            'CountryIdLiteral',
            'ExchangeIdLiteral',
            'ExchangeNameLiteral',
            'SectorLiteral'
        ]
        
        for attr in literal_attrs:
            values = get_args(getattr(TickerConfig, attr))
            assert len(values) == len(set(values)), \
                f"{attr} should not have duplicate values"