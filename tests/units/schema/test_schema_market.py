"""Tests for market schema classes."""
import pytest
from dataclasses import fields
from typing import get_type_hints, get_args
from morningpy.schema.market import (
    MarketCalendarUsInfoSchema,
    MarketFairValueSchema,
    MarketIndexesSchema,
    MarketMoversSchema,
    MarketCommoditiesSchema,
    MarketCurrenciesSchema
)
from tests.units.schema.test_base import SchemaTestBase


@pytest.mark.schema
class TestMarketCalendarUsInfoSchema(SchemaTestBase):
    """Test suite for MarketCalendarUsInfoSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketCalendarUsInfoSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketCalendarUsInfoSchema)
    
    def test_has_name_field(self):
        """Test that schema has name field."""
        assert hasattr(MarketCalendarUsInfoSchema, 'name')
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketCalendarUsInfoSchema)


@pytest.mark.schema
class TestMarketFairValueSchema(SchemaTestBase):
    """Test suite for MarketFairValueSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketFairValueSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketFairValueSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketFairValueSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(MarketFairValueSchema)
    
    def test_has_essential_fields(self):
        """Test that essential fields are present."""
        essential_fields = [
            'category', 'security_id', 'ticker', 'name',
            'fair_value', 'evaluated_price', 'price_to_fair_value'
        ]
        schema_fields = {f.name for f in fields(MarketFairValueSchema)}
        
        for field in essential_fields:
            assert field in schema_fields, f"Missing essential field: {field}"
    
    def test_string_fields(self):
        """Test that string fields have correct type."""
        string_fields = [
            'category', 'security_id', 'ticker', 'name',
            'performance_id', 'company_id', 'exchange', 'change',
            'is_quant', 'uncertainty'
        ]
        
        type_hints = get_type_hints(MarketFairValueSchema)
        for field in string_fields:
            assert field in type_hints
            args = get_args(type_hints[field])
            assert args[0] is str, f"Field '{field}' should be str"
    
    def test_numeric_fields(self):
        """Test that numeric fields have correct type."""
        numeric_fields = [
            'evaluated_price', 'fair_value', 'previous_fair_value',
            'one_star_price', 'two_star_price', 'four_star_price',
            'five_star_price', 'price_to_fair_value'
        ]
        
        type_hints = get_type_hints(MarketFairValueSchema)
        for field in numeric_fields:
            assert field in type_hints
            args = get_args(type_hints[field])
            assert args[0] is float, f"Field '{field}' should be float"


@pytest.mark.schema
class TestMarketIndexesSchema(SchemaTestBase):
    """Test suite for MarketIndexesSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketIndexesSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketIndexesSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketIndexesSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(MarketIndexesSchema)
    
    def test_has_price_fields(self):
        """Test that essential price fields are present."""
        price_fields = [
            'last_price', 'open_price', 'high_price', 'low_price',
            'year_high_price', 'year_low_price', 'previous_close_price'
        ]
        schema_fields = {f.name for f in fields(MarketIndexesSchema)}
        
        for field in price_fields:
            assert field in schema_fields, f"Missing price field: {field}"
    
    def test_has_identifier_fields(self):
        """Test that identifier fields are present."""
        id_fields = [
            'security_id', 'ticker', 'name', 'fund_id',
            'master_portfolio_id', 'performance_id'
        ]
        schema_fields = {f.name for f in fields(MarketIndexesSchema)}
        
        for field in id_fields:
            assert field in schema_fields, f"Missing identifier field: {field}"


@pytest.mark.schema
class TestMarketMoversSchema(SchemaTestBase):
    """Test suite for MarketMoversSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketMoversSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketMoversSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketMoversSchema)
    
    def test_has_market_mover_fields(self):
        """Test that market mover specific fields are present."""
        mover_fields = [
            'category', 'ticker', 'name', 'last_price',
            'percent_net_change', 'volume'
        ]
        schema_fields = {f.name for f in fields(MarketMoversSchema)}
        
        for field in mover_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_has_pre_post_market_fields(self):
        """Test that pre/post market fields are present."""
        assert hasattr(MarketMoversSchema, 'pre_market_price')
        assert hasattr(MarketMoversSchema, 'post_market_price')
        assert hasattr(MarketMoversSchema, 'pre_market_net_change')
        assert hasattr(MarketMoversSchema, 'post_market_net_change')
    
    def test_bid_ask_size_are_integers(self):
        """Test that bid/ask size fields are integers."""
        type_hints = get_type_hints(MarketMoversSchema)
        
        for field in ['bid_price_size', 'ask_price_size']:
            args = get_args(type_hints[field])
            assert args[0] is int, f"Field '{field}' should be int"


@pytest.mark.schema
class TestMarketCommoditiesSchema(SchemaTestBase):
    """Test suite for MarketCommoditiesSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketCommoditiesSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketCommoditiesSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketCommoditiesSchema)
    
    def test_has_essential_commodity_fields(self):
        """Test that essential commodity fields are present."""
        essential_fields = [
            'category', 'name', 'instrument_id', 'exchange',
            'last_price', 'net_change', 'percent_net_change'
        ]
        schema_fields = {f.name for f in fields(MarketCommoditiesSchema)}
        
        for field in essential_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_has_option_expiration_date(self):
        """Test that option_expiration_date field exists."""
        assert hasattr(MarketCommoditiesSchema, 'option_expiration_date')


@pytest.mark.schema
class TestMarketCurrenciesSchema(SchemaTestBase):
    """Test suite for MarketCurrenciesSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(MarketCurrenciesSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(MarketCurrenciesSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(MarketCurrenciesSchema)
    
    def test_has_essential_currency_fields(self):
        """Test that essential currency fields are present."""
        essential_fields = [
            'category', 'label', 'name', 'instrument_id',
            'exchange', 'bid_price', 'net_change', 'percent_net_change'
        ]
        schema_fields = {f.name for f in fields(MarketCurrenciesSchema)}
        
        for field in essential_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_bid_price_decimals_is_integer(self):
        """Test that bid_price_decimals is an integer."""
        type_hints = get_type_hints(MarketCurrenciesSchema)
        args = get_args(type_hints['bid_price_decimals'])
        assert args[0] is int, "bid_price_decimals should be int"


@pytest.mark.schema
@pytest.mark.parametrize("schema_class", [
    MarketCalendarUsInfoSchema,
    MarketFairValueSchema,
    MarketIndexesSchema,
    MarketMoversSchema,
    MarketCommoditiesSchema,
    MarketCurrenciesSchema,
])
class TestAllMarketSchemas(SchemaTestBase):
    """Parametrized tests for all market schema classes."""
    
    def test_is_dataclass_schema(self, schema_class):
        """Test that all schemas are proper dataclasses."""
        self.assert_is_dataclass_schema(schema_class)
    
    def test_can_instantiate(self, schema_class):
        """Test that all schemas can be instantiated."""
        self.assert_can_instantiate(schema_class)
    
    def test_all_fields_optional(self, schema_class):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(schema_class)
    
    def test_has_fields(self, schema_class):
        """Test that schemas have fields."""
        self.assert_has_fields(schema_class, min_fields=1)