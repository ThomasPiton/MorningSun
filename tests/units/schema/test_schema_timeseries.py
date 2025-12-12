"""Tests for timeseries schema classes."""
import pytest
from dataclasses import fields
from typing import get_type_hints, get_args
from datetime import datetime
from morningpy.schema.timeseries import (
    IntradayTimeseriesSchema,
    HistoricalTimeseriesSchema
)
from tests.units.schema.test_base import SchemaTestBase


@pytest.mark.schema
class TestIntradayTimeseriesSchema(SchemaTestBase):
    """Test suite for IntradayTimeseriesSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(IntradayTimeseriesSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(IntradayTimeseriesSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(IntradayTimeseriesSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(IntradayTimeseriesSchema)
    
    def test_has_ohlcv_fields(self):
        """Test that OHLCV fields are present."""
        ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']
        schema_fields = {f.name for f in fields(IntradayTimeseriesSchema)}
        
        for field in ohlcv_fields:
            assert field in schema_fields, f"Missing OHLCV field: {field}"
    
    def test_has_identifier_fields(self):
        """Test that identifier fields are present."""
        assert hasattr(IntradayTimeseriesSchema, 'security_id')
        assert hasattr(IntradayTimeseriesSchema, 'date')
    
    def test_has_previous_close(self):
        """Test that previous_close field exists."""
        assert hasattr(IntradayTimeseriesSchema, 'previous_close')
    
    def test_price_fields_are_floats(self):
        """Test that price fields are float type."""
        price_fields = ['open', 'high', 'low', 'close', 'previous_close', 'volume']
        type_hints = get_type_hints(IntradayTimeseriesSchema)
        
        for field in price_fields:
            args = get_args(type_hints[field])
            assert args[0] is float, f"Field '{field}' should be float"
    
    def test_date_is_string(self):
        """Test that date field is string."""
        type_hints = get_type_hints(IntradayTimeseriesSchema)
        args = get_args(type_hints['date'])
        assert args[0] is str, "date field should be str for intraday"
    
    def test_field_count(self):
        """Test that schema has exactly 8 fields."""
        schema_fields = fields(IntradayTimeseriesSchema)
        assert len(schema_fields) == 8, \
            f"Should have 8 fields, got {len(schema_fields)}"


@pytest.mark.schema
class TestHistoricalTimeseriesSchema(SchemaTestBase):
    """Test suite for HistoricalTimeseriesSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(HistoricalTimeseriesSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(HistoricalTimeseriesSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(HistoricalTimeseriesSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(HistoricalTimeseriesSchema)
    
    def test_has_ohlcv_fields(self):
        """Test that OHLCV fields are present."""
        ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']
        schema_fields = {f.name for f in fields(HistoricalTimeseriesSchema)}
        
        for field in ohlcv_fields:
            assert field in schema_fields, f"Missing OHLCV field: {field}"
    
    def test_has_previous_close(self):
        """Test that previous_close field exists."""
        assert hasattr(HistoricalTimeseriesSchema, 'previous_close')
    
    def test_price_fields_are_floats(self):
        """Test that price fields are float type."""
        price_fields = ['open', 'high', 'low', 'close', 'previous_close', 'volume']
        type_hints = get_type_hints(HistoricalTimeseriesSchema)
        
        for field in price_fields:
            args = get_args(type_hints[field])
            assert args[0] is float, f"Field '{field}' should be float"
    
    def test_date_is_datetime(self):
        """Test that date field is datetime."""
        type_hints = get_type_hints(HistoricalTimeseriesSchema)
        args = get_args(type_hints['date'])
        assert args[0] is datetime, "date field should be datetime for historical"
    
    def test_field_count(self):
        """Test that schema has exactly 8 fields."""
        schema_fields = fields(HistoricalTimeseriesSchema)
        assert len(schema_fields) == 8, \
            f"Should have 8 fields, got {len(schema_fields)}"
    
    def test_schemas_have_same_fields_except_date_type(self):
        """Test that intraday and historical schemas differ only in date type."""
        intraday_fields = {f.name for f in fields(IntradayTimeseriesSchema)}
        historical_fields = {f.name for f in fields(HistoricalTimeseriesSchema)}
        
        # Should have same field names
        assert intraday_fields == historical_fields, \
            "Schemas should have same field names"


# Parametrized tests for both timeseries schemas
@pytest.mark.schema
@pytest.mark.parametrize("schema_class", [
    IntradayTimeseriesSchema,
    HistoricalTimeseriesSchema,
])
class TestAllTimeseriesSchemas(SchemaTestBase):
    """Parametrized tests for all timeseries schema classes."""
    
    def test_is_dataclass_schema(self, schema_class):
        """Test that all schemas are proper dataclasses."""
        self.assert_is_dataclass_schema(schema_class)
    
    def test_can_instantiate(self, schema_class):
        """Test that all schemas can be instantiated."""
        self.assert_can_instantiate(schema_class)
    
    def test_all_fields_optional(self, schema_class):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(schema_class)
    
    def test_has_security_id(self, schema_class):
        """Test that all schemas have security_id."""
        assert hasattr(schema_class, 'security_id')
    
    def test_has_complete_ohlcv(self, schema_class):
        """Test that all schemas have complete OHLCV data."""
        required = ['open', 'high', 'low', 'close', 'volume']
        schema_fields = {f.name for f in fields(schema_class)}
        
        for field in required:
            assert field in schema_fields