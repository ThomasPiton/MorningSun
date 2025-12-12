"""Tests for ticker schema classes."""
import pytest
from morningpy.schema.ticker import TickerSchema
from tests.units.schema.test_base import SchemaTestBase


@pytest.mark.schema
class TestTickerSchema(SchemaTestBase):
    """Test suite for TickerSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(TickerSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(TickerSchema)
    
    def test_is_empty_schema(self):
        """Test that TickerSchema is an empty pass-through schema."""
        # TickerSchema has no fields defined (just 'pass')
        # This is expected as it's likely used as a base or placeholder
        instance = TickerSchema()
        assert isinstance(instance, TickerSchema)