"""Tests for news schema classes."""
import pytest
from dataclasses import fields
from typing import get_type_hints, get_args
from morningpy.schema.news import HeadlineNewsSchema
from tests.units.schema.test_base import SchemaTestBase


@pytest.mark.schema
class TestHeadlineNewsSchema(SchemaTestBase):
    """Test suite for HeadlineNewsSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(HeadlineNewsSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(HeadlineNewsSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(HeadlineNewsSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(HeadlineNewsSchema)
    
    def test_has_all_required_fields(self):
        """Test that all required news fields are present."""
        required_fields = [
            'news', 'edition', 'market', 'display_date',
            'title', 'subtitle', 'tags', 'link', 'language'
        ]
        schema_fields = {f.name for f in fields(HeadlineNewsSchema)}
        
        for field in required_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_all_fields_are_strings(self):
        """Test that all fields are string type."""
        type_hints = get_type_hints(HeadlineNewsSchema)
        
        for field_name, field_type in type_hints.items():
            if field_name == 'return':
                continue
            args = get_args(field_type)
            assert args[0] is str, f"Field '{field_name}' should be str"
    
    def test_field_count(self):
        """Test that schema has exactly 9 fields."""
        schema_fields = fields(HeadlineNewsSchema)
        assert len(schema_fields) == 9, \
            f"Should have 9 fields, got {len(schema_fields)}"
    
    def test_has_metadata_fields(self):
        """Test that metadata fields are present."""
        metadata_fields = ['edition', 'market', 'language']
        schema_fields = {f.name for f in fields(HeadlineNewsSchema)}
        
        for field in metadata_fields:
            assert field in schema_fields, f"Missing metadata field: {field}"
    
    def test_has_content_fields(self):
        """Test that content fields are present."""
        content_fields = ['title', 'subtitle', 'tags', 'link']
        schema_fields = {f.name for f in fields(HeadlineNewsSchema)}
        
        for field in content_fields:
            assert field in schema_fields, f"Missing content field: {field}"