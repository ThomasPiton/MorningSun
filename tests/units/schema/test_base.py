"""Base test utilities for schema testing."""
import pytest
import pandas as pd
from dataclasses import fields, is_dataclass
from typing import get_type_hints, get_args, get_origin
from morningpy.core.dataframe_schema import DataFrameSchema


class SchemaTestBase:
    """Base class with reusable test methods for schema validation."""
    
    @staticmethod
    def assert_is_dataclass_schema(schema_class):
        """Test that schema is a dataclass inheriting from DataFrameSchema."""
        assert is_dataclass(schema_class), \
            f"{schema_class.__name__} should be a dataclass"
        assert issubclass(schema_class, DataFrameSchema), \
            f"{schema_class.__name__} should inherit from DataFrameSchema"
    
    @staticmethod
    def assert_all_fields_optional(schema_class):
        """Test that all fields are Optional."""
        type_hints = get_type_hints(schema_class)
        
        for field_name, field_type in type_hints.items():
            if field_name == 'return':  # Skip return type hint
                continue
            
            origin = get_origin(field_type)
            
            # Check if it's Optional (Union with None)
            if origin is not None:
                args = get_args(field_type)
                assert type(None) in args, \
                    f"Field '{field_name}' should be Optional"
    
    @staticmethod
    def assert_field_types_valid(schema_class):
        """Test that field types are valid Python types."""
        type_hints = get_type_hints(schema_class)
        valid_types = (str, int, float, bool, pd.Timestamp, type(None))
        
        for field_name, field_type in type_hints.items():
            if field_name == 'return':
                continue
            
            # Get the actual type from Optional[type]
            args = get_args(field_type)
            if args:
                actual_type = args[0]  # First arg of Optional
                assert actual_type in valid_types or hasattr(actual_type, '__name__'), \
                    f"Field '{field_name}' has invalid type: {actual_type}"
    
    @staticmethod
    def assert_field_names_snake_case(schema_class):
        """Test that all field names use snake_case."""
        import re
        snake_case_pattern = r'^[a-z][a-z0-9_]*$'
        
        for field in fields(schema_class):
            assert re.match(snake_case_pattern, field.name), \
                f"Field '{field.name}' should be snake_case"
    
    @staticmethod
    def assert_has_fields(schema_class, min_fields=1):
        """Test that schema has at least minimum number of fields."""
        schema_fields = fields(schema_class)
        assert len(schema_fields) >= min_fields, \
            f"{schema_class.__name__} should have at least {min_fields} field(s)"
    
    @staticmethod
    def assert_can_instantiate(schema_class):
        """Test that schema can be instantiated."""
        try:
            instance = schema_class()
            assert isinstance(instance, schema_class)
            assert isinstance(instance, DataFrameSchema)
        except Exception as e:
            pytest.fail(f"Failed to instantiate {schema_class.__name__}: {e}")
    
    @staticmethod
    def assert_field_defaults_none(schema_class):
        """Test that all fields have None as default value."""
        for field in fields(schema_class):
            assert field.default is None or field.default_factory is None, \
                f"Field '{field.name}' should have None as default"
    
    @staticmethod
    def get_string_fields(schema_class):
        """Get list of fields with str type."""
        type_hints = get_type_hints(schema_class)
        string_fields = []
        
        for field_name, field_type in type_hints.items():
            args = get_args(field_type)
            if args and args[0] is str:
                string_fields.append(field_name)
        
        return string_fields
    
    @staticmethod
    def get_numeric_fields(schema_class):
        """Get list of fields with numeric types (int, float)."""
        type_hints = get_type_hints(schema_class)
        numeric_fields = []
        
        for field_name, field_type in type_hints.items():
            args = get_args(field_type)
            if args and args[0] in (int, float):
                numeric_fields.append(field_name)
        
        return numeric_fields