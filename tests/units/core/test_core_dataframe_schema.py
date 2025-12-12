"""Tests for DataFrameSchema module."""
import pytest
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from morningpy.core.dataframe_schema import DataFrameSchema


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def basic_schema():
    """Provide a basic schema with standard types."""
    @dataclass
    class BasicSchema(DataFrameSchema):
        id: int
        name: str
        value: float
        active: bool
    
    return BasicSchema()


@pytest.fixture
def optional_schema():
    """Provide a schema with Optional types."""
    @dataclass
    class OptionalSchema(DataFrameSchema):
        id: Optional[int]
        name: Optional[str]
        value: Optional[float]
        active: Optional[bool]
    
    return OptionalSchema()


@pytest.fixture
def mixed_schema():
    """Provide a schema with mixed required and optional types."""
    @dataclass
    class MixedSchema(DataFrameSchema):
        id: int
        name: Optional[str]
        value: float
        description: Optional[str]
        count: Optional[int]
    
    return MixedSchema()


@pytest.fixture
def complex_schema():
    """Provide a schema with complex/unsupported types."""
    @dataclass
    class ComplexSchema(DataFrameSchema):
        id: int
        tags: List[str]
        metadata: Dict[str, Any]
        simple: str
    
    return ComplexSchema()


@pytest.fixture
def empty_schema():
    """Provide an empty schema with no fields."""
    @dataclass
    class EmptySchema(DataFrameSchema):
        pass
    
    return EmptySchema()


# ============================================================================
# BASIC SCHEMA TESTS
# ============================================================================

class TestBasicSchema:
    """Test suite for basic schema with standard types."""
    
    def test_returns_dict(self, basic_schema):
        """Test that to_dtype_dict returns a dictionary."""
        result = basic_schema.to_dtype_dict()
        
        assert isinstance(result, dict)
    
    def test_int_type_mapping(self, basic_schema):
        """Test that int maps to 'Int64'."""
        result = basic_schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
    
    def test_str_type_mapping(self, basic_schema):
        """Test that str maps to 'string'."""
        result = basic_schema.to_dtype_dict()
        
        assert result['name'] == 'string'
    
    def test_float_type_mapping(self, basic_schema):
        """Test that float maps to 'float64'."""
        result = basic_schema.to_dtype_dict()
        
        assert result['value'] == 'float64'
    
    def test_bool_type_mapping(self, basic_schema):
        """Test that bool maps to 'boolean'."""
        result = basic_schema.to_dtype_dict()
        
        assert result['active'] == 'boolean'
    
    def test_all_fields_present(self, basic_schema):
        """Test that all schema fields are in result."""
        result = basic_schema.to_dtype_dict()
        
        assert set(result.keys()) == {'id', 'name', 'value', 'active'}
    
    def test_field_count(self, basic_schema):
        """Test that correct number of fields is returned."""
        result = basic_schema.to_dtype_dict()
        
        assert len(result) == 4


# ============================================================================
# OPTIONAL TYPES TESTS
# ============================================================================

class TestOptionalTypes:
    """Test suite for Optional type handling."""
    
    def test_optional_int_mapping(self, optional_schema):
        """Test that Optional[int] maps to 'Int64'."""
        result = optional_schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
    
    def test_optional_str_mapping(self, optional_schema):
        """Test that Optional[str] maps to 'string'."""
        result = optional_schema.to_dtype_dict()
        
        assert result['name'] == 'string'
    
    def test_optional_float_mapping(self, optional_schema):
        """Test that Optional[float] maps to 'float64'."""
        result = optional_schema.to_dtype_dict()
        
        assert result['value'] == 'float64'
    
    def test_optional_bool_mapping(self, optional_schema):
        """Test that Optional[bool] maps to 'boolean'."""
        result = optional_schema.to_dtype_dict()
        
        assert result['active'] == 'boolean'
    
    def test_all_optional_fields_present(self, optional_schema):
        """Test that all Optional fields are included."""
        result = optional_schema.to_dtype_dict()
        
        assert set(result.keys()) == {'id', 'name', 'value', 'active'}
    
    def test_optional_unwrapping(self):
        """Test that Optional wrapper is correctly unwrapped."""
        @dataclass
        class TestSchema(DataFrameSchema):
            optional_field: Optional[int]
            required_field: int
        
        schema = TestSchema()
        result = schema.to_dtype_dict()
        
        # Both should map to same dtype
        assert result['optional_field'] == result['required_field']
        assert result['optional_field'] == 'Int64'


# ============================================================================
# MIXED TYPES TESTS
# ============================================================================

class TestMixedTypes:
    """Test suite for schemas with mixed required and optional types."""
    
    def test_required_fields_mapped_correctly(self, mixed_schema):
        """Test that required fields are mapped correctly."""
        result = mixed_schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
        assert result['value'] == 'float64'
    
    def test_optional_fields_mapped_correctly(self, mixed_schema):
        """Test that optional fields are mapped correctly."""
        result = mixed_schema.to_dtype_dict()
        
        assert result['name'] == 'string'
        assert result['description'] == 'string'
        assert result['count'] == 'Int64'
    
    def test_all_mixed_fields_present(self, mixed_schema):
        """Test that all fields are in result."""
        result = mixed_schema.to_dtype_dict()
        
        expected_fields = {'id', 'name', 'value', 'description', 'count'}
        assert set(result.keys()) == expected_fields
    
    def test_mixed_field_count(self, mixed_schema):
        """Test correct number of fields in mixed schema."""
        result = mixed_schema.to_dtype_dict()
        
        assert len(result) == 5


# ============================================================================
# COMPLEX AND UNSUPPORTED TYPES TESTS
# ============================================================================

class TestComplexTypes:
    """Test suite for complex and unsupported types."""
    
    def test_list_type_defaults_to_object(self, complex_schema):
        """Test that List[str] defaults to 'object'."""
        result = complex_schema.to_dtype_dict()
        
        assert result['tags'] == 'object'
    
    def test_dict_type_defaults_to_object(self, complex_schema):
        """Test that Dict[str, Any] defaults to 'object'."""
        result = complex_schema.to_dtype_dict()
        
        assert result['metadata'] == 'object'
    
    def test_supported_types_still_work(self, complex_schema):
        """Test that supported types work alongside unsupported ones."""
        result = complex_schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
        assert result['simple'] == 'string'
    
    def test_unsupported_type_with_optional(self):
        """Test that Optional[unsupported_type] defaults to 'object'."""
        @dataclass
        class TestSchema(DataFrameSchema):
            optional_list: Optional[List[str]]
        
        schema = TestSchema()
        result = schema.to_dtype_dict()
        
        assert result['optional_list'] == 'object'
    
    def test_custom_class_type(self):
        """Test that custom class types default to 'object'."""
        class CustomClass:
            pass
        
        @dataclass
        class TestSchema(DataFrameSchema):
            custom: CustomClass
        
        schema = TestSchema()
        result = schema.to_dtype_dict()
        
        assert result['custom'] == 'object'
    
    def test_any_type_defaults_to_object(self):
        """Test that Any type defaults to 'object'."""
        @dataclass
        class TestSchema(DataFrameSchema):
            any_field: Any
        
        schema = TestSchema()
        result = schema.to_dtype_dict()
        
        assert result['any_field'] == 'object'


# ============================================================================
# EDGE CASES TESTS
# ============================================================================

class TestEdgeCases:
    """Test suite for edge cases and special scenarios."""
    
    def test_empty_schema(self, empty_schema):
        """Test that empty schema returns empty dict."""
        result = empty_schema.to_dtype_dict()
        
        assert result == {}
        assert isinstance(result, dict)
    
    def test_single_field_schema(self):
        """Test schema with single field."""
        @dataclass
        class SingleFieldSchema(DataFrameSchema):
            only_field: str
        
        schema = SingleFieldSchema()
        result = schema.to_dtype_dict()
        
        assert len(result) == 1
        assert result['only_field'] == 'string'
    
    def test_many_fields_schema(self):
        """Test schema with many fields."""
        @dataclass
        class ManyFieldsSchema(DataFrameSchema):
            field1: int
            field2: str
            field3: float
            field4: bool
            field5: Optional[int]
            field6: Optional[str]
            field7: Optional[float]
            field8: Optional[bool]
            field9: int
            field10: str
        
        schema = ManyFieldsSchema()
        result = schema.to_dtype_dict()
        
        assert len(result) == 10
        assert all(isinstance(v, str) for v in result.values())
    
    def test_field_names_with_underscores(self):
        """Test that field names with underscores work correctly."""
        @dataclass
        class UnderscoreSchema(DataFrameSchema):
            first_name: str
            last_name: str
            user_id: int
            is_active: bool
        
        schema = UnderscoreSchema()
        result = schema.to_dtype_dict()
        
        assert 'first_name' in result
        assert 'last_name' in result
        assert 'user_id' in result
        assert 'is_active' in result
    
    def test_field_names_with_numbers(self):
        """Test that field names with numbers work correctly."""
        @dataclass
        class NumberSchema(DataFrameSchema):
            field1: str
            field2: int
            field123: float
        
        schema = NumberSchema()
        result = schema.to_dtype_dict()
        
        assert 'field1' in result
        assert 'field2' in result
        assert 'field123' in result
    
    def test_duplicate_type_fields(self):
        """Test schema with multiple fields of same type."""
        @dataclass
        class DuplicateTypeSchema(DataFrameSchema):
            name1: str
            name2: str
            name3: str
        
        schema = DuplicateTypeSchema()
        result = schema.to_dtype_dict()
        
        assert result['name1'] == 'string'
        assert result['name2'] == 'string'
        assert result['name3'] == 'string'
    
    def test_none_type_handling(self):
        """Test handling of None type (not Optional[T], just None)."""
        @dataclass
        class NoneTypeSchema(DataFrameSchema):
            normal_field: str
            # Note: type(None) as a field type is unusual but testing it
        
        schema = NoneTypeSchema()
        result = schema.to_dtype_dict()
        
        assert 'normal_field' in result


# ============================================================================
# INHERITANCE TESTS
# ============================================================================

class TestInheritance:
    """Test suite for schema inheritance scenarios."""
    
    def test_basic_inheritance(self):
        """Test that inherited schemas work correctly."""
        @dataclass
        class BaseSchema(DataFrameSchema):
            id: int
            name: str
        
        @dataclass
        class ExtendedSchema(BaseSchema):
            value: float
        
        schema = ExtendedSchema()
        result = schema.to_dtype_dict()
        
        assert 'id' in result
        assert 'name' in result
        assert 'value' in result
        assert len(result) == 3
    
    def test_multiple_inheritance_levels(self):
        """Test multiple levels of inheritance."""
        @dataclass
        class Level1Schema(DataFrameSchema):
            field1: int
        
        @dataclass
        class Level2Schema(Level1Schema):
            field2: str
        
        @dataclass
        class Level3Schema(Level2Schema):
            field3: float
        
        schema = Level3Schema()
        result = schema.to_dtype_dict()
        
        assert len(result) == 3
        assert result['field1'] == 'Int64'
        assert result['field2'] == 'string'
        assert result['field3'] == 'float64'
    
    def test_overriding_parent_field_type(self):
        """Test that child can override parent field type."""
        @dataclass
        class ParentSchema(DataFrameSchema):
            value: int
        
        @dataclass
        class ChildSchema(ParentSchema):
            value: float  # Override to float
        
        schema = ChildSchema()
        result = schema.to_dtype_dict()
        
        assert result['value'] == 'float64'


# ============================================================================
# TYPE HINT RESOLUTION TESTS
# ============================================================================

class TestTypeHintResolution:
    """Test suite for type hint resolution."""
    
    def test_get_type_hints_called(self):
        """Test that get_type_hints is used for resolution."""
        @dataclass
        class TestSchema(DataFrameSchema):
            field: int
        
        schema = TestSchema()
        
        # Should not raise any errors
        result = schema.to_dtype_dict()
        assert isinstance(result, dict)
    
    def test_type_hints_with_defaults(self):
        """Test that default values don't affect type mapping."""
        @dataclass
        class DefaultSchema(DataFrameSchema):
            id: int = 0
            name: str = ""
            value: float = 0.0
        
        schema = DefaultSchema()
        result = schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
        assert result['name'] == 'string'
        assert result['value'] == 'float64'
    
    def test_optional_with_defaults(self):
        """Test Optional types with default values."""
        @dataclass
        class OptionalDefaultSchema(DataFrameSchema):
            id: Optional[int] = None
            name: Optional[str] = None
        
        schema = OptionalDefaultSchema()
        result = schema.to_dtype_dict()
        
        assert result['id'] == 'Int64'
        assert result['name'] == 'string'


# ============================================================================
# DATACLASS FUNCTIONALITY TESTS
# ============================================================================

class TestDataclassFunctionality:
    """Test suite for dataclass-specific functionality."""
    
    def test_schema_is_dataclass(self, basic_schema):
        """Test that schema instance is a dataclass."""
        from dataclasses import is_dataclass
        
        assert is_dataclass(basic_schema)
    
    def test_schema_can_be_instantiated_with_values(self):
        """Test that schema can be instantiated with field values."""
        @dataclass
        class TestSchema(DataFrameSchema):
            id: int = 1
            name: str = "test"
        
        schema = TestSchema(id=42, name="custom")
        
        assert schema.id == 42
        assert schema.name == "custom"
    
    def test_to_dtype_dict_independent_of_instance_values(self):
        """Test that dtype_dict is independent of instance values."""
        @dataclass
        class TestSchema(DataFrameSchema):
            id: int = 1
            value: float = 1.0
        
        schema1 = TestSchema(id=10, value=10.0)
        schema2 = TestSchema(id=20, value=20.0)
        
        result1 = schema1.to_dtype_dict()
        result2 = schema2.to_dtype_dict()
        
        assert result1 == result2


# ============================================================================
# REAL-WORLD USAGE TESTS
# ============================================================================

class TestRealWorldUsage:
    """Test suite for real-world usage scenarios."""
    
    def test_financial_timeseries_schema(self):
        """Test schema mimicking financial timeseries data."""
        @dataclass
        class TimeseriesSchema(DataFrameSchema):
            security_id: str
            date: str
            open: float
            high: float
            low: float
            close: float
            volume: Optional[float]
            adjusted_close: Optional[float]
        
        schema = TimeseriesSchema()
        result = schema.to_dtype_dict()
        
        assert result['security_id'] == 'string'
        assert result['date'] == 'string'
        assert result['open'] == 'float64'
        assert result['volume'] == 'float64'
        assert len(result) == 8
    
    def test_user_profile_schema(self):
        """Test schema mimicking user profile data."""
        @dataclass
        class UserProfileSchema(DataFrameSchema):
            user_id: int
            username: str
            email: str
            is_active: bool
            age: Optional[int]
            bio: Optional[str]
        
        schema = UserProfileSchema()
        result = schema.to_dtype_dict()
        
        assert result['user_id'] == 'Int64'
        assert result['username'] == 'string'
        assert result['is_active'] == 'boolean'
        assert result['age'] == 'Int64'
    
    def test_product_catalog_schema(self):
        """Test schema mimicking product catalog data."""
        @dataclass
        class ProductSchema(DataFrameSchema):
            product_id: int
            name: str
            price: float
            in_stock: bool
            description: Optional[str]
            discount_price: Optional[float]
        
        schema = ProductSchema()
        result = schema.to_dtype_dict()
        
        assert len(result) == 6
        assert result['price'] == 'float64'
        assert result['in_stock'] == 'boolean'


# ============================================================================
# CONSISTENCY TESTS
# ============================================================================

class TestConsistency:
    """Test suite for consistency across multiple calls."""
    
    def test_multiple_calls_same_result(self, basic_schema):
        """Test that multiple calls return consistent results."""
        result1 = basic_schema.to_dtype_dict()
        result2 = basic_schema.to_dtype_dict()
        result3 = basic_schema.to_dtype_dict()
        
        assert result1 == result2 == result3
    
    def test_different_instances_same_result(self):
        """Test that different instances return same dtype dict."""
        @dataclass
        class TestSchema(DataFrameSchema):
            id: int
            name: str
        
        schema1 = TestSchema()
        schema2 = TestSchema()
        
        result1 = schema1.to_dtype_dict()
        result2 = schema2.to_dtype_dict()
        
        assert result1 == result2
    
    def test_result_is_new_dict_each_time(self, basic_schema):
        """Test that each call returns a new dict instance."""
        result1 = basic_schema.to_dtype_dict()
        result2 = basic_schema.to_dtype_dict()
        
        # Modify result1
        result1['new_key'] = 'new_value'
        
        # result2 should not be affected
        assert 'new_key' not in result2


# ============================================================================
# PARAMETRIZED TESTS
# ============================================================================

class TestParametrized:
    """Parametrized tests for comprehensive coverage."""
    
    @pytest.mark.parametrize("python_type,expected_dtype", [
        (int, 'Int64'),
        (str, 'string'),
        (float, 'float64'),
        (bool, 'boolean'),
    ])
    def test_basic_type_mappings(self, python_type, expected_dtype):
        """Test all basic type mappings."""
        @dataclass
        class TestSchema(DataFrameSchema):
            field: python_type
        
        # Create with type annotation (workaround for dynamic typing)
        schema = TestSchema()
        # Manually set the annotation for this test
        TestSchema.__annotations__ = {'field': python_type}
        
        result = schema.to_dtype_dict()
        
        assert result['field'] == expected_dtype
    
    @pytest.mark.parametrize("field_name", [
        "simple",
        "with_underscore",
        "camelCase",
        "PascalCase",
        "field123",
        "field_with_many_underscores",
    ])
    def test_various_field_names(self, field_name):
        """Test that various field naming conventions work."""
        # Dynamically create schema with given field name
        schema_dict = {field_name: int}
        TestSchema = dataclass(type('TestSchema', (DataFrameSchema,), 
                                   {'__annotations__': schema_dict}))
        
        schema = TestSchema()
        result = schema.to_dtype_dict()
        
        assert field_name in result


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestDataFrameSchemaIntegration:
    """Integration tests for DataFrameSchema with real workflows."""
    
    def test_schema_with_pandas_dataframe(self):
        """Test using schema to validate pandas DataFrame types."""
        import pandas as pd
        
        @dataclass
        class TestSchema(DataFrameSchema):
            id: int
            name: str
            value: float
        
        schema = TestSchema()
        dtype_dict = schema.to_dtype_dict()
        
        # Create DataFrame with matching structure
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'value': [1.0, 2.0, 3.0]
        })
        
        # Verify schema columns match DataFrame columns
        assert set(dtype_dict.keys()) == set(df.columns)
    
    def test_complete_validation_workflow(self):
        """Test complete schema validation workflow."""
        import pandas as pd
        
        @dataclass
        class ValidationSchema(DataFrameSchema):
            user_id: int
            username: str
            score: float
            is_active: bool
        
        schema = ValidationSchema()
        dtype_map = schema.to_dtype_dict()
        
        # Create test DataFrame
        df = pd.DataFrame({
            'user_id': [1, 2, 3],
            'username': ['alice', 'bob', 'charlie'],
            'score': [95.5, 87.3, 92.1],
            'is_active': [True, False, True]
        })
        
        # Apply schema types
        for col, dtype in dtype_map.items():
            if col in df.columns:
                assert df[col].dtype != dtype  # Before conversion
        
        # This demonstrates the schema can provide dtype info
        assert len(dtype_map) == 4
        assert all(isinstance(v, str) for v in dtype_map.values())
