"""Tests for security schema classes."""
import pytest
from dataclasses import fields
from typing import get_type_hints, get_args
from morningpy.schema.security import (
    FinancialStatementSchema,
    HoldingSchema,
    HoldingInfoSchema
)
from tests.units.schema.test_base import SchemaTestBase


@pytest.mark.schema
class TestFinancialStatementSchema(SchemaTestBase):
    """Test suite for FinancialStatementSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(FinancialStatementSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(FinancialStatementSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(FinancialStatementSchema)
    
    def test_has_security_identifier_fields(self):
        """Test that security identifier fields are present."""
        assert hasattr(FinancialStatementSchema, 'security_id')
        assert hasattr(FinancialStatementSchema, 'security_label')
    
    def test_has_statement_type_field(self):
        """Test that statement_type field exists."""
        assert hasattr(FinancialStatementSchema, 'statement_type')
    
    def test_has_sub_type_fields(self):
        """Test that sub_type fields are present."""
        schema_fields = {f.name for f in fields(FinancialStatementSchema)}
        sub_type_fields = [f'sub_type{i}' for i in range(1, 9)]
        
        for field in sub_type_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_all_fields_are_strings(self):
        """Test that all fields are string type."""
        type_hints = get_type_hints(FinancialStatementSchema)
        
        for field_name, field_type in type_hints.items():
            if field_name == 'return':
                continue
            args = get_args(field_type)
            assert args[0] is str, f"Field '{field_name}' should be str"


@pytest.mark.schema
class TestHoldingSchema(SchemaTestBase):
    """Test suite for HoldingSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(HoldingSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(HoldingSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(HoldingSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(HoldingSchema)
    
    def test_has_parent_child_relationship_fields(self):
        """Test that parent-child relationship fields exist."""
        assert hasattr(HoldingSchema, 'parent_security_id')
        assert hasattr(HoldingSchema, 'child_security_id')
    
    def test_has_essential_holding_fields(self):
        """Test that essential holding fields are present."""
        essential_fields = [
            'security_name', 'ticker', 'weighting', 'sector',
            'country', 'currency', 'holding_type'
        ]
        schema_fields = {f.name for f in fields(HoldingSchema)}
        
        for field in essential_fields:
            assert field in schema_fields, f"Missing field: {field}"
    
    def test_has_rating_fields(self):
        """Test that rating fields are present."""
        rating_fields = [
            'stock_rating', 'morningstar_rating', 'analyst_rating',
            'medalist_rating', 'qual_rating', 'quant_rating'
        ]
        schema_fields = {f.name for f in fields(HoldingSchema)}
        
        for field in rating_fields:
            assert field in schema_fields, f"Missing rating field: {field}"
    
    def test_numeric_fields_have_float_type(self):
        """Test that numeric fields are float."""
        numeric_fields = [
            'weighting', 'number_of_share', 'market_value',
            'share_change', 'total_return_1y', 'forward_pe_ratio'
        ]
        type_hints = get_type_hints(HoldingSchema)
        
        for field in numeric_fields:
            args = get_args(type_hints[field])
            assert args[0] is float, f"Field '{field}' should be float"
    
    def test_integer_fields_have_int_type(self):
        """Test that integer fields are int."""
        int_fields = ['ep_used_for_overall_rating', 'ep_used_for_1y_return', 'sus_esg_risk_globes']
        type_hints = get_type_hints(HoldingSchema)
        
        for field in int_fields:
            args = get_args(type_hints[field])
            assert args[0] is int, f"Field '{field}' should be int"
    
    def test_boolean_fields_have_bool_type(self):
        """Test that boolean fields are bool."""
        assert hasattr(HoldingSchema, 'is_momentum_filter_flag')
        type_hints = get_type_hints(HoldingSchema)
        args = get_args(type_hints['is_momentum_filter_flag'])
        assert args[0] is bool, "is_momentum_filter_flag should be bool"
    
    def test_has_esg_fields(self):
        """Test that ESG fields are present."""
        esg_fields = [
            'sus_esg_risk_score', 'sus_esg_risk_globes',
            'esg_as_of_date', 'sus_esg_risk_category'
        ]
        schema_fields = {f.name for f in fields(HoldingSchema)}
        
        for field in esg_fields:
            assert field in schema_fields, f"Missing ESG field: {field}"


@pytest.mark.schema
class TestHoldingInfoSchema(SchemaTestBase):
    """Test suite for HoldingInfoSchema."""
    
    def test_is_dataclass_schema(self):
        """Test that schema is a proper dataclass."""
        self.assert_is_dataclass_schema(HoldingInfoSchema)
    
    def test_can_instantiate(self):
        """Test that schema can be instantiated."""
        self.assert_can_instantiate(HoldingInfoSchema)
    
    def test_all_fields_optional(self):
        """Test that all fields are Optional."""
        self.assert_all_fields_optional(HoldingInfoSchema)
    
    def test_field_names_snake_case(self):
        """Test that field names are snake_case."""
        self.assert_field_names_snake_case(HoldingInfoSchema)
    
    def test_has_identifier_fields(self):
        """Test that identifier fields are present."""
        id_fields = [
            'security_id', 'master_portfolio_id',
            'base_currency_id', 'domicile_country_id'
        ]
        schema_fields = {f.name for f in fields(HoldingInfoSchema)}
        
        for field in id_fields:
            assert field in schema_fields, f"Missing ID field: {field}"
    
    def test_has_count_fields(self):
        """Test that holding count fields are present."""
        count_fields = [
            'number_of_holding', 'number_of_equity_holding',
            'number_of_bond_holding', 'number_of_other_holding'
        ]
        schema_fields = {f.name for f in fields(HoldingInfoSchema)}
        
        for field in count_fields:
            assert field in schema_fields, f"Missing count field: {field}"
    
    def test_count_fields_are_integers(self):
        """Test that count fields are integers."""
        count_fields = [
            'number_of_holding', 'number_of_equity_holding',
            'number_of_bond_holding', 'number_of_other_holding',
            'top_n_count'
        ]
        type_hints = get_type_hints(HoldingInfoSchema)
        
        for field in count_fields:
            args = get_args(type_hints[field])
            assert args[0] is int, f"Field '{field}' should be int"
    
    def test_percentage_fields_are_floats(self):
        """Test that percentage fields are floats."""
        pct_fields = [
            'number_of_equity_holding_percentage',
            'number_of_bond_holding_percentage',
            'number_of_other_holding_percentage',
            'top_holding_weighting',
            'last_turnover'
        ]
        type_hints = get_type_hints(HoldingInfoSchema)
        
        for field in pct_fields:
            args = get_args(type_hints[field])
            assert args[0] is float, f"Field '{field}' should be float"
    
    def test_has_summary_fields(self):
        """Test that summary fields are present."""
        assert hasattr(HoldingInfoSchema, 'top_holding_weighting')
        assert hasattr(HoldingInfoSchema, 'last_turnover')
        assert hasattr(HoldingInfoSchema, 'last_turnover_date')


# Parametrized tests for all security schemas
@pytest.mark.schema
@pytest.mark.parametrize("schema_class", [
    FinancialStatementSchema,
    HoldingSchema,
    HoldingInfoSchema,
])
class TestAllSecuritySchemas(SchemaTestBase):
    """Parametrized tests for all security schema classes."""
    
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
        """Test that all schemas have security_id or related field."""
        schema_fields = {f.name for f in fields(schema_class)}
        assert any('security_id' in field or 'security_label' in field 
                  for field in schema_fields), \
            "Schema should have security identifier field"