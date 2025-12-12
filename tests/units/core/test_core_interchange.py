"""Tests for DataFrameInterchange module."""
import pytest
import pandas as pd
import polars as pl
import dask.dataframe as dd
import modin.pandas as mpd
import pyarrow as pa
import numpy as np
from unittest.mock import Mock, patch, MagicMock

from morningpy.core.interchange import DataFrameInterchange


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_data():
    """Provide sample data dict for DataFrame creation."""
    return {
        'col_int': [1, 2, 3, 4, 5],
        'col_float': [1.1, 2.2, 3.3, 4.4, 5.5],
        'col_str': ['a', 'b', 'c', 'd', 'e'],
        'col_bool': [True, False, True, False, True]
    }


@pytest.fixture
def sample_dataframe(sample_data):
    """Provide a standard pandas DataFrame."""
    return pd.DataFrame(sample_data)


@pytest.fixture
def sample_interchange(sample_data):
    """Provide a fresh DataFrameInterchange instance for each test."""
    return DataFrameInterchange(sample_data)


@pytest.fixture
def empty_interchange():
    """Provide an empty DataFrameInterchange instance."""
    return DataFrameInterchange()


@pytest.fixture
def complex_data():
    """Provide complex data with various types and null values."""
    return {
        'int_col': [1, 2, 3, None, 5],
        'float_col': [1.5, 2.5, np.nan, 4.5, 5.5],
        'str_col': ['apple', 'banana', None, 'date', 'elderberry'],
        'datetime_col': pd.date_range('2024-01-01', periods=5),
        'bool_col': [True, False, None, True, False],
    }


@pytest.fixture
def large_dataframe():
    """Provide a large DataFrame for performance testing."""
    n = 10000
    return pd.DataFrame({
        'id': range(n),
        'value': np.random.randn(n),
        'category': np.random.choice(['A', 'B', 'C'], n),
    })


# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

class TestDataFrameInterchangeInit:
    """Test suite for DataFrameInterchange initialization."""
    
    def test_init_from_dict(self, sample_data):
        """Test that initialization from dict creates proper instance."""
        df = DataFrameInterchange(sample_data)
        
        assert isinstance(df, DataFrameInterchange)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == list(sample_data.keys())
    
    def test_init_from_pandas_dataframe(self, sample_dataframe):
        """Test that initialization from pandas DataFrame works."""
        df = DataFrameInterchange(sample_dataframe)
        
        assert isinstance(df, DataFrameInterchange)
        pd.testing.assert_frame_equal(df, sample_dataframe)
    
    def test_init_empty(self):
        """Test that empty initialization works."""
        df = DataFrameInterchange()
        
        assert isinstance(df, DataFrameInterchange)
        assert len(df) == 0
        assert len(df.columns) == 0
    
    def test_init_with_index(self):
        """Test that custom index is preserved."""
        data = {'a': [1, 2, 3]}
        index = ['x', 'y', 'z']
        df = DataFrameInterchange(data, index=index)
        
        assert list(df.index) == index
    
    def test_init_with_columns(self):
        """Test that column names can be specified."""
        data = [[1, 2], [3, 4]]
        columns = ['col1', 'col2']
        df = DataFrameInterchange(data, columns=columns)
        
        assert list(df.columns) == columns
    
    def test_constructor_property_returns_class(self, sample_interchange):
        """Test that _constructor property returns DataFrameInterchange."""
        assert sample_interchange._constructor == DataFrameInterchange
    
    def test_operations_return_interchange_type(self, sample_interchange):
        """Test that DataFrame operations preserve type."""
        # Column selection
        result = sample_interchange[['col_int', 'col_float']]
        assert isinstance(result, DataFrameInterchange)
        
        # Head/tail
        result = sample_interchange.head(3)
        assert isinstance(result, DataFrameInterchange)
        
        # Filtering
        result = sample_interchange[sample_interchange['col_int'] > 2]
        assert isinstance(result, DataFrameInterchange)


# ============================================================================
# TO_PANDAS_DATAFRAME TESTS
# ============================================================================

class TestToPandasDataFrame:
    """Test suite for to_pandas_dataframe method."""
    
    def test_returns_pandas_dataframe_instance(self, sample_interchange):
        """Test that result is pandas DataFrame, not DataFrameInterchange."""
        result = sample_interchange.to_pandas_dataframe()
        
        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, DataFrameInterchange)
    
    def test_preserves_all_data(self, sample_interchange):
        """Test that all data is preserved during conversion."""
        result = sample_interchange.to_pandas_dataframe()
        
        pd.testing.assert_frame_equal(result, pd.DataFrame(sample_interchange))
    
    def test_converts_empty_dataframe(self, empty_interchange):
        """Test that empty DataFrame converts correctly."""
        result = empty_interchange.to_pandas_dataframe()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_preserves_custom_index(self):
        """Test that custom index is preserved."""
        df = DataFrameInterchange({'a': [1, 2, 3]}, index=['x', 'y', 'z'])
        result = df.to_pandas_dataframe()
        
        assert list(result.index) == ['x', 'y', 'z']
    
    def test_preserves_data_types(self, complex_data):
        """Test that data types are preserved."""
        df = DataFrameInterchange(complex_data)
        result = df.to_pandas_dataframe()
        
        assert result['datetime_col'].dtype == complex_data['datetime_col'].dtype

# ============================================================================
# TO_POLARS_DATAFRAME TESTS
# ============================================================================

class TestToPolarsDataFrame:
    """Test suite for to_polars_dataframe method."""
    
    def test_returns_polars_dataframe_instance(self, sample_interchange):
        """Test that result is Polars DataFrame."""
        result = sample_interchange.to_polars_dataframe()
        
        assert isinstance(result, pl.DataFrame)
    
    def test_preserves_column_names(self, sample_interchange):
        """Test that column names are preserved."""
        result = sample_interchange.to_polars_dataframe()
        
        assert list(result.columns) == list(sample_interchange.columns)
    
    def test_preserves_row_count(self, sample_interchange):
        """Test that row count is preserved."""
        result = sample_interchange.to_polars_dataframe()
        
        assert len(result) == len(sample_interchange)
    
    def test_preserves_data_values(self, sample_interchange):
        """Test that data values match."""
        result = sample_interchange.to_polars_dataframe()
        
        for col in sample_interchange.columns:
            pandas_vals = sample_interchange[col].tolist()
            polars_vals = result[col].to_list()
            assert pandas_vals == polars_vals
    
    def test_converts_empty_dataframe(self, empty_interchange):
        """Test that empty DataFrame converts correctly."""
        result = empty_interchange.to_polars_dataframe()
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
    
    def test_handles_null_values(self):
        """Test that null values are handled correctly."""
        df = DataFrameInterchange({
            'a': [1, 2, None, 4],
            'b': ['x', None, 'y', 'z']
        })
        result = df.to_polars_dataframe()
        
        assert result['a'].null_count() == 1
        assert result['b'].null_count() == 1
    
    def test_converts_datetime_columns(self, complex_data):
        """Test that datetime columns convert properly."""
        df = DataFrameInterchange(complex_data)
        result = df.to_polars_dataframe()
        
        assert 'datetime_col' in result.columns
        assert result['datetime_col'].dtype == pl.Datetime
    
    def test_handles_large_dataframe(self, large_dataframe):
        """Test conversion of large DataFrame."""
        df = DataFrameInterchange(large_dataframe)
        result = df.to_polars_dataframe()
        
        assert len(result) == len(large_dataframe)
        assert isinstance(result, pl.DataFrame)
    
    @patch('polars.from_pandas')
    def test_calls_polars_from_pandas(self, mock_from_pandas, sample_interchange):
        """Test that conversion uses pl.from_pandas."""
        mock_from_pandas.return_value = Mock(spec=pl.DataFrame)
        
        sample_interchange.to_polars_dataframe()
        
        mock_from_pandas.assert_called_once()


# ============================================================================
# TO_DASK_DATAFRAME TESTS
# ============================================================================

class TestToDaskDataFrame:
    """Test suite for to_dask_dataframe method."""
    
    def test_returns_dask_dataframe_instance(self, sample_interchange):
        """Test that result is Dask DataFrame."""
        result = sample_interchange.to_dask_dataframe()
        
        assert isinstance(result, dd.DataFrame)
    
    def test_preserves_column_names(self, sample_interchange):
        """Test that column names are preserved."""
        result = sample_interchange.to_dask_dataframe()
        
        assert list(result.columns) == list(sample_interchange.columns)
    
    def test_creates_single_partition(self, sample_interchange):
        """Test that DataFrame has single partition."""
        result = sample_interchange.to_dask_dataframe()
        
        assert result.npartitions == 1
    
    def test_converts_empty_dataframe(self, empty_interchange):
        """Test that empty DataFrame converts correctly."""
        result = empty_interchange.to_dask_dataframe()
        
        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == 0
    
    def test_handles_large_dataframe(self, large_dataframe):
        """Test conversion of large DataFrame."""
        df = DataFrameInterchange(large_dataframe)
        result = df.to_dask_dataframe()
        
        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == len(large_dataframe)
    
    @patch('dask.dataframe.from_pandas')
    def test_calls_dask_from_pandas_with_npartitions(self, mock_from_pandas, sample_interchange):
        """Test that conversion uses dd.from_pandas with npartitions=1."""
        mock_from_pandas.return_value = Mock(spec=dd.DataFrame)
        
        sample_interchange.to_dask_dataframe()
        
        mock_from_pandas.assert_called_once()
        call_kwargs = mock_from_pandas.call_args[1]
        assert call_kwargs['npartitions'] == 1



# ============================================================================
# TO_ARROW_TABLE TESTS
# ============================================================================

class TestToArrowTable:
    """Test suite for to_arrow_table method."""
    
    def test_returns_arrow_table_instance(self, sample_interchange):
        """Test that result is PyArrow Table."""
        result = sample_interchange.to_arrow_table()
        
        assert isinstance(result, pa.Table)
    
    def test_preserves_row_count(self, sample_interchange):
        """Test that row count is preserved."""
        result = sample_interchange.to_arrow_table()
        
        assert result.num_rows == len(sample_interchange)
    
    def test_preserves_column_count(self, sample_interchange):
        """Test that column count is preserved."""
        result = sample_interchange.to_arrow_table()
        
        assert result.num_columns == len(sample_interchange.columns)
    
    def test_preserves_column_names(self, sample_interchange):
        """Test that column names are preserved."""
        result = sample_interchange.to_arrow_table()
        
        assert result.column_names == list(sample_interchange.columns)
    
    def test_converts_empty_dataframe(self, empty_interchange):
        """Test that empty DataFrame converts correctly."""
        result = empty_interchange.to_arrow_table()
        
        assert isinstance(result, pa.Table)
        assert result.num_rows == 0
    
    def test_roundtrip_conversion(self, sample_interchange):
        """Test roundtrip: DataFrame -> Arrow -> DataFrame."""
        arrow_table = sample_interchange.to_arrow_table()
        result = arrow_table.to_pandas()
        
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(sample_interchange),
            check_dtype=False
        )
    
    def test_preserves_data_values(self, sample_interchange):
        """Test that data values match."""
        result = sample_interchange.to_arrow_table()
        
        for i, col in enumerate(sample_interchange.columns):
            arrow_col = result.column(i).to_pylist()
            pandas_col = sample_interchange[col].tolist()
            assert arrow_col == pandas_col
    
    def test_handles_null_values(self):
        """Test that null values are handled correctly."""
        df = DataFrameInterchange({
            'a': [1, 2, None, 4],
            'b': ['x', None, 'y', 'z']
        })
        result = df.to_arrow_table()
        
        assert result.column('a').null_count == 1
        assert result.column('b').null_count == 1
    
    def test_converts_datetime_columns(self, complex_data):
        """Test that datetime columns convert properly."""
        df = DataFrameInterchange(complex_data)
        result = df.to_arrow_table()
        
        assert 'datetime_col' in result.column_names
        assert pa.types.is_timestamp(result.schema.field('datetime_col').type)
    
    def test_handles_large_dataframe(self, large_dataframe):
        """Test conversion of large DataFrame."""
        df = DataFrameInterchange(large_dataframe)
        result = df.to_arrow_table()
        
        assert result.num_rows == len(large_dataframe)
        assert isinstance(result, pa.Table)
    

# ============================================================================
# TO_ENGINE TESTS
# ============================================================================

class TestToEngine:
    """Test suite for to_engine method."""
    
    @pytest.mark.parametrize("engine,expected_type", [
        ("pandas", DataFrameInterchange),
        ("polars", pl.DataFrame),
        ("dask", dd.DataFrame),
        ("arrow", pa.Table),
    ])
    def test_converts_to_all_valid_engines(self, sample_interchange, engine, expected_type):
        """Test that all valid engines return correct type."""
        result = sample_interchange.to_engine(engine)
        
        assert isinstance(result, expected_type)
    
    def test_engine_name_case_insensitive(self, sample_interchange):
        """Test that engine parameter is case-insensitive."""
        result_lower = sample_interchange.to_engine("pandas")
        result_upper = sample_interchange.to_engine("PANDAS")
        result_mixed = sample_interchange.to_engine("PaNdAs")
        
        assert isinstance(result_lower, DataFrameInterchange)
        assert isinstance(result_upper, DataFrameInterchange)
        assert isinstance(result_mixed, DataFrameInterchange)
    
    def test_raises_on_invalid_engine(self, sample_interchange):
        """Test that invalid engine raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported engine 'invalid'"):
            sample_interchange.to_engine("invalid")
    
    def test_raises_on_empty_engine_string(self, sample_interchange):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported engine"):
            sample_interchange.to_engine("")
    
    def test_pandas_engine_returns_self(self, sample_interchange):
        """Test that pandas engine returns the same instance."""
        result = sample_interchange.to_engine("pandas")
        
        assert result is sample_interchange
    
    @pytest.mark.parametrize("unsupported_engine", [
        "numpy", "spark", "cudf", "ray", "vaex"
    ])
    def test_raises_on_unsupported_engines(self, sample_interchange, unsupported_engine):
        """Test that unsupported engines raise ValueError."""
        with pytest.raises(ValueError, match=f"Unsupported engine '{unsupported_engine}'"):
            sample_interchange.to_engine(unsupported_engine)


# ============================================================================
# REPR AND HTML TESTS
# ============================================================================

class TestRepresentation:
    """Test suite for __repr__ and _repr_html_ methods."""
    
    def test_repr_contains_type_identifier(self, sample_interchange):
        """Test that repr contains DataFrameInterchange identifier."""
        result = repr(sample_interchange)
        
        assert "<DataFrameInterchange:" in result
    
    def test_repr_contains_row_count(self, sample_interchange):
        """Test that repr shows row count."""
        result = repr(sample_interchange)
        
        assert "5 rows" in result
    
    def test_repr_contains_column_count(self, sample_interchange):
        """Test that repr shows column count."""
        result = repr(sample_interchange)
        
        assert "4 cols" in result
    
    def test_repr_empty_dataframe(self, empty_interchange):
        """Test repr of empty DataFrame."""
        result = repr(empty_interchange)
        
        assert "<DataFrameInterchange:" in result
        assert "0 rows" in result
        assert "0 cols" in result
    
    def test_repr_contains_data_preview(self, sample_interchange):
        """Test that repr includes data preview."""
        result = repr(sample_interchange)
        
        # Should contain column names or data
        assert any(col in result for col in ['col_int', 'col_float', 'col_str', 'col_bool'])
    
    def test_repr_single_row(self):
        """Test repr with single row."""
        df = DataFrameInterchange({'a': [1], 'b': [2]})
        result = repr(df)
        
        assert "1 rows" in result
        assert "2 cols" in result
    
    def test_repr_large_dataframe(self, large_dataframe):
        """Test repr of large DataFrame."""
        df = DataFrameInterchange(large_dataframe)
        result = repr(df)
        
        assert "10000 rows" in result
        assert "3 cols" in result
    
    def test_repr_html_returns_string(self, sample_interchange):
        """Test that _repr_html_ returns string."""
        result = sample_interchange._repr_html_()
        
        assert isinstance(result, str)
        assert len(result) > 0
    
    def test_repr_html_contains_html_tags(self, sample_interchange):
        """Test that _repr_html_ contains HTML."""
        result = sample_interchange._repr_html_()
        
        assert "<table" in result or "<div" in result
    
    def test_repr_html_empty_dataframe(self, empty_interchange):
        """Test HTML repr of empty DataFrame."""
        result = empty_interchange._repr_html_()
        
        assert isinstance(result, str)


# ============================================================================
# PANDAS COMPATIBILITY TESTS
# ============================================================================

class TestPandasCompatibility:
    """Test suite for pandas compatibility."""
    
    def test_column_access_returns_series(self, sample_interchange):
        """Test that column access returns Series."""
        result = sample_interchange['col_int']
        
        assert isinstance(result, pd.Series)
    
    def test_column_assignment_works(self, sample_interchange):
        """Test that new column can be assigned."""
        sample_interchange['new_col'] = [10, 20, 30, 40, 50]
        
        assert 'new_col' in sample_interchange.columns
        assert sample_interchange['new_col'].tolist() == [10, 20, 30, 40, 50]
    
    def test_loc_indexing_works(self, sample_interchange):
        """Test that loc indexing works."""
        result = sample_interchange.loc[0]
        
        assert isinstance(result, pd.Series)
        assert result['col_int'] == 1
    
    def test_boolean_filtering_works(self, sample_interchange):
        """Test that boolean filtering works."""
        result = sample_interchange[sample_interchange['col_int'] > 2]
        
        assert isinstance(result, DataFrameInterchange)
        assert len(result) == 3
    
    def test_aggregation_methods_work(self, sample_interchange):
        """Test that aggregation works."""
        result = sample_interchange['col_int'].sum()
        
        assert result == 15
    
    def test_merge_operations_work(self, sample_interchange):
        """Test that merge works."""
        other = DataFrameInterchange({'col_int': [1, 2], 'extra': [100, 200]})
        result = sample_interchange.merge(other, on='col_int')
        
        assert isinstance(result, DataFrameInterchange)
        assert 'extra' in result.columns
    
    def test_groupby_operations_work(self, sample_interchange):
        """Test that groupby works."""
        sample_interchange['group'] = ['A', 'B', 'A', 'B', 'A']
        result = sample_interchange.groupby('group')['col_int'].sum()
        
        assert isinstance(result, pd.Series)
        assert result['A'] == 9  # 1 + 3 + 5
        assert result['B'] == 6  # 2 + 4
    
    def test_iteration_works(self, sample_interchange):
        """Test that row iteration works."""
        count = 0
        for idx, row in sample_interchange.iterrows():
            count += 1
        
        assert count == 5
    
    def test_copy_creates_independent_instance(self, sample_interchange):
        """Test that copy creates independent DataFrame."""
        result = sample_interchange.copy()
        
        assert isinstance(result, DataFrameInterchange)
        assert result is not sample_interchange
        pd.testing.assert_frame_equal(result, sample_interchange)
    
    def test_reset_index_preserves_type(self, sample_interchange):
        """Test that reset_index returns DataFrameInterchange."""
        result = sample_interchange.reset_index(drop=True)
        
        assert isinstance(result, DataFrameInterchange)
    
    def test_sort_values_preserves_type(self, sample_interchange):
        """Test that sort_values returns DataFrameInterchange."""
        result = sample_interchange.sort_values('col_int', ascending=False)
        
        assert isinstance(result, DataFrameInterchange)
        assert result.iloc[0]['col_int'] == 5
    
    def test_dropna_preserves_type(self):
        """Test that dropna returns DataFrameInterchange."""
        df = DataFrameInterchange({'a': [1, None, 3]})
        result = df.dropna()
        
        assert isinstance(result, DataFrameInterchange)
        assert len(result) == 2


# ============================================================================
# EDGE CASES TESTS
# ============================================================================

class TestEdgeCases:
    """Test suite for edge cases and special scenarios."""
    
    def test_single_column_dataframe(self):
        """Test DataFrame with single column."""
        df = DataFrameInterchange({'single': [1, 2, 3]})
        
        assert isinstance(df, DataFrameInterchange)
        assert len(df.columns) == 1
        assert isinstance(df.to_pandas_dataframe(), pd.DataFrame)
        assert isinstance(df.to_polars_dataframe(), pl.DataFrame)
    
    def test_single_row_dataframe(self):
        """Test DataFrame with single row."""
        df = DataFrameInterchange({'a': [1], 'b': [2], 'c': [3]})
        
        assert len(df) == 1
        assert isinstance(df.to_arrow_table(), pa.Table)
    
    def test_all_null_column(self):
        """Test column with all null values."""
        df = DataFrameInterchange({'nulls': [None, None, None]})
        
        assert df['nulls'].isna().all()
        result = df.to_polars_dataframe()
        assert result['nulls'].null_count() == 3
    
    def test_mixed_types_column(self):
        """Test column with mixed types (object dtype)."""
        df = DataFrameInterchange({'mixed': [1, 'two', 3.0, None]})
        
        assert df['mixed'].dtype == object
        assert isinstance(df.to_pandas_dataframe(), pd.DataFrame)
    
    def test_unicode_column_names(self):
        """Test DataFrame with unicode column names."""
        df = DataFrameInterchange({'名前': [1, 2], 'città': [3, 4]})
        
        assert '名前' in df.columns
        assert 'città' in df.columns
        result = df.to_polars_dataframe()
        assert '名前' in result.columns
    
    def test_unicode_data_values(self):
        """Test DataFrame with unicode data."""
        df = DataFrameInterchange({
            'text': ['Hello', 'Héllo', '你好', 'مرحبا']
        })
        
        result = df.to_arrow_table()
        assert result.num_rows == 4
    
    def test_very_long_column_names(self):
        """Test DataFrame with very long column names."""
        long_name = 'a' * 1000
        df = DataFrameInterchange({long_name: [1, 2, 3]})
        
        assert long_name in df.columns
        result = df.to_pandas_dataframe()
        assert long_name in result.columns
    
    def test_numeric_column_names(self):
        """Test DataFrame with numeric column names."""
        df = DataFrameInterchange({0: [1, 2], 1: [3, 4]})
        
        assert 0 in df.columns
        assert 1 in df.columns
    
    def test_special_characters_in_column_names(self):
        """Test column names with special characters."""
        df = DataFrameInterchange({
            'col@1': [1, 2],
            'col#2': [3, 4],
            'col$3': [5, 6]
        })
        
        assert 'col@1' in df.columns
        result = df.to_polars_dataframe()
        assert 'col@1' in result.columns


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestDataFrameInterchangeIntegration:
    """Integration tests for complete workflows."""
    
    def test_complete_conversion_workflow(self, sample_dataframe):
        """Test complete workflow: pandas -> interchange -> all engines."""
        df = DataFrameInterchange(sample_dataframe)
        
        results = {
            'pandas': df.to_pandas_dataframe(),
            'polars': df.to_polars_dataframe(),
            'dask': df.to_dask_dataframe(),
            'arrow': df.to_arrow_table()
        }
        
        assert all(result is not None for result in results.values())
    
    def test_roundtrip_polars_conversion(self, sample_interchange):
        """Test roundtrip: Interchange -> Polars -> Pandas -> Interchange."""
        polars_df = sample_interchange.to_polars_dataframe()
        back_to_pandas = polars_df.to_pandas()
        final = DataFrameInterchange(back_to_pandas)
        
        pd.testing.assert_frame_equal(
            final,
            sample_interchange,
            check_dtype=False
        )
    
    def test_chained_pandas_operations(self, sample_interchange):
        """Test that chained operations preserve type."""
        result = (sample_interchange
                  .query('col_int > 2')
                  .assign(new_col=lambda x: x['col_int'] * 2)
                  .head(2))
        
        assert isinstance(result, DataFrameInterchange)
        assert 'new_col' in result.columns
        assert len(result) == 2
    
    def test_dynamic_engine_selection_workflow(self, sample_interchange):
        """Test dynamic engine selection in workflow."""
        engines = ['pandas', 'polars', 'arrow']
        
        for engine in engines:
            result = sample_interchange.to_engine(engine)
            assert result is not None
    
    def test_multiple_sequential_conversions(self, sample_interchange):
        """Test multiple sequential conversions without errors."""
        for _ in range(5):
            _ = sample_interchange.to_pandas_dataframe()
            _ = sample_interchange.to_polars_dataframe()
            _ = sample_interchange.to_arrow_table()
        
        # Should complete without errors
        assert True
    
    def test_large_dataframe_all_conversions(self, large_dataframe):
        """Test that all conversions work with large DataFrame."""
        df = DataFrameInterchange(large_dataframe)
        
        pandas_df = df.to_pandas_dataframe()
        assert len(pandas_df) == 10000
        
        polars_df = df.to_polars_dataframe()
        assert len(polars_df) == 10000
        
        arrow_table = df.to_arrow_table()
        assert arrow_table.num_rows == 10000