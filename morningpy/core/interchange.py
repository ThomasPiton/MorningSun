import pandas as pd
import polars as pl
import dask.dataframe as dd
import modin.pandas as mpd
import pyarrow as pa

class DataFrameInterchange(pd.DataFrame):
    """
    Pandas-compatible DataFrame with fast conversion methods to Polars, Dask, Modin, PyArrow, etc.
    """
    @property
    def _constructor(self):
        return DataFrameInterchange
    
    def to_polars_dataframe(self):
        """Convert to polars.DataFrame"""
        return pl.from_pandas(self)

    def to_dask_dataframe(self):
        """Convert to dask.DataFrame"""
        return dd.from_pandas(self, npartitions=1)

    def to_modin_dataframe(self):
        """Convert to modin.pandas.DataFrame"""
        return mpd.DataFrame(self)

    def to_arrow_table(self):
        """Convert to pyarrow.Table"""
        return pa.Table.from_pandas(self)

    def to_engine(self, engine: str):
        """Convert dynamically to the requested engine."""
        engine = engine.lower()
        converters = {
            "pandas": lambda: self,
            "polars": self.to_polars_dataframe,
            "dask": self.to_dask_dataframe,
            "modin": self.to_modin_dataframe,
            "arrow": self.to_arrow_table,
        }
        if engine not in converters:
            raise ValueError(f"Unsupported engine '{engine}'.")
        return converters[engine]()

    def __repr__(self):
        base_repr = super().__repr__()
        return f"<DataFrameInterchange: {len(self)} rows × {len(self.columns)} cols>\n{base_repr}"

