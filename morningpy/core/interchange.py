import pandas as pd
import polars as pl
import dask.dataframe as dd
import modin.pandas as mpd
import pyarrow as pa

class DataFrameInterchange(pd.DataFrame):
    """
    Pandas-compatible DataFrame with convenient conversion methods to multiple
    dataframe engines and formats such as Polars, Dask, Modin, and PyArrow.
    
    This class inherits from pandas.DataFrame and adds interoperability methods
    to enable fast conversions between different dataframe engines.
    """
    
    @property
    def _constructor(self):
        """
        Ensure operations on this DataFrame return a DataFrameInterchange instance.
        """
        return DataFrameInterchange
    
    def to_pandas_dataframe(self) -> pd.DataFrame:
        """
        Convert the current DataFrameInterchange instance to a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame equivalent of the current DataFrame.
        """
        return pd.DataFrame(self)

    def to_polars_dataframe(self) -> pl.DataFrame:
        """
        Convert the current DataFrameInterchange instance to a Polars DataFrame.

        Returns
        -------
        pl.DataFrame
            Polars DataFrame equivalent of the current DataFrame.
        """
        return pl.from_pandas(self)

    def to_dask_dataframe(self) -> dd.DataFrame:
        """
        Convert the current DataFrameInterchange instance to a Dask DataFrame.

        Returns
        -------
        dd.DataFrame
            Dask DataFrame equivalent of the current DataFrame.
        """
        return dd.from_pandas(self, npartitions=1)

    def to_modin_dataframe(self) -> mpd.DataFrame:
        """
        Convert the current DataFrameInterchange instance to a Modin DataFrame.

        Returns
        -------
        mpd.DataFrame
            Modin DataFrame equivalent of the current DataFrame.
        """
        return mpd.DataFrame(self)

    def to_arrow_table(self) -> pa.Table:
        """
        Convert the current DataFrameInterchange instance to a PyArrow Table.

        Returns
        -------
        pa.Table
            PyArrow Table equivalent of the current DataFrame.
        """
        return pa.Table.from_pandas(self)

    def to_engine(self, engine: str):
        """
        Dynamically convert the DataFrameInterchange to a specific engine.

        Parameters
        ----------
        engine : str
            Name of the target engine. Supported values:
            "pandas", "polars", "dask", "modin", "arrow".

        Returns
        -------
        object
            DataFrame or Table in the requested engine.

        Raises
        ------
        ValueError
            If the requested engine is not supported.
        """
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

    def __repr__(self) -> str:
        """
        Custom string representation showing row/column count with base pandas repr.

        Returns
        -------
        str
            String representation of the DataFrameInterchange.
        """
        base_repr = super().__repr__()
        return f"<DataFrameInterchange: {len(self)} rows × {len(self.columns)} cols>\n{base_repr}"
    
    def _repr_html_(self):
        """
        HTML representation for Jupyter and IDE viewers.
        """
        return super()._repr_html_()