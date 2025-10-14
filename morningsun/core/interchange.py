import pandas as pd
import polars as pl
import dask.dataframe as dd
import modin.pandas as mpd
import pyarrow as pa

class DataFrameInterchange:
    """
    Classe universelle pour encapsuler des données tabulaires.
    Par défaut en pandas, avec conversions rapides vers polars, dask, pyarrow, modin, vaex, etc.
    """

    def __init__(self, data: pd.DataFrame):
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Les données initiales doivent être un pandas.DataFrame.")
        self._df = data

    # --- Accès direct ---
    @property
    def dataframe(self) -> pd.DataFrame:
        """Retourne le DataFrame pandas sous-jacent."""
        return self._df

    # --- Conversion pandas natives ---
    def to_csv(self, *args, **kwargs):
        return self._df.to_csv(*args, **kwargs)

    def to_json(self, *args, **kwargs):
        return self._df.to_json(*args, **kwargs)

    def to_dict(self, *args, **kwargs):
        return self._df.to_dict(*args, **kwargs)

    def to_excel(self, *args, **kwargs):
        return self._df.to_excel(*args, **kwargs)
    
    def to_parquet(self, *args, **kwargs):
        return self._df.to_parquet(*args, **kwargs)
    
    def to_sql(self, *args, **kwargs):
        return self._df.to_sql(*args, **kwargs)

    # --- Conversion vers d'autres engines ---
    def to_polars_dataframe(self):
        """Convertit en polars.DataFrame"""
        return pl.from_pandas(self._df)

    def to_dask_dataframe(self):
        """Convertit en dask.DataFrame"""
        return dd.from_pandas(self._df, npartitions=1)

    def to_modin_dataframe(self):
        """Convertit en modin.pandas.DataFrame"""
        return mpd.DataFrame(self._df)

    def to_arrow_table(self):
        """Convertit en pyarrow.Table"""
        return pa.Table.from_pandas(self._df)

    # --- Conversion universelle ---
    def to_engine(self, engine: str):
        """
        Convertit dynamiquement vers l'engine spécifié.
        Exemples :
            .to_engine("polars")
            .to_engine("dask")
        """
        engine = engine.lower()
        converters = {
            "pandas": self.dataframe,
            "polars": self.to_polars_dataframe,
            "dask": self.to_dask_dataframe,
            "modin": self.to_modin_dataframe,
            "vaex": self.to_vaex_dataframe,
            "arrow": self.to_arrow_table,
        }
        if engine not in converters:
            raise ValueError(f"Engine '{engine}' non supporté.")
        converter = converters[engine]
        return converter() if callable(converter) else converter

    # --- Représentation ---
    def __repr__(self):
        return f"<BaseDataResponse: {len(self._df)} lignes × {len(self._df.columns)} colonnes>\n{repr(self._df.head())}"