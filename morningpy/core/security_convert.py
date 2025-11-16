import pandas as pd
import warnings
from typing import List, Union

from morningpy.core.config import PATH_TICKERS

class IdSecurityConverter:
    """
    Convert various types of security identifiers (ticker, ISIN, security_id,
    performance_id) into standardized Morningstar ``security_id`` values.

    This class loads a local mapping from ``PATH_TICKERS`` containing all known
    correspondences between identifier types and Morningstar internal IDs.

    Typical workflow:
        - Normalize inputs into lists.
        - Look up identifiers in the mapping.
        - Validate Morningstar-style IDs (10-character alphanumeric strings).
        - Return a deduplicated list of valid ``security_id`` values.

    Attributes
    ----------
    ticker : List[str]
        List of ticker symbols to convert.
    isin : List[str]
        List of ISIN codes to convert.
    security_id : List[str]
        List of Morningstar internal ``security_id`` values to validate or convert.
    performance_id : List[str]
        List of Morningstar performer IDs to validate or convert.
    mapping : pd.DataFrame
        DataFrame containing all identifier correspondences.
    """

    def __init__(
        self,
        ticker: Union[str, List[str], None] = None,
        isin: Union[str, List[str], None] = None,
        security_id: Union[str, List[str], None] = None,
        performance_id: Union[str, List[str], None] = None,
    ):

        self.ticker = self._normalize_input(ticker)
        self.isin = self._normalize_input(isin)
        self.security_id = self._normalize_input(security_id)
        self.performance_id = self._normalize_input(performance_id)
        self.mapping = pd.read_parquet(PATH_TICKERS)

    @staticmethod
    def _normalize_input(value: Union[str, List[str], None]) -> List[str]:
        """
        Normalize any input value into a list.

        Parameters
        ----------
        value : str, list of str, or None
            A single identifier, a list of identifiers, or None.

        Returns
        -------
        list of str
            Normalized list of identifiers (empty if input was None).
        """
        if not value:
            return []
        return [value] if isinstance(value, str) else list(value)

    def _lookup_ids(self, values: List[str], column: str) -> List[str]:

        if not values:
            return []

        matches = self.mapping[self.mapping[column].isin(values)]

        duplicates = (
            matches.groupby(column)["security_id"]
            .nunique()
            .loc[lambda x: x > 1]
            .index.tolist()
        )
        if duplicates:
            warnings.warn(
                f"Multiple IDs found for {column}(s): {duplicates}. "
                f"All matching IDs will be included."
            )

        found_ids = matches["security_id"].dropna().unique().tolist()

        missing = set(values) - set(matches[column].unique())
        if missing:
            warnings.warn(f"No match found in column '{column}' for: {sorted(missing)}")

        return found_ids

    def _validate_ids(self, ids: List[str]) -> List[str]:

        if not ids:
            return []

        valid_format = [i for i in ids if isinstance(i, str) and len(i) == 10]
        invalid_format = set(ids) - set(valid_format)
        if invalid_format:
            warnings.warn(f"Invalid ID format detected: {sorted(invalid_format)}")

        valid_in_mapping = self.mapping[self.mapping["security_id"].isin(valid_format)
        ]["security_id"].unique().tolist()

        missing = set(valid_format) - set(valid_in_mapping)
        if missing:
            warnings.warn(
                f"The following IDs are not found in the mapping: {sorted(missing)}. "
                f"They will still be returned."
            )

        return valid_format

    def convert(self) -> List[str]:
        """
        Convert all provided identifiers into a unified list of Morningstar IDs.

        Parameters
        ----------
        None

        Returns
        -------
        list of str
            Sorted and deduplicated list of valid Morningstar ``security_id`` values.

        Notes
        -----
        Conversion steps:
            1. Validate provided ``security_id`` values.
            2. Look up matches for ``performance_id``, ``isin`` and ``ticker``.
            3. Deduplicate and sort all collected IDs.
        """
        ids = set()

        ids.update(self._validate_ids(self.security_id))
        ids.update(self._lookup_ids(self.performance_id, "performance_id"))
        ids.update(self._lookup_ids(self.isin, "isin"))
        ids.update(self._lookup_ids(self.ticker, "ticker"))

        return sorted(ids)
