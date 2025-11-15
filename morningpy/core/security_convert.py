import pandas as pd
import warnings
from typing import List, Union

from morningpy.core.config import PATH_TICKERS


class IdSecurityConverter:
    """
    A utility class to convert various types of security identifiers
    (ticker, ISIN, security_id, performance_id) into standardized Morningstar
    `security_id` values.

    This class loads a local mapping from `PATH_TICKERS` that links each identifier
    type (e.g., ticker, ISIN) to Morningstar internal identifiers. It supports
    flexible inputs (single strings or lists), validates identifier formats, and
    returns a deduplicated list of valid Morningstar IDs.

    Typical workflow:
        1. Normalize all provided inputs into lists.
        2. Look up each identifier type in the mapping.
        3. Validate Morningstar-style IDs (10-character alphanumeric strings).
        4. Return a sorted and deduplicated list of valid `security_id` values.

    Attributes:
        ticker (List[str]): List of ticker symbols to convert.
        isin (List[str]): List of ISIN codes to convert.
        security_id (List[str]): List of internal Morningstar security IDs to validate or convert.
        performance_id (List[str]): List of Morningstar performer IDs to validate or convert.
        mapping (pd.DataFrame): DataFrame containing the mapping between all identifier types.
    """

    def __init__(
        self,
        ticker: Union[str, List[str], None] = None,
        isin: Union[str, List[str], None] = None,
        security_id: Union[str, List[str], None] = None,
        performance_id: Union[str, List[str], None] = None,
    ):
        """
        Initialize the converter with one or multiple types of identifiers.

        Args:
            ticker: One or more ticker symbols.
            isin: One or more ISIN codes.
            security_id: One or more Morningstar internal security IDs.
            performance_id: One or more Morningstar performer IDs
                (validated and optionally cross-checked with the mapping file).
        """
        self.ticker = self._normalize_input(ticker)
        self.isin = self._normalize_input(isin)
        self.security_id = self._normalize_input(security_id)
        self.performance_id = self._normalize_input(performance_id)
        self.mapping = pd.read_parquet(PATH_TICKERS)

    @staticmethod
    def _normalize_input(value: Union[str, List[str], None]) -> List[str]:
        """
        Normalize any input value into a list.

        Args:
            value: A single identifier, a list of identifiers, or None.

        Returns:
            List[str]: A list of identifiers (empty if input was None).
        """
        if not value:
            return []
        return [value] if isinstance(value, str) else list(value)

    def _lookup_ids(self, values: List[str], column: str) -> List[str]:
        """
        Look up Morningstar `security_id` values from a specified column in the mapping.

        For each provided identifier (e.g., ticker, ISIN, performance_id), this method:
          - Retrieves all matching `security_id` entries.
          - Issues warnings if multiple mappings are found for the same identifier.
          - Warns when no corresponding match exists in the mapping.

        Args:
            values: List of identifiers to search for.
            column: Column name in the mapping DataFrame to query
                (e.g., "ticker", "isin", or "performance_id").

        Returns:
            List[str]: List of matching `security_id` values.
        """
        if not values:
            return []

        matches = self.mapping[self.mapping[column].isin(values)]

        # Identify ambiguous mappings (e.g., one ticker → multiple IDs)
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

        # Warn if some identifiers have no match
        missing = set(values) - set(matches[column].unique())
        if missing:
            warnings.warn(f"No match found in column '{column}' for: {sorted(missing)}")

        return found_ids

    def _validate_ids(self, ids: List[str]) -> List[str]:
        """
        Validate Morningstar `security_id` values.

        A valid Morningstar ID is a 10-character alphanumeric string (e.g., "0P0000BCG9").
        This method:
          - Filters out invalid formats.
          - Warns about invalid formats.
          - Checks whether valid IDs exist in the mapping.
          - Warns if valid-format IDs are missing from the mapping
            (they are still returned for potential external use).

        Args:
            ids: List of IDs to validate.

        Returns:
            List[str]: Valid `security_id` values.
        """
        if not ids:
            return []

        valid_format = [i for i in ids if isinstance(i, str) and len(i) == 10]
        invalid_format = set(ids) - set(valid_format)
        if invalid_format:
            warnings.warn(f"Invalid ID format detected: {sorted(invalid_format)}")

        valid_in_mapping = self.mapping[
            self.mapping["security_id"].isin(valid_format)
        ]["security_id"].unique().tolist()

        missing = set(valid_format) - set(valid_in_mapping)
        if missing:
            warnings.warn(
                f"The following IDs are not found in the mapping: {sorted(missing)}. "
                f"They will still be returned and can be used for direct API queries."
            )

        return valid_format

    def convert(self) -> List[str]:
        """
        Convert all provided identifiers (ticker, ISIN, security_id, performance_id)
        into a unified list of valid Morningstar `security_id` values.

        Conversion process:
            1. Validate and include correctly formatted `security_id` values.
            2. Look up additional matches for `performance_id`, `isin`, and `ticker`.
            3. Deduplicate and sort the resulting IDs.

        Returns:
            List[str]: Sorted, deduplicated list of valid Morningstar `security_id` values.
        """
        ids = set()

        ids.update(self._validate_ids(self.security_id))
        ids.update(self._lookup_ids(self.performance_id, "performance_id"))
        ids.update(self._lookup_ids(self.isin, "isin"))
        ids.update(self._lookup_ids(self.ticker, "ticker"))

        return sorted(ids)
