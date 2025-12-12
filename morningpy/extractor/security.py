import pandas as pd
from typing import Any, Dict, List, Union

from morningpy.core.security_loader import SecurityLoader
from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.security import *
from morningpy.schema.security import *
    
import pandas as pd
from typing import Union, List, Dict, Any

from morningpy.core.security_loader import SecurityLoader
from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.security import FinancialStatementConfig
from morningpy.schema.security import FinancialStatementSchema


class FinancialStatementExtractor(BaseExtractor):
    """
    Extracts financial statement data (income statement, balance sheet, cash flow) from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs, statement type, report frequency)
        - Building API requests per security and statement type
        - Processing the API response into a standardized pandas DataFrame with normalized sub-levels

    Attributes
    ----------
    ticker : str or list of str or None
        Ticker symbols of securities.
    isin : str or list of str or None
        ISIN codes of securities.
    security_id : str or list of str or None
        Morningstar internal security IDs.
    performance_id : str or list of str or None
        Morningstar performance IDs.
    statement_type : list of str
        Type(s) of statement to extract ("income", "balance", "cashflow").
    report_frequency : str
        Report frequency ("annual", "quarterly", etc.).
    url : str
        Base API URL for financial statements.
    endpoint : dict
        Endpoint mapping per statement type.
    valid_frequency : dict
        Valid frequencies for reports.
    frequency_mapping : dict
        Mapping from user-friendly frequency to API frequency.
    metadata : list of dict
        Security metadata including IDs and labels.
    """
    config = FinancialStatementConfig
    schema = FinancialStatementSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,
        statement_type: Union[str, List[str]] = None,
        report_frequency: str = None
    ):
        """
        Initialize the FinancialStatementExtractor.

        Parameters
        ----------
        ticker : str or list of str, optional
            Single ticker symbol or list of ticker symbols (e.g., 'AAPL' or ['AAPL', 'MSFT']).
            Mutually exclusive with isin, security_id, and performance_id.
        isin : str or list of str, optional
            Single ISIN code or list of ISIN codes (e.g., 'US0378331005').
            Mutually exclusive with ticker, security_id, and performance_id.
        security_id : str or list of str, optional
            Single Morningstar security ID or list of IDs.
            Mutually exclusive with ticker, isin, and performance_id.
        performance_id : str or list of str, optional
            Single Morningstar performance ID or list of IDs.
            Mutually exclusive with ticker, isin, and security_id.
        statement_type : str or list of str, optional
            Type of financial statement to extract. Valid values: 
            {"income", "balance", "cashflow"}. Can be a single string or list of strings
            to extract multiple statement types.
        report_frequency : {"annual", "quarterly"}, optional
            Frequency of the financial reports. Determines the periodicity of data returned.

        Raises
        ------
        ValueError
            If invalid statement_type or report_frequency is provided.
            
        Notes
        -----
        At least one security identifier (ticker, isin, security_id, or performance_id)
        must be provided. If multiple are provided, the SecurityLookup class handles
        the priority.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)
        
        self.url = self.config.API_URL
        self.endpoint = self.config.ENDPOINT
        self.valid_frequency = self.config.VALID_FREQUENCY
        self.frequency_mapping = self.config.MAPPING_FREQUENCY
        self.report_frequency = report_frequency 
        self.filter_values = self.config.FILTER_VALUE
        self.params = self.config.PARAMS
        self.metadata = []
        
        self.statement_type = (
            [statement_type] if isinstance(statement_type, str) 
            else list(statement_type) if statement_type 
            else []
        )
        
        self.metadata = SecurityLoader(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).get(fields=["security_id", "security_label"])

    def _check_inputs(self) -> None:
        """
        Validate user inputs for statement type and report frequency.
        
        Notes
        -----
        This method is currently a placeholder for future validation logic.
        """
        pass

    def _build_request(self) -> None:
        """
        Build list of API requests for all securities and statement types.

        Notes
        -----
        Creates a list of dict-based request objects of the form:
            {
                "url": ...,
                "params": ...,
                "metadata": ...
            }
        for each (security × statement type) combination.
        """
        self.requests = [
            {
                "url": f"{self.url}{meta['security_id']}/{self.endpoint[stmt_type]}/detail",
                "params": {
                    **self.params,
                    "dataType": self.frequency_mapping[self.report_frequency],
                },
                "metadata": meta,
            }
            for meta in self.metadata
            for stmt_type in self.statement_type
        ]
    
    @staticmethod
    def clean_label(label: str) -> str:
        """
        Remove 'total' from labels and normalize whitespace.
        
        Parameters
        ----------
        label : str
            Raw label from financial statement API response.
        
        Returns
        -------
        str
            Cleaned label with 'total' removed and normalized whitespace.
        
        Examples
        --------
        >>> FinancialStatementExtractor.clean_label("Total Revenue")
        'Revenue'
        >>> FinancialStatementExtractor.clean_label("  Operating   Income  ")
        'Operating Income'
        """
        return " ".join(word for word in label.split() if word.lower() != "total")
    
    @staticmethod
    def normalize_value(value: Any) -> float:
        """
        Convert value to float, handling None and special cases.
        
        Parameters
        ----------
        value : any
            Value from financial statement API response. Can be int, float,
            str, None, or special marker like '_PO_' (preliminary/omitted).
        
        Returns
        -------
        float
            Normalized float value. Returns 0.0 for None, '_PO_', or
            values that cannot be converted to float.
        
        Examples
        --------
        >>> FinancialStatementExtractor.normalize_value(1000)
        1000.0
        >>> FinancialStatementExtractor.normalize_value(None)
        0.0
        >>> FinancialStatementExtractor.normalize_value('_PO_')
        0.0
        >>> FinancialStatementExtractor.normalize_value("123.45")
        123.45
        """
        if value in (None, "_PO_"):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def recursive_tree(
        node: dict, 
        path: list, 
        period_cols: list,
        data_rows: list,
        depth_tracker: list
    ) -> None:
        """
        Recursively traverse hierarchical tree structure and collect leaf nodes.
        
        This method navigates the nested financial statement structure, building
        paths through the hierarchy and collecting leaf node data with values.
        
        Parameters
        ----------
        node : dict
            Current node in the tree structure containing:
            - label : str
                Display label for this line item
            - datum : list
                Data values for all periods (if leaf node)
            - subLevel : list of dict
                Child nodes (if not a leaf)
        path : list
            Current path from root to this node (list of labels).
        period_cols : list
            Column names for period data (dates/quarters).
        data_rows : list
            Accumulator list for collecting leaf node data.
            Modified in place.
        depth_tracker : list
            Accumulator list for tracking path depths.
            Modified in place.
        
        Returns
        -------
        None
            Modifies data_rows and depth_tracker lists in place.
        
        Notes
        -----
        This is a recursive function that:
        1. Cleans and validates the current node's label
        2. Tracks the depth of the current path
        3. If leaf node (no subLevel): extracts data values
        4. If parent node (has subLevel): recurses on children
        
        Examples
        --------
        >>> node = {
        ...     "label": "Revenue",
        ...     "datum": [None, None, None, None, None, 1000, 1100, 1200],
        ...     "subLevel": []
        ... }
        >>> data_rows = []
        >>> depth_tracker = []
        >>> FinancialStatementExtractor.recursive_tree(
        ...     node, [], ["2022", "2023", "2024"], data_rows, depth_tracker
        ... )
        >>> len(data_rows)
        1
        >>> data_rows[0]['_path']
        ['Revenue']
        """
        current_label = FinancialStatementExtractor.clean_label(
            node.get("label", "")
        ).strip()
        
        if not current_label:
            return
            
        current_path = path + [current_label]
        depth_tracker.append(len(current_path))

        # Check if this is a leaf node (no children)
        if not node.get("subLevel"):
            datum = node.get("datum", [])
            if datum and len(datum) > 5:
                values = [
                    FinancialStatementExtractor.normalize_value(v) 
                    for v in datum[5:]
                ]
                row = {"_path": current_path}
                row.update(dict(zip(period_cols, values)))
                data_rows.append(row)
            return
        
        # Recurse on children if this is a parent node
        for child in node.get("subLevel", []):
            FinancialStatementExtractor.recursive_tree(
                child, 
                current_path, 
                period_cols,
                data_rows,
                depth_tracker
            )
        
    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process API response into a normalized DataFrame with hierarchical financial data.
        
        This method performs the following operations:
            - Extracts column definitions and period information
            - Recursively traverses the hierarchical tree structure of financial line items
            - Normalizes nested levels to consistent depth
            - Filters data by statement type
            - Scales values to millions (multiplies by 10^6)
        
        Parameters
        ----------
        response : dict
            API response dictionary containing:
            - columnDefs : list
                Column definitions including period labels
            - metadata : dict
                Security identification information
            - rows : list
                Hierarchical tree structure of financial statement items
            - _meta : dict
                Additional metadata including statement type
            
        Returns
        -------
        pd.DataFrame
            Normalized DataFrame with columns:
            - id_security : str
                Morningstar security ID
            - security_label : str
                Security name/label
            - statement_type : str
                Top-level category (filtered)
            - sub_type1, sub_type2, ... : str
                Hierarchical line item levels
            - Period columns : float
                Financial values for each reporting period (in millions)
            
            Returns empty DataFrame if response is invalid or contains no data.
        """
        # Early validation
        if not isinstance(response, dict) or not response:
            return pd.DataFrame()
        
        # Extract metadata
        column_defs = response.get("columnDefs", [])
        period_cols = column_defs[5:]
        
        metadata = response.get("metadata", {})
        security_id = metadata.get("security_id")
        security_label = metadata.get("security_label")
        
        statement_type = response.get("_meta", {}).get("statementType")
        
        if not statement_type or statement_type not in self.filter_values:
            return pd.DataFrame()
        
        data_rows = []
        depth_tracker = []
        
        # Build data rows using static recursive_tree method
        for item in response.get("rows", []):
            self.recursive_tree(item, [], period_cols, data_rows, depth_tracker)
        
        if not data_rows:
            return pd.DataFrame()
        
        # Normalize path depth
        max_depth = max(depth_tracker)
        
        normalized_rows = []
        for row in data_rows:
            path = row.pop("_path")
            padded_path = path + [path[-1]] * (max_depth - len(path))
            for i, label in enumerate(padded_path):
                row[f"sub_type{i}"] = label
            
            normalized_rows.append(row)
        
        df = pd.DataFrame(normalized_rows)
        df.rename(columns={"sub_type0": "statement_type"}, inplace=True)
        subtype_cols = ["statement_type"] + [f"sub_type{i}" for i in range(1, max_depth)]
        df = df[subtype_cols + period_cols]
        
        # Filter by statement type
        filter_value = self.filter_values[statement_type]
        df = df[df["statement_type"].isin([filter_value])]
        
        # Scale to millions
        df.loc[:, period_cols] = df.loc[:, period_cols] * 10**6
        
        # Add security identifiers
        df.insert(0, "id_security", security_id)
        df.insert(1, "security_label", security_label)
        
        return df
    

class HoldingExtractor(BaseExtractor):
    """
    Extracts ETF or fund holdings data from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs)
        - Building API requests per security
        - Processing ETF/fund holdings including equity, bond, and other holdings into a standardized pandas DataFrame

    Attributes
    ----------
    ticker : str or list of str or None
        Ticker symbols of securities.
    isin : str or list of str or None
        ISIN codes of securities.
    security_id : str or list of str or None
        Morningstar internal security IDs.
    performance_id : str or list of str or None
        Morningstar performance IDs.
    url : str
        Base API URL for holdings.
    params : dict
        Request parameters for API calls.
    field_mapping : dict
        Mapping of standardized field names to API field names.
    rename_columns : dict
        Mapping of API column names to standardized names.
    str_columns : list of str
        Columns to treat as strings.
    numeric_columns : list of str
        Columns to treat as numeric.
    final_columns : list of str
        Final column order for the DataFrame.
    metadata : list of dict
        Security metadata including IDs and labels.
    """
    config = HoldingConfig
    schema = HoldingSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,
    ):
        """
        Initialize the HoldingExtractor.

        Parameters
        ----------
        ticker : str or list of str, optional
            Single ticker symbol or list of ticker symbols for ETFs/funds
            (e.g., 'SPY' or ['SPY', 'QQQ']). Mutually exclusive with isin, 
            security_id, and performance_id.
        isin : str or list of str, optional
            Single ISIN code or list of ISIN codes for ETFs/funds
            (e.g., 'US78462F1030'). Mutually exclusive with ticker, security_id, 
            and performance_id.
        security_id : str or list of str, optional
            Single Morningstar security ID or list of IDs for ETFs/funds.
            Mutually exclusive with ticker, isin, and performance_id.
        performance_id : str or list of str, optional
            Single Morningstar performance ID or list of IDs for ETFs/funds.
            Mutually exclusive with ticker, isin, and security_id.

        Notes
        -----
        At least one security identifier (ticker, isin, security_id, or performance_id)
        must be provided. The extractor will retrieve detailed holdings information
        including equity holdings, bond holdings, and other asset holdings for each
        specified security.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.field_mapping = self.config.FIELD_MAPPING
        self.rename_columns = self.config.RENAME_COLUMNS
        self.columns = self.config.COLUMNS
        
        self.metadata = SecurityLoader(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).get(["security_id", "security_label"])      

    def _check_inputs(self) -> None:
        """
        Validate user inputs for holdings extraction.
        
        Notes
        -----
        This method is currently a placeholder for future validation logic.
        """
        pass

    def _build_request(self) -> None:
        """
        Build list of API requests for all securities.

        Notes
        -----
        Creates a list of dict-based request objects of the form:
            { "url": ..., "params": ..., "metadata": ... }
        for each security to be requested.
        """
        self.requests = [
            {
                "url": f"{self.url  }{meta['security_id']}/data",
                "params": {**self.params }, 
                "metadata": meta,
            }
            for meta in self.metadata
        ]
            
    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar ETF holdings response across equity, bond, and other holdings.
        
        This method performs the following operations:
            - Extracts holdings from equity, bond, and other holding pages
            - Uses config-based field mapping for cleaner data extraction
            - Normalizes field names and data types
            - Handles missing values (strings: 'N/A', numerics: 0)
            - Converts date fields to datetime
            - Sorts results by parent and child security IDs
        
        Parameters
        ----------
        response : dict
            API response dictionary containing:
            - metadata : dict
                Parent security identification (security_id, security_label)
            - equityHoldingPage : dict
                Equity holdings with holdingList
            - boldHoldingPage : dict
                Bond holdings with holdingList
            - otherHoldingPage : dict
                Other holdings with holdingList
                
        Returns
        -------
        pd.DataFrame
            DataFrame with columns for each holding including:
            - parent_security_id, parent_security_name : str
                Parent ETF/fund identifiers
            - child_security_id, security_name : str
                Holding identifiers
            - weighting, market_value : float
                Position information
            - sector, country : str
                Classification data
            - Various rating and performance metrics
            
            Returns empty DataFrame if response is invalid or contains no holdings.
        """
        if not isinstance(response, dict) or not response:
            return pd.DataFrame()

        metadata = response.get("metadata", {})
        security_id = metadata.get("security_id")
        security_label = metadata.get("security_label")
        
        holding_pages = ["equityHoldingPage", "boldHoldingPage", "otherHoldingPage"]
        rows = []

        for page_key in holding_pages:
            page = response.get(page_key, {})
            holding_list = page.get("holdingList", [])
            
            for holding in holding_list:
                row = {
                    "parent_security_id": security_id,
                    "parent_security_name": security_label,
                }
                row.update({key: holding.get(value) for key, value in self.field_mapping.items()})
                rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.columns]
        df.sort_values(
            by=["parent_security_id", "child_security_id"], 
            inplace=True, 
            ignore_index=True
        )

        return df
             

class HoldingInfoExtractor(BaseExtractor):
    """
    Extracts high-level metadata about ETF or fund holdings from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs)
        - Building API requests per security
        - Processing holding info response into a single-row pandas DataFrame per security, 
          including top holdings, turnover, and other portfolio statistics

    Attributes
    ----------
    ticker : str or list of str or None
        Ticker symbols of securities.
    isin : str or list of str or None
        ISIN codes of securities.
    security_id : str or list of str or None
        Morningstar internal security IDs.
    performance_id : str or list of str or None
        Morningstar performance IDs.
    url : str
        Base API URL for holding info.
    params : dict
        Request parameters for API calls.
    field_mapping : dict
        Mapping of standardized field names to API field names.
    holding_summary_mapping : dict
        Mapping for holdingSummary nested fields.
    rename_columns : dict
        Mapping of API column names to standardized names.
    str_columns : list of str
        Columns to treat as strings.
    numeric_columns : list of str
        Columns to treat as numeric.
    final_columns : list of str
        Final column order for the DataFrame.
    metadata : list of dict
        Security metadata including IDs and labels.
    """
    config = HoldingInfoConfig
    schema = HoldingInfoSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,
    ):
        """
        Initialize the HoldingInfoExtractor.

        Parameters
        ----------
        ticker : str or list of str, optional
            Single ticker symbol or list of ticker symbols for ETFs/funds
            (e.g., 'VOO' or ['VOO', 'VTI']). Mutually exclusive with isin, 
            security_id, and performance_id.
        isin : str or list of str, optional
            Single ISIN code or list of ISIN codes for ETFs/funds
            (e.g., 'US9229087690'). Mutually exclusive with ticker, security_id, 
            and performance_id.
        security_id : str or list of str, optional
            Single Morningstar security ID or list of IDs for ETFs/funds.
            Mutually exclusive with ticker, isin, and performance_id.
        performance_id : str or list of str, optional
            Single Morningstar performance ID or list of IDs for ETFs/funds.
            Mutually exclusive with ticker, isin, and security_id.

        Notes
        -----
        At least one security identifier (ticker, isin, security_id, or performance_id)
        must be provided. The extractor will retrieve summary-level holdings information
        including portfolio statistics, turnover ratios, and holding counts for each
        specified security.
        """
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.field_mapping = self.config.FIELD_MAPPING
        self.holding_summary_mapping = self.config.HOLDING_SUMMARY_MAPPING
        self.rename_columns = self.config.RENAME_COLUMNS
        self.columns = self.config.COLUMNS
        
        self.metadata = SecurityLoader(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).get(["security_id", "security_label"])      

    def _check_inputs(self) -> None:
        """
        Validate user inputs for holdings info extraction.
        
        Notes
        -----
        This method is currently a placeholder for future validation logic.
        """
        pass

    def _build_request(self) -> None:
        """
        Build list of API requests for all securities.

        Notes
        -----
        Creates a list of dict-based request objects of the form:
            {
                "url": ...,
                "params": ...,
                "metadata": ...
            }
        for each security to be requested.
        """
        self.requests = [
            {
                "url": f"{self.url}{meta['security_id']}/data",
                "params": {**self.params},
                "metadata": meta,
            }
            for meta in self.metadata
        ]
        
    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar ETF Holding Info API response safely and cleanly.
        
        This method performs the following operations:
            - Extracts portfolio-level metadata and statistics using config-based field mapping
            - Processes holding summary information (top holdings weight, turnover)
            - Handles both long and short positions
            - Normalizes field names and data types
            - Handles missing values (strings: 'N/A', numerics: 0)
            - Converts date fields to datetime
        
        Parameters
        ----------
        response : dict
            API response dictionary containing:
            - metadata : dict
                Security identification (security_id, security_label)
            - masterPortfolioId : str
                Portfolio identifier
            - numberOfHolding, numberOfEquityHolding, etc. : int
                Holding counts
            - holdingSummary : dict
                Summary statistics including topHoldingWeighting and lastTurnover
            - assetType, baseCurrencyId, etc. : various
                Portfolio attributes
                
        Returns
        -------
        pd.DataFrame
            Single-row DataFrame with columns:
            - master_portfolio_id, security_id, security_label : str
                Identifiers
            - number_of_holding, number_of_equity_holding, etc. : int
                Holding counts
            - top_holding_weighting : float
                Percentage weight of top holdings
            - last_turnover : float
                Portfolio turnover ratio
            - last_turnover_date : datetime
                Date of turnover calculation
            - Various other portfolio attributes
            
            Returns empty DataFrame if response is invalid.
        """
        if not isinstance(response, dict) or not response:
            return pd.DataFrame()

        metadata = response.get("metadata",{})
        security_id = metadata.get("security_id")
        security_label = metadata.get("security_label")
        
        row = {
            "security_id": security_id,
            "security_label": security_label,
        }
        row.update({key: response.get(value) for key, value in self.field_mapping.items()})
        
        holding_summary = response.get("holdingSummary", {})
        for key, value in self.holding_summary_mapping.items():
            if isinstance(value, list):
                row[key] = next((holding_summary.get(v) for v in value if holding_summary.get(v) is not None), None)
            else:
                row[key] = holding_summary.get(value)

        df = pd.DataFrame([row]).copy() 
        
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.columns]
        return df