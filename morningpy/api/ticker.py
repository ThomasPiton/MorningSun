import pandas as pd
from typing import Optional, Literal,Union,List

from morningpy.extractor.ticker import TickerExtractor
from morningpy.config.ticker import TickerConfig

def search_tickers(
    is_active: Optional[TickerConfig.BooleanLiteral] = None,
    security_type: Optional[TickerConfig.SecurityTypeLiteral] = None,
    security_id: Union[str, List[str], None] = None,
    security_label: Union[str, List[str], None] = None,
    ticker: Union[str, List[str], None] = None,
    isin: Union[str, List[str], None] = None,
    sector: Optional[TickerConfig.SectorLiteral] = None,
    industry: Union[str, List[str], None] = None,
    country: Optional[TickerConfig.CountryLiteral] = None,
    country_id: Optional[TickerConfig.CountryIdLiteral] = None,
    currency: Union[str, List[str], None] = None,
    exchange_id: Optional[TickerConfig.ExchangeIdLiteral] = None,
    exchange: Optional[TickerConfig.ExchangeNameLiteral] = None,
    fund_id: Union[str, List[str], None] = None,
    family_id: Union[str, List[str], None] = None,
    portfolio_id: Union[str, List[str], None] = None,
    provider_id: Union[str, List[str], None] = None,
    asset_class: Union[str, List[str], None] = None,
    region: Union[str, List[str], None] = None,
    bond_sector: Union[str, List[str], None] = None,
    credit_rating: Union[str, List[str], None] = None,
    display_rank: Union[str, List[str], None] = None,
    esg_index: Optional[TickerConfig.BooleanLiteral] = None,
    hedged: Optional[TickerConfig.BooleanLiteral] = None,
    market_development: Union[str, List[str], None] = None,
    rebalance_date: Union[str, List[str], None] = None,
    return_type: Union[str, List[str], None] = None,
    size: Union[str, List[str], None] = None,
    style: Union[str, List[str], None] = None,
    strategic_beta: Union[str, List[str], None] = None,
    performance_id: Union[str, List[str], None] = None,
    company_id: Union[str, List[str], None] = None,
    master_portfolio_id: Union[str, List[str], None] = None,
    security_type_id: Union[str, List[str], None] = None,
    stock_style_box: Union[str, List[str], None] = None,
    dividend_distribution_frequency: Union[str, List[str], None] = None,
    inception_date: Union[str, List[str], None] = None,
    broad_category_group: Union[str, List[str], None] = None,
    morningstar_category: Union[str, List[str], None] = None,
    distribution_fund_type: Union[str, List[str], None] = None,
    replication_method: Union[str, List[str], None] = None,
    is_index_fund: Optional[TickerConfig.BooleanLiteral] = None,
    primary_benchmark: Union[str, List[str], None] = None,
    management_expense_ratio: Union[str, List[str], None] = None,
    has_performance_fee: Optional[TickerConfig.BooleanLiteral] = None,
    fund_star_rating: Union[str, List[str], None] = None,
    morningstar_risk_rating: Optional[int] = None,
    medalist_rating: Union[str, List[str], None] = None,
    sustainability_rating: Union[str, List[str], None] = None,
    fund_equity_style_box: Union[str, List[str], None] = None,
    fund_fixed_income_style_box: Union[str, List[str], None] = None,
    fund_alternative_style_box: Union[str, List[str], None] = None,
    exact_match: bool = False
) -> pd.DataFrame:
    """
    Search for financial securities using multiple filter criteria.

    Performs a comprehensive search across the Morningstar ticker database with support
    for filtering by security characteristics, classifications, ratings, and identifiers.
    All text-based filters support both single values and lists for OR logic matching.
    Multiple different filters are combined with AND logic.

    Parameters
    ----------
    is_active : bool, optional
        Filter by active trading status. True returns only actively traded securities.
    security_type : str, optional
        Type of security. Common values: "stock", "fund", "etf", "bond".
    security_id : str or list of str, optional
        Morningstar security identifier(s). Unique ID for each security.
    security_label : str or list of str, optional
        Security name or label. Supports partial matching unless exact_match=True.
    ticker : str or list of str, optional
        Trading ticker symbol(s). E.g., "AAPL", "MSFT".
    isin : str or list of str, optional
        International Securities Identification Number(s).
    sector : str, optional
        Economic sector classification. E.g., "Technology", "Healthcare", "Financial Services".
    industry : str or list of str, optional
        Industry classification within sector. More granular than sector.
    country : str, optional
        Country code or name where security is domiciled. E.g., "US", "GB", "DE".
    country_id : str, optional
        Morningstar country identifier code.
    currency : str or list of str, optional
        Trading currency code(s). E.g., "USD", "EUR", "GBP".
    exchange_id : str, optional
        Exchange identifier code where security is listed.
    exchange : str, optional
        Exchange name where security is listed. E.g., "NASDAQ", "NYSE", "LSE".
    fund_id : str or list of str, optional
        Morningstar fund identifier(s). For mutual funds and ETFs.
    family_id : str or list of str, optional
        Fund family/provider identifier(s).
    portfolio_id : str or list of str, optional
        Portfolio identifier(s) for the security.
    provider_id : str or list of str, optional
        Data provider identifier(s).
    asset_class : str or list of str, optional
        Primary asset class. E.g., "Equity", "Fixed Income", "Commodity".
    region : str or list of str, optional
        Geographic region(s). E.g., "North America", "Europe", "Asia-Pacific".
    bond_sector : str or list of str, optional
        Bond-specific sector classification(s).
    credit_rating : str or list of str, optional
        Credit rating(s). E.g., "AAA", "AA", "BBB".
    display_rank : str or list of str, optional
        Display ranking value(s) for sorting/filtering.
    esg_index : bool, optional
        Filter by ESG (Environmental, Social, Governance) index inclusion status.
    hedged : bool, optional
        Filter by currency hedged status. True for hedged securities.
    market_development : str or list of str, optional
        Market development classification(s). E.g., "Developed", "Emerging", "Frontier".
    rebalance_date : str or list of str, optional
        Fund rebalancing date(s).
    return_type : str or list of str, optional
        Return calculation type(s). E.g., "Total Return", "Price Return".
    size : str or list of str, optional
        Market capitalization size classification(s). E.g., "Large", "Mid", "Small".
    style : str or list of str, optional
        Investment style classification(s). E.g., "Growth", "Value", "Blend".
    strategic_beta : str or list of str, optional
        Strategic beta strategy classification(s).
    performance_id : str or list of str, optional
        Performance tracking identifier(s).
    company_id : str or list of str, optional
        Company identifier(s) for the issuing company.
    master_portfolio_id : str or list of str, optional
        Master portfolio identifier(s) for fund structures.
    security_type_id : str or list of str, optional
        Numeric security type identifier(s).
    stock_style_box : str or list of str, optional
        Morningstar equity style box position(s). 9-box grid combining size and style.
    dividend_distribution_frequency : str or list of str, optional
        How often dividends are distributed. E.g., "Quarterly", "Monthly", "Annual".
    inception_date : str or list of str, optional
        Fund inception/launch date(s).
    broad_category_group : str or list of str, optional
        High-level category grouping(s).
    morningstar_category : str or list of str, optional
        Morningstar's detailed category classification(s).
    distribution_fund_type : str or list of str, optional
        Fund distribution type(s). E.g., "Accumulation", "Distribution".
    replication_method : str or list of str, optional
        ETF replication method(s). E.g., "Physical", "Synthetic".
    is_index_fund : bool, optional
        Filter by index fund status. True returns only index-tracking funds.
    primary_benchmark : str or list of str, optional
        Primary benchmark index(es) tracked by the fund.
    management_expense_ratio : str or list of str, optional
        Annual management expense ratio value(s) or range.
    has_performance_fee : bool, optional
        Filter by presence of performance-based fees.
    fund_star_rating : str or list of str, optional
        Morningstar star rating(s). Typically 1-5 stars.
    morningstar_risk_rating : int, optional
        Morningstar risk rating value. Typically 1-5 scale.
    medalist_rating : str or list of str, optional
        Morningstar Medalist Rating(s). E.g., "Gold", "Silver", "Bronze", "Neutral", "Negative".
    sustainability_rating : str or list of str, optional
        ESG sustainability rating(s). Typically 1-5 globes.
    fund_equity_style_box : str or list of str, optional
        Fund's equity style box position for equity funds.
    fund_fixed_income_style_box : str or list of str, optional
        Fund's fixed income style box position for bond funds.
    fund_alternative_style_box : str or list of str, optional
        Fund's alternative style box position for alternative investment funds.
    exact_match : bool, default False
        Matching mode for string-based filters:
        - False: Case-insensitive partial matching (contains)
        - True: Exact matching (case-sensitive equality)

    Returns
    -------
    pd.DataFrame
        DataFrame containing all securities matching the specified filter criteria.
        Each row represents one security with all available attributes as columns.
        Returns empty DataFrame if no matches found.

    Notes
    -----
    - All filters use AND logic when combined (all conditions must be met)
    - List values within a single parameter use OR logic (match any value in list)
    - String filters support partial matching by default for flexibility
    - Use exact_match=True for precise string matching requirements
    - None/null parameter values are ignored (no filtering applied)
    - Empty lists are treated as no filter (all values match)

    Examples
    --------
    Basic searches:
    
    >>> # Find all US stocks
    >>> us_stocks = search_tickers(security_type="stock", country="US")
    
    >>> # Find specific tickers
    >>> tech_giants = search_tickers(ticker=["AAPL", "MSFT", "GOOGL", "AMZN"])
    
    >>> # Find funds with 5-star rating
    >>> top_funds = search_tickers(
    ...     security_type="fund",
    ...     fund_star_rating="5"
    ... )

    Advanced searches:
    
    >>> # Find large-cap growth stocks in technology sector
    >>> growth_tech = search_tickers(
    ...     security_type="stock",
    ...     sector="Technology",
    ...     size="Large",
    ...     style="Growth",
    ...     is_active=True
    ... )
    
    >>> # Find European ETFs with ESG focus
    >>> esg_etfs = search_tickers(
    ...     security_type="etf",
    ...     region="Europe",
    ...     esg_index=True,
    ...     sustainability_rating=["4", "5"]
    ... )
    
    >>> # Find high-quality bond funds with low fees
    >>> quality_bonds = search_tickers(
    ...     security_type="fund",
    ...     asset_class="Fixed Income",
    ...     credit_rating=["AAA", "AA"],
    ...     medalist_rating=["Gold", "Silver"],
    ...     is_index_fund=False
    ... )
    
    >>> # Find funds by partial name match
    >>> vanguard_funds = search_tickers(
    ...     security_label="Vanguard",
    ...     exact_match=False
    ... )
    
    >>> # Exact ISIN lookup
    >>> specific_security = search_tickers(
    ...     isin="US0378331005",
    ...     exact_match=True
    ... )

    See Also
    --------
    TickerExtractor.search_tickers : Lower-level search method with dict-based filters
    TickerConfig : Configuration constants including valid Literal values for filters
    """
    filters = {
        "is_active": is_active,
        "security_type": security_type,
        "security_id": security_id,
        "security_label": security_label,
        "ticker": ticker,
        "isin": isin,
        "sector": sector,
        "industry": industry,
        "country": country,
        "country_id": country_id,
        "currency": currency,
        "exchange_id": exchange_id,
        "exchange": exchange,
        "fund_id": fund_id,
        "family_id": family_id,
        "portfolio_id": portfolio_id,
        "provider_id": provider_id,
        "asset_class": asset_class,
        "region": region,
        "bond_sector": bond_sector,
        "credit_rating": credit_rating,
        "display_rank": display_rank,
        "esg_index": esg_index,
        "hedged": hedged,
        "market_development": market_development,
        "rebalance_date": rebalance_date,
        "return_type": return_type,
        "size": size,
        "style": style,
        "strategic_beta": strategic_beta,
        "performance_id": performance_id,
        "company_id": company_id,
        "master_portfolio_id": master_portfolio_id,
        "security_type_id": security_type_id,
        "stock_style_box": stock_style_box,
        "dividend_distribution_frequency": dividend_distribution_frequency,
        "inception_date": inception_date,
        "broad_category_group": broad_category_group,
        "morningstar_category": morningstar_category,
        "distribution_fund_type": distribution_fund_type,
        "replication_method": replication_method,
        "is_index_fund": is_index_fund,
        "primary_benchmark": primary_benchmark,
        "management_expense_ratio": management_expense_ratio,
        "has_performance_fee": has_performance_fee,
        "fund_star_rating": fund_star_rating,
        "morningstar_risk_rating": morningstar_risk_rating,
        "medalist_rating": medalist_rating,
        "sustainability_rating": sustainability_rating,
        "fund_equity_style_box": fund_equity_style_box,
        "fund_fixed_income_style_box": fund_fixed_income_style_box,
        "fund_alternative_style_box": fund_alternative_style_box,
    }
    
    extractor = TickerExtractor()
    return extractor.search_tickers(filters=filters, exact_match=exact_match)

def convert(
    ticker: Optional[str] = None,
    isin: Optional[str] = None,
    performance_id: Optional[str] = None,
    security_id: Optional[str] = None,
    convert_to: TickerConfig.IdLiteral = None
) -> Optional[str]:
    """
    Convert between ticker, ISIN, performance_id, or security_id.

    Parameters
    ----------
    ticker : str, optional
        Ticker symbol to convert from.
    isin : str, optional
        ISIN code to convert from.
    performance_id : str, optional
        Performance ID to convert from.
    security_id : str, optional
        Security ID to convert from.
    convert_to : {"ticker", "isin", "performance_id", "security_id"}
        The target field to convert to.

    Returns
    -------
    str or None
        The corresponding value in the target column, or None if not found.

    Raises
    ------
    ValueError
        If no source identifier is provided or convert_to is not specified.

    Examples
    --------
    >>> # Convert ticker to ISIN
    >>> convert(ticker="AAPL", convert_to="isin")
    'US0378331005'
    >>> 
    >>> # Convert ISIN to security ID
    >>> convert(isin="US0378331005", convert_to="security_id")
    '0P000000GY'
    >>> 
    >>> # Convert performance ID to ticker
    >>> convert(performance_id="0P000000GY", convert_to="ticker")
    'AAPL'
    """
    extractor = TickerExtractor()
    return extractor.convert_to(
        ticker=ticker, 
        isin=isin, 
        performance_id=performance_id,
        security_id=security_id, 
        convert_to=convert_to
    )


def batch_convert(
    identifiers: List[str],
    from_field: TickerConfig.IdLiteral,
    to_field: TickerConfig.IdLiteral
) -> pd.DataFrame:
    """
    Convert multiple identifiers in batch.

    Parameters
    ----------
    identifiers : list of str
        List of identifiers to convert.
    from_field : {"ticker", "isin", "performance_id", "security_id"}
        The source field type.
    to_field : {"ticker", "isin", "performance_id", "security_id"}
        The target field type.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns for source identifier, target identifier,
        and security label.

    Examples
    --------
    >>> # Convert multiple tickers to ISINs
    >>> batch_convert(["AAPL", "MSFT", "GOOGL"], from_field="ticker", to_field="isin")
    >>> 
    >>> # Convert ISINs to security IDs
    >>> batch_convert(
    ...     ["US0378331005", "US5949181045"], 
    ...     from_field="isin", 
    ...     to_field="security_id"
    ... )
    """
    extractor = TickerExtractor()
    return extractor.batch_convert(identifiers, from_field, to_field)