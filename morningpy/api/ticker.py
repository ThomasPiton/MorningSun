import pandas as pd
from typing import Optional, Literal,Union,List

from morningpy.extractor.ticker import TickerExtractor
from morningpy.config.ticker import TickerConfig

def search_tickers(
    is_active: TickerConfig.BooleanLitarel=False,
    security_type: TickerConfig.SecurityTypeLiteral=None,
    security_id: Union[str,List[str]] = None,
    security_label: Union[str,List[str]] = None,
    ticker: Union[str,List[str]] = None,
    isin: Union[str,List[str]] = None,
    sector: TickerConfig.SectorLiteral = None,
    industry: Union[str,List[str]] = None,
    country: TickerConfig.CountryLiteral = None,
    country_id: TickerConfig.CountryIdLiteral = None,
    currency: Union[str,List[str]] = None,
    exchange_id: TickerConfig.ExchangeIdLiteral = None,
    exchange: TickerConfig.ExchangeNameLiteral = None,
    fund_id: Union[str,List[str]] = None,
    family_id: Union[str,List[str]] = None,
    portfolio_id: Union[str,List[str]] = None,
    provider_id: Union[str,List[str]] = None,
    asset_class: Union[str,List[str]] = None,
    region: Union[str,List[str]] = None,
    bond_sector: Union[str,List[str]] = None,
    credit_rating: Union[str,List[str]] = None,
    display_rank: Union[str,List[str]] = None,
    esg_index: TickerConfig.BooleanLitarel=False,
    hedged: TickerConfig.BooleanLitarel=False,
    market_development: Union[str,List[str]] = None,
    rebalance_date: Union[str,List[str]] = None,
    return_type: Union[str,List[str]] = None,
    size: Union[str,List[str]] = None,
    style: Union[str,List[str]] = None,
    strategic_beta: Union[str,List[str]] = None,
    performance_id: Union[str,List[str]] = None,
    company_id: Union[str,List[str]] = None,
    master_portfolio_id: Union[str,List[str]] = None,
    security_type_id: Union[str,List[str]] = None,
    stock_style_box: Union[str,List[str]] = None,
    dividend_distribution_frequency: Union[str,List[str]] = None,
    inception_date: Union[str,List[str]] = None,
    broad_category_group: Union[str,List[str]] = None,
    morningstar_category: Union[str,List[str]] = None,
    distribution_fund_type: Union[str,List[str]] = None,
    replication_method: Union[str,List[str]] = None,
    is_index_fund: TickerConfig.BooleanLitarel=False,
    primary_benchmark: Union[str,List[str]] = None,
    management_expense_ratio: Union[str,List[str]] = None,
    has_performance_fee: TickerConfig.BooleanLitarel=False,
    fund_star_rating: Union[str,List[str]] = None,
    morningstar_risk_rating: Optional[int] = None,
    medalist_rating: Union[str,List[str]] = None,
    sustainability_rating: Union[str,List[str]] = None,
    fund_equity_style_box: Union[str,List[str]] = None,
    fund_fixed_income_style_box: Union[str,List[str]] = None,
    fund_alternative_style_box: Union[str,List[str]] = None,
    exact_match: TickerConfig.BooleanLitarel=False
) -> pd.DataFrame:
    """test"""
    filters = {k: v for k, v in locals().items()}
    
    return TickerExtractor(**filters).search_tickers()
    


def convert(
    ticker: Optional[str] = None,
    isin: Optional[str] = None,
    performance_id: Optional[str] = None,
    convert_to: Literal["ticker", "isin", "performance_id", "security_id"] = None
) -> Optional[str]:
    return TickerExtractor(ticker=ticker, isin=isin, performance_id=performance_id).convert_to(convert_to)