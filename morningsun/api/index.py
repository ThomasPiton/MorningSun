from typing import Union, List
import pandas as pd

def get_index_performance_table(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index performance table."""
    return IndexPerformanceTableExtractor(id_sec).run()

def get_index_return_data(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index return data."""
    return IndexReturnDataExtractor(id_sec).run()

def get_index_investments(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index investments."""
    return IndexInvestmentsExtractor(id_sec).run()

def get_index_investments_quote(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index investments quote."""
    return IndexInvestmentsQuoteExtractor(id_sec).run()

def get_index_disclosure_flag(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index disclosure flag."""
    return IndexDisclosureFlagExtractor(id_sec).run()

def get_index_risk_return_scatter_plot(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index risk-return scatter plot."""
    return IndexRiskReturnScatterPlotExtractor(id_sec).run()

def get_index_risk_volatility(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index risk volatility."""
    return IndexRiskVolatilityExtractor(id_sec).run()

def get_index_security_data(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index security metadata."""
    return IndexSecurityDataExtractor(id_sec).run()

def get_index_quote(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index quote."""
    return IndexQuoteExtractor(id_sec).run()

def get_index_operation(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index operation data."""
    return IndexOperationExtractor(id_sec).run()

def get_index_description(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index description."""
    return IndexDescriptionExtractor(id_sec).run()

def get_index_performance(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index performance."""
    return IndexPerformanceExtractor(id_sec).run()

def get_index_asset(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index asset allocation."""
    return IndexAssetExtractor(id_sec).run()

def get_index_ownership_zone(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index ownership zone."""
    return IndexOwnershipZoneExtractor(id_sec).run()

def get_index_market_cap(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index market cap data."""
    return IndexMarketCapExtractor(id_sec).run()

def get_index_stock_style(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index stock style."""
    return IndexStockStyleExtractor(id_sec).run()

def get_index_financial_metrics(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index financial metrics."""
    return IndexFinancialMetricsExtractor(id_sec).run()

def get_index_factor_profile(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index factor profile."""
    return IndexFactorProfileExtractor(id_sec).run()

def get_index_sector(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index sector allocation."""
    return IndexSectorExtractor(id_sec).run()

def get_index_fixed_income_style(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index fixed income style."""
    return IndexFixedIncomeStyleExtractor(id_sec).run()

def get_index_credit_quality(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index credit quality."""
    return IndexCreditQualityExtractor(id_sec).run()

def get_index_coupon_range(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index coupon range."""
    return IndexCouponRangeExtractor(id_sec).run()

def get_index_fixed_income_style_box_history(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index fixed income style box history."""
    return IndexFixedIncomeStyleBoxHistoryExtractor(id_sec).run()

def get_index_maturity_schedule(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index maturity schedule."""
    return IndexMaturityScheduleExtractor(id_sec).run()

def get_index_trailing_return(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index trailing return."""
    return IndexTrailingReturnExtractor(id_sec).run()

def get_index_sustainability_country(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get index sustainability by country."""
    return IndexSustainabilityCountryExtractor(id_sec).run()
