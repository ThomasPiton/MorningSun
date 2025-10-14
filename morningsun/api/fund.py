from typing import Union, List
import pandas as pd

def get_fund_best_investments(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get best investments fund list."""
    return FundBestInvestmentsExtractor(id_sec).run()

def get_fund_price_alternative_options(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund price alternative options."""
    return FundPriceAlternativeOptionsExtractor(id_sec).run()

def get_fund_price_selected_option(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund price selected option."""
    return FundPriceSelectedOptionExtractor(id_sec).run()

def get_fund_price_cost_illustration(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund price cost illustration."""
    return FundPriceCostIllustrationExtractor(id_sec).run()

def get_fund_disclosure_flag(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund disclosure flag."""
    return FundDisclosureFlagExtractor(id_sec).run()

def get_fund_price(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund price."""
    return FundPriceExtractor(id_sec).run()

def get_fund_investment_strategy(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund investment strategy."""
    return FundInvestmentStrategyExtractor(id_sec).run()

def get_fund_strategy_preview(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund strategy preview."""
    return FundStrategyPreviewExtractor(id_sec).run()

def get_fund_esg_risk(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund ESG risk data."""
    return FundEsgRiskExtractor(id_sec).run()

def get_fund_quote(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund quote."""
    return FundQuoteExtractor(id_sec).run()

def get_fund_annual_distribution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund annual distribution."""
    return FundAnnualDistributionExtractor(id_sec).run()

def get_fund_latest_distribution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund latest distribution."""
    return FundLatestDistributionExtractor(id_sec).run()

def get_fund_esg_product_involvment(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund ESG product involvement."""
    return FundEsgProductInvolvmentExtractor(id_sec).run()

def get_fund_performance_market_volatility_measure(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund performance market volatility measure."""
    return FundPerformanceMarketVolatilityMeasureExtractor(id_sec).run()

def get_fund_risk_return_summary(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund risk return summary."""
    return FundRiskReturnSummaryExtractor(id_sec).run()

def get_fund_risk_score(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund risk score."""
    return FundRiskScoreExtractor(id_sec).run()

def get_fund_risk_return_scatter_plot(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund risk return scatter plot."""
    return FundRiskReturnScatterPlotExtractor(id_sec).run()

def get_fund_risk_volatility(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund risk volatility."""
    return FundRiskVolatilityExtractor(id_sec).run()

def get_fund_holding(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund holdings."""
    return FundHoldingExtractor(id_sec).run()

def get_fund_sector(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund sector allocation."""
    return FundSectorExtractor(id_sec).run()

def get_fund_asset(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund asset data."""
    return FundAssetExtractor(id_sec).run()

def get_fund_ownership_zone(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund ownership zone."""
    return FundOwnershipZoneExtractor(id_sec).run()

def get_fund_factor_profile(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund factor profile."""
    return FundFactorProfileExtractor(id_sec).run()

def get_fund_stock_style(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund stock style."""
    return FundStockStyleExtractor(id_sec).run()

def get_fund_market_cap(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund market cap data."""
    return FundMarketCapExtractor(id_sec).run()

def get_fund_financial_metrics(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund financial metrics."""
    return FundFinancialMetricsExtractor(id_sec).run()

def get_fund_weighting(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund weighting data."""
    return FundWeightingExtractor(id_sec).run()

def get_fund_equity_style_box_history(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund equity style box history."""
    return FundEquityStyleBoxHistoryExtractor(id_sec).run()

def get_fund_regional_sector(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund regional sector allocation."""
    return FundRegionalSectorExtractor(id_sec).run()

def get_fund_regional_sector_include_countries(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund regional sector including countries."""
    return FundRegionalSectorIncludeCountriesExtractor(id_sec).run()

def get_fund_people(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund people data."""
    return FundPeopleExtractor(id_sec).run()

def get_fund_medalist_rating(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund medalist rating."""
    return FundMedalistRatingExtractor(id_sec).run()

def get_fund_medalist_rating_top_funds_up_down(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get medalist rating top funds up/down."""
    return FundMedalistRatingTopFundsUpDownExtractor(id_sec).run()

def get_fund_medalist_rating_top_funds(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get medalist rating top funds."""
    return FundMedalistRatingTopFundsExtractor(id_sec).run()

def get_fund_mstar_rating(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund parent Morningstar rating."""
    return FundMstarRatingExtractor(id_sec).run()

def get_fund_mstar_rating_fund_asc(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund Morningstar rating (ascending)."""
    return FundMstarRatingFundAscExtractor(id_sec).run()

def get_fund_mstar_rating_fund_desc(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund Morningstar rating (descending)."""
    return FundMstarRatingFundDescExtractor(id_sec).run()

def get_fund_security_data(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get fund security metadata."""
    return FundSecurityDataExtractor(id_sec).run()