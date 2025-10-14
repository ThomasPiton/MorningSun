import asyncio
from typing import Union, List

from morningsun.core.interchange import DataFrameInterchange
from morningsun.extractors.fund import *


def get_fund_best_investments(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get best investments fund list."""
    extractor = FundBestInvestmentsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_price_alternative_options(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund price alternative options."""
    extractor = FundPriceAlternativeOptionsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_price_selected_option(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund price selected option."""
    extractor = FundPriceSelectedOptionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_price_cost_illustration(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund price cost illustration."""
    extractor = FundPriceCostIllustrationExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_disclosure_flag(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund disclosure flag."""
    extractor = FundDisclosureFlagExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_price(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund price."""
    extractor = FundPriceExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_investment_strategy(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund investment strategy."""
    extractor = FundInvestmentStrategyExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_strategy_preview(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund strategy preview."""
    extractor = FundStrategyPreviewExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_esg_risk(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund ESG risk data."""
    extractor = FundEsgRiskExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund quote."""
    extractor = FundQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_annual_distribution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund annual distribution."""
    extractor = FundAnnualDistributionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_latest_distribution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund latest distribution."""
    extractor = FundLatestDistributionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_esg_product_involvment(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund ESG product involvement."""
    extractor = FundEsgProductInvolvmentExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_performance_market_volatility_measure(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund performance market volatility measure."""
    extractor = FundPerformanceMarketVolatilityMeasureExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_risk_return_summary(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund risk return summary."""
    extractor = FundRiskReturnSummaryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_risk_score(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund risk score."""
    extractor = FundRiskScoreExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_risk_return_scatter_plot(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund risk return scatter plot."""
    extractor = FundRiskReturnScatterPlotExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_risk_volatility(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund risk volatility."""
    extractor = FundRiskVolatilityExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_holding(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund holdings."""
    extractor = FundHoldingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_sector(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund sector allocation."""
    extractor = FundSectorExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_asset(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund asset data."""
    extractor = FundAssetExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_ownership_zone(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund ownership zone."""
    extractor = FundOwnershipZoneExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_factor_profile(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund factor profile."""
    extractor = FundFactorProfileExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_stock_style(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund stock style."""
    extractor = FundStockStyleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_market_cap(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund market cap data."""
    extractor = FundMarketCapExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_financial_metrics(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund financial metrics."""
    extractor = FundFinancialMetricsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_weighting(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund weighting data."""
    extractor = FundWeightingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_equity_style_box_history(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund equity style box history."""
    extractor = FundEquityStyleBoxHistoryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_regional_sector(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund regional sector allocation."""
    extractor = FundRegionalSectorExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_regional_sector_include_countries(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund regional sector including countries."""
    extractor = FundRegionalSectorIncludeCountriesExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_people(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund people data."""
    extractor = FundPeopleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_medalist_rating(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund medalist rating."""
    extractor = FundMedalistRatingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_medalist_rating_top_funds_up_down(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get medalist rating top funds up/down."""
    extractor = FundMedalistRatingTopFundsUpDownExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_medalist_rating_top_funds(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get medalist rating top funds."""
    extractor = FundMedalistRatingTopFundsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_mstar_rating(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund parent Morningstar rating."""
    extractor = FundMstarRatingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_mstar_rating_fund_asc(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund Morningstar rating (ascending)."""
    extractor = FundMstarRatingFundAscExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_mstar_rating_fund_desc(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund Morningstar rating (descending)."""
    extractor = FundMstarRatingFundDescExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_fund_security_data(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get fund security metadata."""
    extractor = FundSecurityDataExtractor(id_sec).run()
    return asyncio.run(extractor)