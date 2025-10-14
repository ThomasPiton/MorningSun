import asyncio
from typing import Union, List

from morningsun.core.interchange import DataFrameInterchange
from morningsun.extractors.etf import *


def get_etf_disclosure_flag(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF disclosure flag."""
    extractor = EtfDisclosureFlagExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF quote."""
    extractor = EtfQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_price(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF price."""
    extractor = EtfPriceExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_investment_strategy(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF investment strategy."""
    extractor = EtfInvestmentStrategyExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_esg_risk(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF ESG risk data."""
    extractor = EtfEsgRiskExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_performance(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF performance."""
    extractor = EtfPerformanceExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_security_data(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF security metadata."""
    extractor = EtfSecurityDataExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_performance_table(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF performance table."""
    extractor = EtfPerformanceTableExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_price_cost_illustration(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF price cost illustration."""
    extractor = EtfPriceCostIllustrationExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_realtime_chart_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF real-time chart quote."""
    extractor = EtfRealtimeChartQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_realtime_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF real-time quote."""
    extractor = EtfRealtimeQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_trailing_return(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF trailing return."""
    extractor = EtfTrailingReturnExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_esg_product_involvement(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF ESG product involvement."""
    extractor = EtfEsgProductInvolvementExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_risk_score(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF risk score."""
    extractor = EtfRiskScoreExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_risk_return_summary(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF risk-return summary."""
    extractor = EtfRiskReturnSummaryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_risk_volatility(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF risk volatility."""
    extractor = EtfRiskVolatilityExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_risk_return_scatter_plot(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF risk-return scatter plot."""
    extractor = EtfRiskReturnScatterPlotExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_market_volatility_measure(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF market volatility measure."""
    extractor = EtfMarketVolatilityMeasureExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_holding(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF holdings."""
    extractor = EtfHoldingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_asset(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF asset allocation."""
    extractor = EtfAssetExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_market_cap(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF market cap data."""
    extractor = EtfMarketCapExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_factor_profile(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF factor profile."""
    extractor = EtfFactorProfileExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_sector(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF sector allocation."""
    extractor = EtfSectorExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_ownership_zone(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF ownership zone."""
    extractor = EtfOwnershipZoneExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_stock_style(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF stock style."""
    extractor = EtfStockStyleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_financial_metrics(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF financial metrics."""
    extractor = EtfFinancialMetricsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_weighting(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF weighting data."""
    extractor = EtfWeightingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_equity_style_box_history(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF equity style box history."""
    extractor = EtfEquityStyleBoxHistoryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_regional_sector(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF regional sector allocation."""
    extractor = EtfRegionalSectorExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_regional_sector_include_countries(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF regional sector including countries."""
    extractor = EtfRegionalSectorIncludeCountriesExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_people(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF people data."""
    extractor = EtfPeopleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_top_funds_up_down(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF top funds up/down."""
    extractor = EtfTopFundsUpDownExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_mstar_rating(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF Morningstar rating."""
    extractor = EtfMstarRatingExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_mstar_rating_desc(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF Morningstar rating (descending)."""
    extractor = EtfMstarRatingDescExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_etf_mstar_rating_asc(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get ETF Morningstar rating (ascending)."""
    extractor = EtfMstarRatingAscExtractor(id_sec).run()
    return asyncio.run(extractor)