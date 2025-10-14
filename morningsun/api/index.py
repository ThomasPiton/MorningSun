import asyncio
from typing import Union, List

from morningsun.core.interchange import DataFrameInterchange
from morningsun.extractors.index import *


def get_index_performance_table(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index performance table."""
    extractor = IndexPerformanceTableExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_return_data(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index return data."""
    extractor = IndexReturnDataExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_investments(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index investments."""
    extractor = IndexInvestmentsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_investments_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index investments quote."""
    extractor = IndexInvestmentsQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_disclosure_flag(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index disclosure flag."""
    extractor = IndexDisclosureFlagExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_risk_return_scatter_plot(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index risk-return scatter plot."""
    extractor = IndexRiskReturnScatterPlotExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_risk_volatility(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index risk volatility."""
    extractor = IndexRiskVolatilityExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_security_data(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index security metadata."""
    extractor = IndexSecurityDataExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_quote(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index quote."""
    extractor = IndexQuoteExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_operation(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index operation data."""
    extractor = IndexOperationExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_description(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index description."""
    extractor = IndexDescriptionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_performance(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index performance."""
    extractor = IndexPerformanceExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_asset(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index asset allocation."""
    extractor = IndexAssetExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_ownership_zone(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index ownership zone."""
    extractor = IndexOwnershipZoneExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_market_cap(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index market cap data."""
    extractor = IndexMarketCapExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_stock_style(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index stock style."""
    extractor = IndexStockStyleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_financial_metrics(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index financial metrics."""
    extractor = IndexFinancialMetricsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_factor_profile(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index factor profile."""
    extractor = IndexFactorProfileExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_sector(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index sector allocation."""
    extractor = IndexSectorExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_fixed_income_style(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index fixed income style."""
    extractor = IndexFixedIncomeStyleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_credit_quality(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index credit quality."""
    extractor = IndexCreditQualityExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_coupon_range(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index coupon range."""
    extractor = IndexCouponRangeExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_fixed_income_style_box_history(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index fixed income style box history."""
    extractor = IndexFixedIncomeStyleBoxHistoryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_maturity_schedule(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index maturity schedule."""
    extractor = IndexMaturityScheduleExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_trailing_return(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index trailing return."""
    extractor = IndexTrailingReturnExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_index_sustainability_country(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get index sustainability by country."""
    extractor = IndexSustainabilityCountryExtractor(id_sec).run()
    return asyncio.run(extractor)