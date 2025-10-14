import asyncio
from typing import List

from morningsun.extractors.market import *
from morningsun.core.interchange import DataFrameInterchange

def get_market_us_calendar_info(date: str | List[str], info_type: str) -> DataFrameInterchange:
    extractor = MarketCalendarUsInfoExtractor(date=date, info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_info(info_type:str | List[str]) -> DataFrameInterchange:
    """Get Morningstar global market overview."""
    extractor = MarketExtractor(info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_indexes(index_type:str) -> DataFrameInterchange:
    """Get Morningstar market indexes data."""
    extractor = MarketIndexesExtractor(index_type=index_type)
    return asyncio.run(extractor.run())

def get_market_fair_value(value_type:str) -> DataFrameInterchange:
    """Get Morningstar market fair value data."""
    extractor = MarketFairValueExtractor(value_type=value_type)
    return asyncio.run(extractor.run())

def get_market_movers(info_type:str) -> DataFrameInterchange:
    """Get Morningstar top market movers data."""
    extractor = MarketMoversExtractor(info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_commodities() -> DataFrameInterchange:
    """Get Morningstar market commodities data."""
    extractor = MarketCommoditiesExtractor()
    return asyncio.run(extractor.run())

def get_market_currencies() -> DataFrameInterchange:
    """Get Morningstar market currencies data."""
    extractor = MarketCurrenciesExtractor()
    return asyncio.run(extractor.run())