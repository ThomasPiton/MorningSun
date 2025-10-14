from typing import Union, List
import pandas as pd

from morningsun.extractors.market import *


def get_market_us_calendar_info(date: str | list[str], info_type: str) -> pd.DataFrame:
    extractor = MarketCalendarUsInfoExtractor(date=date, info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_info(info_type:str | list[str]) -> pd.DataFrame:
    """Get Morningstar global market overview."""
    extractor = MarketExtractor(info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_indexes(index_type:str) -> pd.DataFrame:
    """Get Morningstar market indexes data."""
    extractor = MarketIndexesExtractor(index_type=index_type)
    return asyncio.run(extractor.run())

def get_market_fair_value(value_type:str) -> pd.DataFrame:
    """Get Morningstar market fair value data."""
    extractor = MarketFairValueExtractor(value_type=value_type)
    return asyncio.run(extractor.run())

def get_market_movers(info_type:str) -> pd.DataFrame:
    """Get Morningstar top market movers data."""
    extractor = MarketMoversExtractor(info_type=info_type)
    return asyncio.run(extractor.run())

def get_market_commodities() -> pd.DataFrame:
    """Get Morningstar market commodities data."""
    extractor = MarketCommoditiesExtractor()
    return asyncio.run(extractor.run())

def get_market_currencies() -> pd.DataFrame:
    """Get Morningstar market currencies data."""
    extractor = MarketCurrenciesExtractor()
    return asyncio.run(extractor.run())