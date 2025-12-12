import asyncio
from typing import List, Union, Literal

from morningpy.extractor.market import (
    MarketCalendarUsInfoExtractor,
    MarketIndexesExtractor,
    MarketFairValueExtractor,
    MarketMoversExtractor,
    MarketCommoditiesExtractor,
    MarketCurrenciesExtractor,
)
from morningpy.core.interchange import DataFrameInterchange


def get_market_us_calendar_info(
    date: Union[str, List[str]],
    info_type: Literal["earnings", "economic-releases", "ipos", "splits"] = None
) -> DataFrameInterchange:
    """
    Retrieve U.S. market calendar information for one or multiple dates.

    Parameters
    ----------
    date : str or list of str
        Date(s) in ISO format.
    info_type : {"earnings", "economic-releases", "ipos", "splits"}, optional
        Specific type of calendar information to retrieve.

    Returns
    -------
    DataFrameInterchange
        Structured market calendar information.
    """
    extractor = MarketCalendarUsInfoExtractor(date=date, info_type=info_type)
    return asyncio.run(extractor.run())


def get_market_indexes(
    index_type: Union[
        Literal["americas", "asia", "europe", "private", "sector", "us"],
        List[Literal["americas", "asia", "europe", "private", "sector", "us"]]
    ]
) -> DataFrameInterchange:
    """
    Retrieve market index information.

    Parameters
    ----------
    index_type : str or list of str
        Categories of indices to retrieve.

    Returns
    -------
    DataFrameInterchange
        Market index dataset.
    """
    extractor = MarketIndexesExtractor(index_type=index_type)
    return asyncio.run(extractor.run())


def get_market_fair_value(
    value_type: Literal["undervaluated", "overvaluated"]
) -> DataFrameInterchange:
    """
    Retrieve market fair value estimates.

    Parameters
    ----------
    value_type : {"undervaluated", "overvaluated"}
        Whether to fetch undervalued or overvalued market segments.

    Returns
    -------
    DataFrameInterchange
        Fair value dataset.
    """
    extractor = MarketFairValueExtractor(value_type=value_type)
    return asyncio.run(extractor.run())


def get_market_movers(
    mover_type: Union[
        Literal["gainers", "losers", "actives"],
        List[Literal["gainers", "losers", "actives"]]
    ]
) -> DataFrameInterchange:
    """
    Retrieve top market movers.

    Parameters
    ----------
    mover_type : str or list of str
        Category of movers to retrieve: gainers, losers, or actives.

    Returns
    -------
    DataFrameInterchange
        Market movers dataset.
    """
    extractor = MarketMoversExtractor(mover_type=mover_type)
    return asyncio.run(extractor.run())


def get_market_commodities() -> DataFrameInterchange:
    """
    Retrieve commodity market data.

    Returns
    -------
    DataFrameInterchange
        Commodity prices and metrics.
    """
    extractor = MarketCommoditiesExtractor()
    return asyncio.run(extractor.run())


def get_market_currencies() -> DataFrameInterchange:
    """
    Retrieve currency market data.

    Returns
    -------
    DataFrameInterchange
        Exchange rates and FX metrics.
    """
    extractor = MarketCurrenciesExtractor()
    return asyncio.run(extractor.run())
