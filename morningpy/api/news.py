import asyncio
from typing import Literal

from morningpy.extractor.news import *
from morningpy.core.interchange import DataFrameInterchange
from typing import Literal

def get_headline_news(
    edition: Literal[
        "Asia",
        "Benelux",
        "Canada English",
        "Canada French",
        "Central Europe",
        "France",
        "Germany",
        "Italy",
        "Japan",
        "Nordics",
        "Spain",
        "Sweden",
        "United Kingdom",
    ],
    market: Literal[
        "All Europe",
        "Asia",
        "Austria",
        "Belgium",
        "Canada",
        "Denmark",
        "Finland",
        "France",
        "Germany",
        "Hong Kong",
        "Ireland",
        "Italy",
        "Luxembourg",
        "Malaysia",
        "Netherlands",
        "Norway",
        "Nordics",
        "Portugal",
        "Singapore",
        "Spain",
        "Sweden",
        "Switzerland",
        "Taiwan",
        "Thailand",
        "United Kingdom",
        "United States",
    ],
    news: Literal[
        "economy",
        "personal-finance",
        "sustainable-investing",
        "bonds",
        "etfs",
        "funds",
        "stocks",
        "markets",
    ],
) -> DataFrameInterchange:
    """
    Retrieve Morningstar headline news for a given edition, market, and category.

    Parameters
    ----------
    edition : Literal
        Geographic edition to query (e.g., "France", "Asia").
    market : Literal
        Market from which the news will be retrieved (e.g., "Germany", "United States").
    news : Literal
        News category such as "economy", "stocks", or "funds".

    Returns
    -------
    DataFrameInterchange
        A dataframe-like object containing the retrieved headline news.

    Notes
    -----
    This function internally executes an async extractor. If an event loop
    is already running (e.g., in a Jupyter notebook), the coroutine is
    awaited directly; otherwise, ``asyncio.run`` is used.
    """
    
    extractor = HeadlineNewsExtractor(edition=edition,market=market, news=news)
    return asyncio.run(extractor.run())
