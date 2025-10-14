import asyncio

from morningsun.extractors.news import *
from morningsun.core.interchange import DataFrameInterchange

def get_stories_canada() -> DataFrameInterchange:
    """Get latest stories from Morningstar Canada."""
    extractor = StoriesCanadaExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_bond() -> DataFrameInterchange:
    """Get Morningstar Canada bond news."""
    extractor = NewsCanadaBondExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_etf() -> DataFrameInterchange:
    """Get Morningstar Canada ETF news."""
    extractor = NewsCanadaEtfExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_fund() -> DataFrameInterchange:
    """Get Morningstar Canada fund news."""
    extractor = NewsCanadaFundExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_stock() -> DataFrameInterchange:
    """Get Morningstar Canada stock news."""
    extractor = NewsCanadaStockExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_market() -> DataFrameInterchange:
    """Get Morningstar Canada market news."""
    extractor = NewsCanadaMarketExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_suistainable() -> DataFrameInterchange:
    """Get Morningstar Canada sustainable investing news."""
    extractor = NewsCanadaSuistainableExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_personal_finance() -> DataFrameInterchange:
    """Get Morningstar Canada personal finance news."""
    extractor = NewsCanadaPersonalFinanceExtractor().run()
    return asyncio.run(extractor)


def get_news_canada_economy() -> DataFrameInterchange:
    """Get Morningstar Canada economy news."""
    extractor = NewsCanadaEconomyExtractor().run()
    return asyncio.run(extractor)


def get_news_us_alternative_investments() -> DataFrameInterchange:
    """Get Morningstar US alternative investments news."""
    extractor = NewsUsAlternativeInvestmentsExtractor().run()
    return asyncio.run(extractor)


def get_news_us_financial_advisors() -> DataFrameInterchange:
    """Get Morningstar US financial advisors news."""
    extractor = NewsUsFinancialAdvisorsExtractor().run()
    return asyncio.run(extractor)


def get_news_us_retirements() -> DataFrameInterchange:
    """Get Morningstar US retirements news."""
    extractor = NewsUsRetirementsExtractor().run()
    return asyncio.run(extractor)


def get_news_us_portfolios() -> DataFrameInterchange:
    """Get Morningstar US portfolios news."""
    extractor = NewsUsPortfoliosExtractor().run()
    return asyncio.run(extractor)


def get_news_us_economy() -> DataFrameInterchange:
    """Get Morningstar US economy news."""
    extractor = NewsUsEconomyExtractor().run()
    return asyncio.run(extractor)


def get_news_us_sustainable_investing() -> DataFrameInterchange:
    """Get Morningstar US sustainable investing news."""
    extractor = NewsUsSustainableInvestingExtractor().run()
    return asyncio.run(extractor)


def get_news_us_personal_finance() -> DataFrameInterchange:
    """Get Morningstar US personal finance news."""
    extractor = NewsUsPersonalFinanceExtractor().run()
    return asyncio.run(extractor)