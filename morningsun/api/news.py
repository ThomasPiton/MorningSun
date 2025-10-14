from typing import Union, List
import pandas as pd

def get_stories_canada() -> pd.DataFrame:
    """Get latest stories from Morningstar Canada."""
    return StoriesCanadaExtractor().run()

def get_news_canada_bond() -> pd.DataFrame:
    """Get Morningstar Canada bond news."""
    return NewsCanadaBondExtractor().run()

def get_news_canada_etf() -> pd.DataFrame:
    """Get Morningstar Canada ETF news."""
    return NewsCanadaEtfExtractor().run()

def get_news_canada_fund() -> pd.DataFrame:
    """Get Morningstar Canada fund news."""
    return NewsCanadaFundExtractor().run()

def get_news_canada_stock() -> pd.DataFrame:
    """Get Morningstar Canada stock news."""
    return NewsCanadaStockExtractor().run()

def get_news_canada_market() -> pd.DataFrame:
    """Get Morningstar Canada market news."""
    return NewsCanadaMarketExtractor().run()

def get_news_canada_suistainable() -> pd.DataFrame:
    """Get Morningstar Canada sustainable investing news."""
    return NewsCanadaSuistainableExtractor().run()

def get_news_canada_personal_finance() -> pd.DataFrame:
    """Get Morningstar Canada personal finance news."""
    return NewsCanadaPersonalFinanceExtractor().run()

def get_news_canada_economy() -> pd.DataFrame:
    """Get Morningstar Canada economy news."""
    return NewsCanadaEconomyExtractor().run()

def get_news_us_alternative_investments() -> pd.DataFrame:
    """Get Morningstar US alternative investments news."""
    return NewsUsAlternativeInvestmentsExtractor().run()

def get_news_us_financial_advisors() -> pd.DataFrame:
    """Get Morningstar US financial advisors news."""
    return NewsUsFinancialAdvisorsExtractor().run()

def get_news_us_retirements() -> pd.DataFrame:
    """Get Morningstar US retirements news."""
    return NewsUsRetirementsExtractor().run()

def get_news_us_portfolios() -> pd.DataFrame:
    """Get Morningstar US portfolios news."""
    return NewsUsPortfoliosExtractor().run()

def get_news_us_economy() -> pd.DataFrame:
    """Get Morningstar US economy news."""
    return NewsUsEconomyExtractor().run()

def get_news_us_sustainable_investing() -> pd.DataFrame:
    """Get Morningstar US sustainable investing news."""
    return NewsUsSustainableInvestingExtractor().run()

def get_news_us_personal_finance() -> pd.DataFrame:
    """Get Morningstar US personal finance news."""
    return NewsUsPersonalFinanceExtractor().run()
