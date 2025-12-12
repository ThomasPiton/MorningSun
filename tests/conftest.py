from pathlib import Path
from typing import Dict, List, Type

import pytest

BASE_DIR = Path(__file__).parent
REQUESTS_DIR = BASE_DIR / "fixtures/fake_requests"
RESPONSES_DIR = BASE_DIR / "fixtures/fake_responses"
MOCKS_DIR = BASE_DIR / "mocks"


@pytest.fixture(scope="session")
def config_classes() -> Dict[str, List[Type]]:
    """
    Import and return all config classes organized by module.
    
    Returns
    -------
    Dict[str, List[Type]]
        Dictionary with module names as keys and lists of config classes as values.
        Example: {'market': [MarketCurrenciesConfig, MarketCommoditiesConfig], ...}
    
    Scope: session (computed once per test session)
    """
    from morningpy.config.market import (
        MarketCurrenciesConfig,
        MarketCommoditiesConfig
    )
    from morningpy.config.news import NewsConfig
    from morningpy.config.security import SecurityConfig
    from morningpy.config.ticker import TickerConfig
    from morningpy.config.timeseries import TimeSeriesConfig
    
    return {
        'market': [MarketCurrenciesConfig, MarketCommoditiesConfig],
        'news': [NewsConfig],
        'security': [SecurityConfig],
        'ticker': [TickerConfig],
        'timeseries': [TimeSeriesConfig],
    }


@pytest.fixture(scope="session")
def all_config_classes(config_classes) -> List[Type]:
    """
    Returns a flat list of all config classes.
    
    Parameters
    ----------
    config_classes : dict
        Dictionary of config classes by module.
    
    Returns
    -------
    List[Type]
        Flat list of all config classes.
    
    Scope: session
    """
    return [
        config_class
        for config_list in config_classes.values()
        for config_class in config_list
    ]


@pytest.fixture(scope="session")
def url_pattern():
    """
    Regex pattern for validating URLs.
    
    Returns
    -------
    str
        Regex pattern that matches valid HTTP/HTTPS URLs.
    
    Scope: session
    """
    return r'^https?://[^\s/$.?#].[^\s]*$'


@pytest.fixture(scope="session")
def snake_case_pattern():
    """
    Regex pattern for validating snake_case strings.
    
    Returns
    -------
    str
        Regex pattern that matches valid snake_case identifiers.
    
    Scope: session
    """
    return r'^[a-z][a-z0-9_]*$'

