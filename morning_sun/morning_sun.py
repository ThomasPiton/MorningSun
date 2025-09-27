from abc import ABC, abstractmethod
from typing import List,Dict
from config import URL

def get_last_quotes(
        queries:List[Dict]=None,
        ids:List[Dict]=None,
        **args
        ):
    """ 
    query = [
        {"exchange":"xnas","ticker":"nvda"},
        {"exchange":"xbue","ticker":"nvda"},
        {"exchange":"ukex","ticker":"nvda"},
        {"exchange":"xmex","ticker":"nvda"},
        {"exchange":"xlim","ticker":"nvda"},
        {"exchange":"neoe","ticker":"nvda"},
    ]
    """
    return LastQuotes(**args).get()

def get_historical_timeseries(
        tickers:List[str]=None,
        **args
    ):
    return HistoricalTimeseries(**args).get()

class BaseExtractor(ABC):

    def __init__(self) -> None:
        super().__init__()
    
    @abstractmethod
    def build_url(self):
        pass

    @abstractmethod
    def call_api(self):
        pass
    
    @abstractmethod
    def validate_response(self):
        pass

    @abstractmethod
    def process_response(self):
        pass

class LastQuotes(BaseExtractor):
    
    def __init__(self) -> None:
        super().__init__()

    def get(self):
        pass

class HistoricalTimeseries(BaseExtractor):
    
    def __init__(self) -> None:
        super().__init__()

    def get(self):
        pass

if __name__ == '__main__':
    
    last_quotes = api.get_last_quotes(["AAPL"])

