
from base import construct_url

def get_intraday_timeseries(ticker,exchange):
    endpoint = f"stocks/{exchange}/{ticker}/quote"
    return construct_url(endpoint)

def get_last_news(ticker, exchange):
    endpoint = f"stocks/{exchange}/{ticker}/news"
    return construct_url(endpoint)

def get_historical_timeseries(ticker):
    endpoint = f"timeseries/historical/{ticker}"
    return construct_url(endpoint)