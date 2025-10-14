import asyncio
from typing import Union, List

import pandas as pd

def get_timeseries_stock_realtime(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get Morningstar stock real-time data."""
    return TimeseriesStockRealtimeExtractor(id_sec).run()

def get_timeseries_chartservice_v2() -> pd.DataFrame:
    """Get Morningstar chart service v2 timeseries data."""
    return TimeseriesChartserviceV2Extractor().run()

def get_timeseries_realtime_v2() -> pd.DataFrame:
    """Get Morningstar real-time timeseries from API v2."""
    return TimeseriesRealtimeV2Extractor().run()

def get_timeseries_realtime_quotes() -> pd.DataFrame:
    """Get Morningstar real-time quotes."""
    return TimeseriesRealtimeQuotesExtractor().run()

def get_timeseries_chartservice_v3() -> pd.DataFrame:
    """Get Morningstar chart service v3 timeseries data."""
    return TimeseriesChartserviceV3Extractor().run()

def get_timeseries_realtime_quotes_ca() -> pd.DataFrame:
    """Get Morningstar Canada real-time quotes."""
    return TimeseriesRealtimeQuotesCAExtractor().run()

def get_timeseries_realtime_movers_ca() -> pd.DataFrame:
    """Get Morningstar Canada real-time movers."""
    return TimeseriesRealtimeMoversCAExtractor().run()

def get_timeseries_realtime_timeseries_ca() -> pd.DataFrame:
    """Get Morningstar Canada real-time timeseries."""
    return TimeseriesRealtimeTimeseriesCAExtractor().run()
