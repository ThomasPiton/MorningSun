from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from morningpy.core.dataframe_schema import DataFrameSchema

@dataclass
class IntradayTimeseriesSchema(DataFrameSchema):
    security_id: Optional[str] = None
    date: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    volume: Optional[float] = None
    
@dataclass
class HistoricalTimeseriesSchema(DataFrameSchema):
    security_id: Optional[str] = None
    date: Optional[datetime] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    previous_close: Optional[float] = None
    volume: Optional[float] = None