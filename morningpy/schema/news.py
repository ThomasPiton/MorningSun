from dataclasses import dataclass
from typing import Optional

from morningpy.core.dataframe_schema import DataFrameSchema

@dataclass
class HeadlineNewsSchema(DataFrameSchema):
    id: Optional[str] = None
    instrumentID: Optional[str] = None
    label: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    bidPriceDecimals: Optional[int] = None