from dataclasses import dataclass
from typing import Optional

from morningpy.core.dataframe_schema import DataFrameSchema

@dataclass
class HeadlineNewsSchema(DataFrameSchema):
    news: Optional[str] = None
    edition: Optional[str] = None
    market: Optional[str] = None
    display_date: Optional[str] = None
    title: Optional[str] = None
    subtitle: Optional[str] = None
    tags: Optional[str] = None
    link: Optional[str] = None
    language: Optional[str] = None