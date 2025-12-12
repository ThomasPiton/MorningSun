from dataclasses import dataclass
from typing import Optional

from morningpy.core.dataframe_schema import DataFrameSchema
    
@dataclass
class MarketCalendarUsInfoSchema(DataFrameSchema):
    name: Optional[str] = None
    
@dataclass
class MarketFairValueSchema(DataFrameSchema):
    category: Optional[str] = None
    security_id: Optional[str] = None
    ticker: Optional[str] = None
    name: Optional[str] = None
    performance_id: Optional[str] = None
    company_id: Optional[str] = None
    exchange: Optional[str] = None
    change: Optional[str] = None
    is_quant: Optional[str] = None
    uncertainty: Optional[str] = None
    evaluated_price: Optional[float] = None
    fair_value: Optional[float] = None
    previous_fair_value: Optional[float] = None
    one_star_price: Optional[float] = None
    two_star_price: Optional[float] = None
    four_star_price: Optional[float] = None
    five_star_price: Optional[float] = None
    price_to_fair_value: Optional[float] = None
    stock_star_rating: Optional[float] = None
    previous_stock_star_rating: Optional[float] = None
    evaluated_price_stock_star_rating: Optional[float] = None
    
@dataclass
class MarketIndexesSchema(DataFrameSchema):
    category: Optional[str] = None
    security_id: Optional[str] = None
    ticker: Optional[str] = None
    name: Optional[str] = None
    short_name: Optional[str] = None
    fund_id: Optional[str] = None
    master_portfolio_id: Optional[str] = None
    performance_id: Optional[str] = None
    exchange: Optional[str] = None
    exchange_country: Optional[str] = None
    currency: Optional[str] = None
    universe: Optional[str] = None
    trading_status: Optional[str] = None
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    last_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    year_high_price: Optional[float] = None
    year_low_price: Optional[float] = None
    adjusted_close_price: Optional[float] = None
    previous_close_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    net_change: Optional[float] = None
    percent_net_change: Optional[float] = None
    
@dataclass
class MarketMoversSchema(DataFrameSchema):
    category: Optional[str] = None
    ticker: Optional[str] = None
    name: Optional[str] = None
    currency: Optional[str] = None
    performance_id: Optional[str] = None
    exchange: Optional[str] = None
    market_cap: Optional[float] = None
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    trading_status: Optional[str] = None
    last_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    year_high_price: Optional[float] = None
    year_low_price: Optional[float] = None
    adjusted_close_price: Optional[float] = None
    previous_close_price: Optional[float] = None
    post_market_price: Optional[float] = None
    pre_market_price: Optional[float] = None
    bid_price: Optional[float] = None
    bid_price_size: Optional[int] = None
    ask_price: Optional[float] = None
    ask_price_size: Optional[int] = None
    percent_net_change: Optional[float] = None
    post_market_net_change: Optional[float] = None
    pre_market_net_change: Optional[float] = None
    updated_on: Optional[str] = None
    
@dataclass
class MarketCommoditiesSchema(DataFrameSchema):
    category: Optional[str] = None
    name: Optional[str] = None
    instrument_id: Optional[str] = None
    exchange: Optional[str] = None
    option_expiration_date: Optional[str] = None
    last_price: Optional[float] = None
    net_change: Optional[float] = None
    percent_net_change: Optional[float] = None
    updated_on: Optional[str] = None

@dataclass
class MarketCurrenciesSchema(DataFrameSchema):
    category: Optional[str] = None
    label: Optional[str] = None
    name: Optional[str] = None
    instrument_id: Optional[str] = None
    exchange: Optional[str] = None
    bid_price_decimals: Optional[int] = None
    bid_price: Optional[float] = None
    net_change: Optional[float] = None
    percent_net_change: Optional[float] = None
