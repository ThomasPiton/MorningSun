from dataclasses import dataclass
from typing import Optional

from morningpy.core.dataframe_schema import DataFrameSchema

@dataclass
class FinancialStatementSchema(DataFrameSchema):
    security_id: Optional[str] = None
    security_label: Optional[str] = None
    statement_type: Optional[str] = None
    sub_type1: Optional[str] = None
    sub_type2: Optional[str] = None
    sub_type3: Optional[str] = None
    sub_type4: Optional[str] = None
    sub_type5: Optional[str] = None
    sub_type6: Optional[str] = None
    sub_type7: Optional[str] = None
    sub_type8: Optional[str] = None

@dataclass
class HoldingSchema(DataFrameSchema):
    parent_security_id: Optional[str] = None
    child_security_id: Optional[str] = None
    performance_id: Optional[str] = None
    security_name: Optional[str] = None
    holding_type_id: Optional[str] = None
    holding_type: Optional[str] = None
    weighting: Optional[float] = None
    number_of_share: Optional[float] = None
    market_value: Optional[float] = None
    original_market_value: Optional[float] = None
    share_change: Optional[float] = None
    country: Optional[str] = None
    ticker: Optional[str] = None
    isin: Optional[str] = None
    cusip: Optional[str] = None
    total_return_1y: Optional[float] = None
    forward_pe_ratio: Optional[float] = None
    stock_rating: Optional[str] = None
    assessment: Optional[str] = None
    economic_moat: Optional[str] = None
    sector: Optional[str] = None
    sector_code: Optional[str] = None
    secondary_sector_id: Optional[str] = None
    super_sector_name: Optional[str] = None
    primary_sector_name: Optional[str] = None
    secondary_sector_name: Optional[str] = None
    first_bought_date: Optional[str] = None
    maturity_date: Optional[str] = None
    coupon: Optional[float] = None
    currency: Optional[str] = None
    currency_name: Optional[str] = None
    local_currency_code: Optional[str] = None
    prospectus_net_expense_ratio: Optional[float] = None
    one_year_return: Optional[float] = None
    morningstar_rating: Optional[str] = None
    ep_used_for_overall_rating: Optional[int] = None
    analyst_rating: Optional[str] = None
    medalist_rating: Optional[str] = None
    medalist_rating_label: Optional[str] = None
    overall_rating_publish_date_utc: Optional[str] = None
    total_assets: Optional[float] = None
    ttm_yield: Optional[float] = None
    ep_used_for_1y_return: Optional[int] = None
    morningstar_category: Optional[str] = None
    total_assets_magnitude: Optional[float] = None
    last_turnover_ratio: Optional[float] = None
    sus_esg_risk_score: Optional[float] = None
    sus_esg_risk_globes: Optional[int] = None
    esg_as_of_date: Optional[str] = None
    sus_esg_risk_category: Optional[str] = None
    management_expense_ratio: Optional[float] = None
    qual_rating: Optional[str] = None
    quant_rating: Optional[str] = None
    best_rating_type: Optional[str] = None
    security_type: Optional[str] = None
    domicile_country_id: Optional[str] = None
    is_momentum_filter_flag: Optional[bool] = None
    
@dataclass
class HoldingInfoSchema(DataFrameSchema):
    master_portfolio_id: Optional[str] = None
    security_id: Optional[str] = None
    base_currency_id: Optional[str] = None
    domicile_country_id: Optional[str] = None
    portfolio_suppression: Optional[str] = None
    asset_type: Optional[str] = None
    portfolio_latest_date_footer: Optional[str] = None
    number_of_holding: Optional[int] = None
    number_of_equity_holding: Optional[int] = None
    number_of_bond_holding: Optional[int] = None
    number_of_other_holding: Optional[int] = None
    number_of_holding_short: Optional[int] = None
    number_of_equity_holding_short: Optional[int] = None
    number_of_bond_holding_short: Optional[int] = None
    number_of_other_holding_short: Optional[int] = None
    top_n_count: Optional[int] = None
    asset_type: Optional[str] = None
    top_holding_weighting: Optional[float] = None
    last_turnover: Optional[float] = None
    last_turnover_date: Optional[str] = None
    number_of_equity_holding_percentage: Optional[float] = None
    number_of_bond_holding_percentage: Optional[float] = None
    number_of_other_holding_percentage: Optional[float] = None
     