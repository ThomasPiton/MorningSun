from morningpy.core.auth import AuthType

class FinancialStatementConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.API_KEY
    
    API_URL = "https://api-global.morningstar.com/sal-service/v1/stock/newfinancials/"

    PARAMS = {
        "dataType": "Q",
        "reportType": "A",
        "languageId": "en-ca",
        "locale": "en-ca",
        "clientId": "MDC",
        "benchmarkId": "undefined",
        "component": "sal-equity-financials",
        "version": "4.69.0"
    }
    
    VALID_FREQUENCY = {
        "Annualy",
        "Quarterly"
    }
    
    MAPPING_FREQUENCY = {
        "Annualy":"A",
        "Quarterly":"Q"
    }
    
    ENDPOINT = {
        "Balance Sheet":"balanceSheet",
        "Cash Flow Statement":"cashFlow",
        "Income Statement":"incomeStatement"
    }
    FILTER_VALUE = {
        "balance-sheet":"BalanceSheet",
        "cash-flow":"CashFlow",
        "income-statement":"IncomeStatement"
    }


class HoldingConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.API_KEY
    
    API_URL = "https://api-global.morningstar.com/sal-service/v1/etf/portfolio/holding/v2/"
    
    PARAMS = {
        "premiumNum": 10000,
        "freeNum": 10000,
        "hideesg": "false",
        "languageId": "en-ca",
        "locale": "en-ca",
        "clientId": "MDC",
        "benchmarkId": "prospectus_primary",
        "component": "sal-mip-holdings",
        "version": "4.69.0"
    }
    
    FIELD_MAPPING = {
        "child_security_id": "secId",
        "performance_id": "performanceId",
        "security_name": "securityName",
        "holding_type_id": "holdingTypeId",
        "holding_type": "holdingType",
        "weighting": "weighting",
        "number_of_share": "numberOfShare",
        "market_value": "marketValue",
        "original_market_value": "originalMarketValue",
        "share_change": "shareChange",
        "country": "country",
        "ticker": "ticker",
        "isin": "isin",
        "cusip": "cusip",
        "total_return_1y": "totalReturn1Year",
        "forward_pe_ratio": "forwardPERatio",
        "stock_rating": "stockRating",
        "assessment": "assessment",
        "economic_moat": "economicMoat",
        "sector": "sector",
        "sector_code": "sectorCode",
        "secondary_sector_id": "secondarySectorId",
        "super_sector_name": "superSectorName",
        "primary_sector_name": "primarySectorName",
        "secondary_sector_name": "secondarySectorName",
        "first_bought_date": "firstBoughtDate",
        "maturity_date": "maturityDate",
        "coupon": "coupon",
        "currency": "currency",
        "currency_name": "currencyName",
        "local_currency_code": "localCurrencyCode",
        "prospectus_net_expense_ratio": "prospectusNetExpenseRatio",
        "one_year_return": "oneYearReturn",
        "morningstar_rating": "morningstarRating",
        "ep_used_for_overall_rating": "epUsedForOverallRating",
        "analyst_rating": "analystRating",
        "medalist_rating": "medalistRating",
        "medalist_rating_label": "medalistRatingLabel",
        "overall_rating_publish_date_utc": "overallRatingPublishDateUtc",
        "total_assets": "totalAssets",
        "ttm_yield": "ttmYield",
        "ep_used_for_1y_return": "epUsedFor1YearReturn",
        "morningstar_category": "morningstarCategory",
        "total_assets_magnitude": "totalAssetsMagnitude",
        "last_turnover_ratio": "lastTurnoverRatio",
        "sus_esg_risk_score": "susEsgRiskScore",
        "sus_esg_risk_globes": "susEsgRiskGlobes",
        "esg_as_of_date": "esgAsOfDate",
        "sus_esg_risk_category": "susEsgRiskCategory",
        "management_expense_ratio": "managementExpenseRatio",
        "qual_rating": "qualRating",
        "quant_rating": "quantRating",
        "best_rating_type": "bestRatingType",
        "security_type": "securityType",
        "domicile_country_id": "domicileCountryId",
        "is_momentum_filter_flag": "isMomentumFilterFlag",
    }
    
    RENAME_COLUMNS = {
        "securityName": "security_name",
        "secId": "security_id",
        "performanceId": "performance_id",
        "holdingTypeId": "holding_type_id",
        "weighting": "weighting",
        "numberOfShare": "number_of_share",
        "marketValue": "market_value",
        "shareChange": "share_change",
        "country": "country",
        "ticker": "ticker",
        "totalReturn1Year": "total_return_1y",
        "forwardPERatio": "forward_pe_ratio",
        "stockRating": "stock_rating",
        "assessment": "assessment",
        "economicMoat": "economic_moat",
        "sector": "sector",
        "sectorCode": "sector_code",
        "holdingTrend": "holding_trend",
        "holdingType": "holding_type",
        "isin": "isin",
        "cusip": "cusip",
        "secondarySectorId": "secondary_sector_id",
        "superSectorName": "super_sector_name",
        "primarySectorName": "primary_sector_name",
        "secondarySectorName": "secondary_sector_name",
        "firstBoughtDate": "first_bought_date",
        "maturityDate": "maturity_date",
        "coupon": "coupon",
        "currency": "currency",
        "localCurrencyCode": "local_currency_code",
        "prospectusNetExpenseRatio": "prospectus_net_expense_ratio",
        "oneYearReturn": "one_year_return",
        "morningstarRating": "morningstar_rating",
        "ePUsedForOverallRating": "ep_used_for_overall_rating",
        "analystRating": "analyst_rating",
        "medalistRating": "medalist_rating",
        "medalistRatingLabel": "medalist_rating_label",
        "overallRatingPublishDateUtc": "overall_rating_publish_date_utc",
        "totalAssets": "total_assets",
        "ttmYield": "ttm_yield",
        "epUsedFor1YearReturn": "ep_used_for_1y_return",
        "morningstarCategory": "morningstar_category",
        "totalAssetsMagnitude": "total_assets_magnitude",
        "lastTurnoverRatio": "last_turnover_ratio",
        "susEsgRiskScore": "sus_esg_risk_score",
        "susEsgRiskGlobes": "sus_esg_risk_globes",
        "esgAsOfDate": "esg_as_of_date",
        "susEsgRiskCategory": "sus_esg_risk_category",
        "managementExpenseRatio": "management_expense_ratio",
        "qualRating": "qual_rating",
        "quantRating": "quant_rating",
        "bestRatingType": "best_rating_type",
        "securityType": "security_type",
        "domicileCountryId": "domicile_country_id",
        "currencyName": "currency_name",
        "originalMarketValue": "original_market_value",
        "isMomentumFilterFlag": "is_momentum_filter_flag",
    }
    
    COLUMNS = [
        "parent_security_id",
        "child_security_id",
        "security_name",
        "performance_id",
        "holding_type_id",
        "country",
        "ticker",
        "stock_rating",
        "assessment",
        "economic_moat",
        "sector",
        "sector_code",
        "holding_type",
        "isin",
        "cusip",
        "secondary_sector_id",
        "super_sector_name",
        "primary_sector_name",
        "secondary_sector_name",
        "first_bought_date",
        "maturity_date",
        "currency",
        "local_currency_code",
        "prospectus_net_expense_ratio",
        "one_year_return",
        "morningstar_rating",
        "ep_used_for_overall_rating",
        "analyst_rating",
        "medalist_rating",
        "medalist_rating_label",
        "overall_rating_publish_date_utc",
        "morningstar_category",
        "esg_as_of_date",
        "sus_esg_risk_category",
        "management_expense_ratio",
        "qual_rating",
        "quant_rating",
        "best_rating_type",
        "security_type",
        "domicile_country_id",
        "currency_name",
        "is_momentum_filter_flag",
        "weighting",
        "number_of_share",
        "market_value",
        "share_change",
        "total_return_1y",
        "forward_pe_ratio",
        "ep_used_for_1y_return",
        "sus_esg_risk_score",
        "sus_esg_risk_globes",
        "original_market_value",
    ]


class HoldingInfoConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.API_KEY
    
    API_URL = "https://api-global.morningstar.com/sal-service/v1/etf/portfolio/holding/v2/"
    
    PARAMS = {
        "premiumNum": 10000,
        "freeNum": 1,
        "hideesg": "false",
        "languageId": "en-ca",
        "locale": "en-ca",
        "clientId": "MDC",
        "benchmarkId": "prospectus_primary",
        "component": "sal-mip-holdings",
        "version": "4.69.0"
    }
    
    FIELD_MAPPING = {
        "master_portfolio_id": "masterPortfolioId",
        "base_currency_id": "baseCurrencyId",
        "domicile_country_id": "domicileCountryId",
        "portfolio_suppression": "portfolioSuppression",
        "asset_type": "assetType",
        "portfolio_latest_date_footer": "portfolioLastestDateFooter",
        "number_of_holding": "numberOfHolding",
        "number_of_equity_holding": "numberOfEquityHolding",
        "number_of_bond_holding": "numberOfBondHolding",
        "number_of_other_holding": "numberOfOtherHolding",
        "number_of_holding_short": "numberOfHoldingShort",
        "number_of_equity_holding_short": "numberOfEquityHoldingShort",
        "number_of_bond_holding_short": "numberOfBondHoldingShort",
        "number_of_other_holding_short": "numberOfOtherHoldingShort",
        "top_n_count": "topNCount",
        "number_of_equity_holding_percentage": "numberOfEquityHoldingPer",
        "number_of_bond_holding_percentage": "numberOfBondHoldingPer",
        "number_of_other_holding_percentage": "numberOfOtherHoldingPer",
    }
    
    HOLDING_SUMMARY_MAPPING = {
        "top_holding_weighting": "topHoldingWeighting",
        "last_turnover": "lastTurnover",
        "last_turnover_date": ["lastTurnoverDate", "LastTurnoverDate"],  # Handle both cases
    }
    
    RENAME_COLUMNS = {
        "masterPortfolioId": "master_portfolio_id",
        "secId": "security_id",
        "baseCurrencyId": "base_currency_id",
        "domicileCountryId": "domicile_country_id",
        "numberOfHolding": "number_of_holding",
        "numberOfEquityHolding": "number_of_equity_holding",
        "numberOfBondHolding": "number_of_bond_holding",
        "numberOfOtherHolding": "number_of_other_holding",
        "numberOfHoldingShort": "number_of_holding_short",
        "numberOfEquityHoldingShort": "number_of_equity_holding_short",
        "numberOfBondHoldingShort": "number_of_bond_holding_short",
        "numberOfOtherHoldingShort": "number_of_other_holding_short",
        "topNCount": "top_n_count",
        "portfolioSuppression": "portfolio_suppression",
        "assetType": "asset_type",
        "portfolioLastestDateFooter": "portfolio_latest_date_footer",
        "userType": "user_type",
        "noPremiumChinaFund": "no_premium_china_fund",
        "numberOfEquityHoldingPer": "number_of_equity_holding_percentage",
        "numberOfBondHoldingPer": "number_of_bond_holding_percentage",
        "numberOfOtherHoldingPer": "number_of_other_holding_percentage",
        "holdingSummary.topHoldingWeighting": "top_holding_weighting",
        "holdingSummary.lastTurnover": "last_turnover",
        "holdingSummary.LastTurnoverDate": "last_turnover_date",
    }
    
    COLUMNS = [
        "security_id",
        "master_portfolio_id",
        "base_currency_id",
        "domicile_country_id",
        "portfolio_suppression",
        "asset_type",
        "portfolio_latest_date_footer",
        "number_of_holding",
        "number_of_equity_holding",
        "number_of_bond_holding",
        "number_of_other_holding",
        "number_of_holding_short",
        "number_of_equity_holding_short",
        "number_of_bond_holding_short",
        "number_of_other_holding_short",
        "top_n_count",
        "top_holding_weighting",
        "last_turnover",
        "number_of_equity_holding_percentage",
        "number_of_bond_holding_percentage",
        "number_of_other_holding_percentage",
    ]
    
    