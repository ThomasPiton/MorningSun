from morningpy.core.auth import AuthType

class MarketCalendarUsInfoConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/calendar"
    
    API_URL = "https://www.morningstar.com/api/v2/markets/calendar"
    
    PARAMS = {
        "date":"",
        "category":""
    }
    
    VALID_INPUTS = {
        "earnings", 
        "economic-releases", 
        "ipos", 
        "splits"
    }
    
    FIELD_MAPPING = {
        "base":{
            "calendar_date":"date",
            "updated_at":"updatedAt",
            "vendor":"vendor",
            "type":"type",
            "calendar":"calendar",
            "vendor_id":"vendorId",
        },
        "earnings":{
            "security_id":"securityID",
            "ticker":"ticker",
            "name":"name",
            "exchange":"exchange",
            "market_cap":"market_cap",
            "isin":"isin",
            "exchange_country":"exchangeCountry",
            "quarter_end_date":"quarterEndDate",
            "actual_diluted_eps":"actualDilutedEps",
            "net_income":"netIncome",
            "consensus_estimate":"consensusEstimate",
            "percentage_surprise":"percentageSurprise",
            "quarterly_sales":"quarterlySales"
        }, 
        "economic-releases":{
            "release":"release",
            "period":"period",
            "release_time":"releaseTime",
            "consensus_estimate":"consensusEstimate",
            "briefing_estimate":"briefingEstimate",
            "after_release_actual":"afterReleaseActual",
            "prior_release_actual":"priorReleaseActual"
        }, 
        "ipos":{
            "security_id":"securityID",
            "ticker":"ticker",
            "name":"name",
            "exchange":"exchange",
            "market_cap":"marketCap",
            "share_value":"shareValue",
            "opened_share_value":"openedShareValue",
            "lead_underwriter":"leadUnderWriter",
            "initial_shares":"initialShares",
            "initial_low_range":"initialLowRange",
            "initial_high_range":"initialHighRange",
            "date_priced":"datePriced",
            "week_priced":"weekPriced",
            "company_description":"description",
        }, 
        "splits":{
            "security_id":"securityID",
            "ticker":"ticker",
            "name":"name",
            "exchange":"exchange",
            "market_cap":"marketCap",
            "share_worth":"shareWorth",
            "old_share_worth":"oldShareWorth",
            "ex_date":"exDate",
            "announce_date":"announceDate",
            "payable_date":"payableDate"
        }
    }
    
    
    
class MarketFairValueConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/fair-value"
    
    API_URL = "https://www.morningstar.com/api/v2/markets/fair-value"
    
    VALID_INPUTS = {
        "undervaluated", 
        "overvaluated"
    }
    
    MAPPING_INPUTS = {
        "undervaluated": "undervaluedStocks",
        "overvaluated": "overvaluedStocks",
    }
    
    RENAME_COLUMNS = {
        "securityID":"security_id",
        "performanceID":"performance_id",
        "companyID":"company_id",
        "evaluatedPrice":"evaluated_price",
        "fairValue":"fair_value",
        "fairValue_isQuant":"is_quant",
        "fairValue_change":"change",
        "fairValue_previous":"previous_fair_value",
        "fairValue_uncertainty":"uncertainty",
        "fairValue_oneStarPrice":"one_star_price",
        "fairValue_twoStarPrice":"two_star_price",
        "fairValue_fourStarPrice":"four_star_price",
        "fairValue_fiveStarPrice":"five_star_price",
        "priceToFairValue":"price_to_fair_value",
        "stockStarRating":"stock_star_rating",
        "stockStarRating_previous":"previous_stock_star_rating",
        "stockStarRating_evaluatedPrice":"evaluated_price_stock_star_rating"
    }
    
    STRING_COLUMNS = [
        "category",        
        "security_id",
        "ticker",
        "name",
        "performance_id",
        "company_id",
        "exchange",
        "change",
        "is_quant",
        "uncertainty"
    ]
    
    NUMERIC_COLUMNS = [
        "evaluated_price",
        "fair_value",
        "previous_fair_value",
        "one_star_price",
        "two_star_price",
        "four_star_price",
        "five_star_price",
        "price_to_fair_value",
        "stock_star_rating",
        "previous_stock_star_rating",
        "evaluated_price_stock_star_rating"
    ]
    
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS


class MarketIndexesConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/indexes"
    
    API_URL = "https://www.morningstar.com/api/v2/markets/indexes"
    
    VALID_INPUTS = {
        "americas", 
        "asia", 
        "europe", 
        "private", 
        "sector", 
        "us"
    }
    
    MAPPING_INPUTS = {
        "americas": "americasIndexes",
        "asia": "asiaIndexes",
        "europe": "europeIndexes",
        "private": "privateIndexes",
        "sector": "sectorIndexes",
        "us": "usIndexes",
    }
    
    RENAME_COLUMNS = {
        "netChange":"net_change",
        "askPrice":"ask_price",
        "adjustedClosePrice":"adjusted_close_price",
        "openPrice":"open_price",
        "tradingStatus":"trading_status",
        "yearLowPrice":"year_low_price",
        "bidPrice":"bid_price",
        "previousClosePrice":"previous_close_price",
        "performanceID":"performance_id",
        "lowPrice":"low_price",
        "listedCurrency":"currency",
        "highPrice":"high_price",
        "yearHighPrice":"year_high_price",
        "securityID":"security_id",
        "fundID":"fund_id",
        "masterPortfolioID":"master_portfolio_id",
        "exchangeCountry":"exchange_country",
        "shortName":"short_name",
        "averageVolume":"avg_volume",
        "percentNetChange":"percent_net_change",
        "lastPrice":"last_price"
    }
    
    STRING_COLUMNS = [        
        "category",
        "security_id",
        "ticker",
        "name",
        "short_name",
        "fund_id",
        "master_portfolio_id",
        "performance_id",
        "exchange",
        "exchange_country",
        "currency",
        "universe",
        "trading_status"
    ]
    
    NUMERIC_COLUMNS = [
        "volume",
        "avg_volume",
        "last_price",
        "open_price",
        "high_price",
        "low_price",
        "year_high_price",
        "year_low_price",
        "adjusted_close_price",
        "previous_close_price",
        "bid_price",
        "ask_price",
        "net_change",
        "percent_net_change",
    ]
    
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS


class MarketMoversConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/movers"
    
    API_URL = "https://www.morningstar.com/api/v2/stores/realtime/movers"
    
    VALID_INPUTS = {
        "gainers", 
        "losers", 
        "actives"
    }
    
    RENAME_COLUMNS = {
        "preMarketNetChange":"pre_market_net_change",
        "openPrice":"open_price",
        "tradingStatus":"trading_status",
        "lowPrice":"low_price",
        "highPrice":"high_price",
        "percentNetChange":"percent_net_change",
        "preMarketPrice":"pre_market_price",
        "netChange":"net_change",
        "askPrice":"ask_price",
        "askPrice_size":"ask_price_size",
        "marketCap":"market_cap",
        "ticker":"ticker",
        "adjustedClosePrice":"adjusted_close_price",
        "yearLowPrice":"year_low_price",
        "bidPrice":"bid_price",
        "bidPrice_size":"bid_price_size",
        "previousClosePrice":"previous_close_price",
        "performanceID":"performance_id",
        "averageVolume":"avg_volume",
        "listedCurrency":"currency",
        "yearHighPrice":"year_high_price",
        "lastPrice":"last_price",
    }
    
    STRING_COLUMNS = [        
        "category",
        "ticker",
        "name",
        "currency",
        "performance_id",
        "exchange",
        "updated_on"
    ]
    
    NUMERIC_COLUMNS = [
        "market_cap",
        "volume",
        "avg_volume",
        "trading_status",
        "last_price",
        "open_price",
        "high_price",
        "low_price",
        "year_high_price",
        "year_low_price",
        "adjusted_close_price",
        "previous_close_price",
        "pre_market_price",
        "bid_price",
        "bid_price_size",
        "ask_price",
        "ask_price_size",
        "percent_net_change",
        "pre_market_net_change",
    ]
    
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS


class MarketCommoditiesConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/commodities"
    
    API_URL = "https://www.morningstar.com/api/v2/markets/commodities"
    
    RENAME_COLUMNS = {
        "instrumentID":"instrument_id",
        "netChange_exchange":"exchange",
        "netChange_value":"net_change",
        "percentNetChange_value":"percent_net_change",
        "lastPrice_value":"last_price",
        "optionExpirationDate_value":"option_expiration_date"
    }
    
    FINAL_COLUMNS = [
        "category",
        "name",
        "instrument_id",
        "exchange",
        "option_expiration_date",
        "last_price",
        "net_change",
        "percent_net_change"
    ]
    
    
class MarketCurrenciesConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    
    PAGE_URL = "https://www.morningstar.com/markets/currencies"
    
    API_URL = "https://www.morningstar.com/api/v2/markets/currencies"
    
    RENAME_COLUMNS = {
        "instrumentID":"instrument_id",
        "bidPriceDecimals":"bid_price_decimals",
        "bidPrice_value":"bid_price",
        "netChange_value":"net_change",
        "percentNetChange_exchange":"exchange",
        "percentNetChange_value":"percent_net_change",
    }
    
    FINAL_COLUMNS = [
        "category",
        "label",
        "name",
        "instrument_id",
        "exchange",
        "bid_price_decimals",
        "bid_price",
        "net_change",
        "percent_net_change"
    ]