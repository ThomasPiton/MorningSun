from morningpy.core.auth import AuthType

class IntradayTimeseriesConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.BEARER_TOKEN
    
    API_URL = "https://www.us-api.morningstar.com/QS-markets/chartservice/v2/timeseries"
    
    PARAMS = {
        'query': '',
        'frequency': '',
        'preAfter': '',
        'trackMarketData': '3.6.5',
        'instid': 'DOTCOM'
    }
    
    VALID_FREQUENCY = {
        "1min", 
        "5min",
        "10min",
        "15min",
        "30min",
        "60min"
    }
    
    MAPPING_FREQUENCY = {
        "1min":1,
        "5min":5,
        "10min":10,
        "15min":15,
        "30min":30,
        "60min":60,
    }
    
    FIELD_MAPPING = {
        "date":"date",
        "open":"open",
        "high":"high",
        "low":"low",
        "close":"close",
        "volume":"volume",
    }
    
    STRING_COLUMNS = [       
        "security_id",
        "date"
    ]
    
    NUMERIC_COLUMNS = [
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
    ]
    
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS


class HistoricalTimeseriesConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.BEARER_TOKEN
    
    API_URL = "https://www.us-api.morningstar.com/QS-markets/chartservice/v2/timeseries"
    
    PARAMS = {
        'query': '',
        'frequency': '',
        'preAfter': '',
        'trackMarketData': '3.6.5',
        'instid': 'DOTCOM'
    }
    
    VALID_FREQUENCY = {
        "daily", 
        "weekly",
        "monthly",
    }
    
    MAPPING_FREQUENCY = {
        "daily":"d",
        "weekly":"w",
        "monthly":"m",
    }
    
    FIELD_MAPPING = {
        "date":"date",
        "open":"open",
        "high":"high",
        "low":"low",
        "close":"close",
        "volume":"volume",
        "previous_close":"previousClose"
    }
    
    STRING_COLUMNS = [       
        "security_id",
        "date"
    ]
    
    NUMERIC_COLUMNS = [
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
    ]
    
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS