# Use absolute import from the package root
from morningsun.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info
)

def extract():
    # 1 
    dates = ["2025-10-06","2025-10-07","2025-10-08","2025-10-09","2025-10-10"]
    value = get_market_us_calendar_info(date=dates,info_type="economic-releases").to
    
    # 2. 
    # value = get_market_commodities()
    
    #3. 
    # value = get_market_currencies()
    
    # 4. 
    # value = get_market_movers(info_type="gainers")
    
    # 5.
    # value = get_market_indexes(index_type="all")
    
    # 6. 
    # value = get_market_fair_value(value_type="overvaluated")
    
    #7. 
    value = get_market_info(info_type="global_barometer")
    
    return value

if __name__ == '__main__':
    value = extract()
    print(value)