# Use absolute import from the package root
from morningpy.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info
)

from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

from morningpy.api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_stocks,
    get_all_securities,
    convert
)

from morningpy.api.news import (
    get_headline_news
)


def extract():
    
    df = get_headline_news(
        market="Spain",
        news="economy",
        edition="Central Europe")
    df
    # df = get_holding_info(
    #     performance_id=["0P0001PU03","0P0001BG3E","0P0001F9QM","0P000192KF"])    
    
    # df = get_holding(
    #     performance_id=[
    #         "0P0001PU03",
    #         "0P00013Z57",
    #         # "0P0001F9QM",
    #         # "0P000192KF"
    #         ])        
    
    # df = get_financial_statement(
    #     statement_type="Income Statement",
    #     report_frequency="Quarterly",
    #     security_id=["0P000115U4"])
    

    
    # df = get_intraday_timeseries(
    #     # ticker=["DIGIGR","EVLI","GRK"],
    #     # isin=["US7181721090","CH1107979838","US29081P3038"],
    #     # id_security=["0P0000BCG9","0P0000BZKB","0P0001BSGZ","0P0001BSG","0P0001BS6Z"],
    #     id_security=["0P00009WL3"],
    #     start_date="2020-11-08",
    #     end_date="2025-11-07",
    #     # start_date="2025-10-06",
    #     # end_date="2025-11-05",
    #     frequency="1min",
    #     pre_after=False)
    
    # df = get_historical_timeseries(
    #     # ticker=["DIGIGR","EVLI","GRK"],
    #     # isin=["US7181721090","CH1107979838","US29081P3038"],
    #     id_security=["0P0000OQN8","0P0001RWKZ"],
    #     # id_security=["0P0000OQN8"],
    #     start_date="2025-11-05",
    #     end_date="2025-11-05",
    #     frequency="daily",
    #     pre_after=False
    # )
    
    # 1 
    
    dates = ["2025-10-06","2025-10-07","2025-10-08","2025-10-09","2025-10-10"]
    dates = ["2025-10-23"]
    df = get_market_us_calendar_info(date=dates,info_type="earnings").to_pandas_dataframe()
    df = get_market_commodities() 
    df = get_market_currencies()
    df = get_market_movers(mover_type=["gainers", "losers", "actives"])
    df = get_market_indexes(index_type=["americas","us"])
    df = get_market_fair_value(value_type=["overvaluated","undervaluated"])
    df = get_market_info(info_type=["global_barometer","commodities"])
    
    # test Interchange
    print(df.head())
    pl_df = df.to_polars_dataframe()
    dask_df = df.to_dask_dataframe()
    arrow_table = df.to_arrow_table()
    
    return df

if __name__ == '__main__':
    value = extract()
    print(value)