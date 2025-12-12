"""
Market Data Example for MorningPy
"""
from morningpy.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value
)

def run():
    # US earnings calendar
    calendar_info = get_market_us_calendar_info(
        date=["2025-08-01","2025-09-01","2025-10-01"],
        info_type="earnings"
    ).to_pandas_dataframe()
    print("US Calendar Info:")
    print(calendar_info.head())

    # # Commodities
    commodities = get_market_commodities().to_pandas_dataframe()
    print("\nCommodities:")
    print(commodities.head())

    # # # Currencies
    currencies = get_market_currencies().to_pandas_dataframe()
    print("\nCurrencies:")
    print(currencies.head())

    # # Market Movers
    movers = get_market_movers(
        mover_type=["gainers", "losers", "actives"]).to_pandas_dataframe()
    print("\nMarket Movers:")
    print(movers.head())

    # # Market Indexes
    indexes = get_market_indexes(index_type="americas").to_pandas_dataframe()
    print("\nMarket Indexes:")
    print(indexes.head())

    # # # Market Fair Value
    fair_value = get_market_fair_value(value_type="overvaluated").to_pandas_dataframe()
    print("\nMarket Fair Value:")
    print(fair_value.head())

    return "Correctly extracted !"
    
if __name__ == "__main__":
    print(run())
