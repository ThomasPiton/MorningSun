"""
Ticker Data Example for MorningPy
"""
from morningpy.api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_stocks,
    get_all_securities,
    convert
)

def run():
    # Retrieve all ETFs
    etfs = get_all_etfs()
    print("ETFs:")
    print(etfs.head())

    # Retrieve all stocks and funds
    stocks = get_all_stocks()
    print("\nStocks:")
    print(stocks.head())

    funds = get_all_funds()
    print("\nFunds:")
    print(funds.head())

    # Convert ticker or ISIN
    converted = convert(["US7181721090", "0P0001PU03"])
    print("\nConverted IDs:")
    print(converted.head())

if __name__ == "__main__":
    run()
