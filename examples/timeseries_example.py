"""
Timeseries Data Example for MorningPy
"""
from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

def run():
    
    intraday = get_intraday_timeseries(
        security_id=["0P0000OQN8"],
        start_date="2025-01-01",
        end_date="2025-12-11",
        frequency="10min",
        pre_after=False
    ).to_pandas_dataframe()
    print("Intraday Timeseries:")
    print(intraday.head())

    historical = get_historical_timeseries(
        security_id=["0P0000OQN8","0P0001RWKZ"],
        start_date="2010-11-05",
        end_date="2025-11-05",
        frequency="daily",
        pre_after=False
    ).to_pandas_dataframe()
    print("\nHistorical Timeseries:")
    print(historical.head())

    return "Correctly extracted !"
    
if __name__ == "__main__":
    print(run())
