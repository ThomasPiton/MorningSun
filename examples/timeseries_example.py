"""
Timeseries Data Example for MorningPy
"""
from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

def run():
    # Intraday data
    intraday = get_intraday_timeseries(
        security_id=["0P00009WL3"],
        start_date="2024-11-08",
        end_date="2025-11-07",
        frequency="1min",
        pre_after=False
    )
    print("Intraday Timeseries:")
    print(intraday.head())

    # Historical daily data
    historical = get_historical_timeseries(
        security_id=["0P0000OQN8","0P0001RWKZ"],
        start_date="2010-11-05",
        end_date="2025-11-05",
        frequency="daily",
        pre_after=False
    )
    print("\nHistorical Timeseries:")
    print(historical.head())

if __name__ == "__main__":
    run()
