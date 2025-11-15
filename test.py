import pandas as pd

df = pd.read_parquet("data/tickers.parquet")

print(df.head())