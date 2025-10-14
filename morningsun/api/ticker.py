

def get_all_tickers(forced_extract: bool = False) -> pd.DataFrame:
    return AllTickersExtractor(forced_extract=forced_extract).run()