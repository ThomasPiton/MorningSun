import asyncio
from typing import Union, List, Literal

from morningpy.extractor.timeseries import *
from morningpy.core.interchange import DataFrameInterchange


def get_all_tickers() -> pd.DataFrame:
    return AllTickersExtractor(forced_extract=forced_extract).run()

def get_ticker_info(id_security: Union[str, List[str]], isin: Union[str, List[str]], ticker: Union[str, List[str]]) -> pd.DataFrame:
    """Allows to get info from an id_security, isin or ticker
    can receive at teh same time id_security, isin and ticker
    input: id_security, isin and ticker
    action: get internal tickers.csv to get all info and then keep only the row with which the time id_security, isin and ticker match
    warning: teh fonction shoudl alert with message if the id_security, isin and ticker hasn't been found.
    output: list of id:security, isin, ticker, asset_type, exchange, security_label, country_code 
    """
    return AllTickersExtractor(forced_extract=forced_extract).run()

def convert_into(convert:str, to:str):
    
    return