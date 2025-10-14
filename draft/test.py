import requests
from urllib.parse import unquote
import pandas as pd
import re

def extract():

    URL = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"

    HEADERS = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",  # le serveur renvoie Brotli
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }

    resp = requests.get(URL, headers=HEADERS)
    raw = resp.text
    match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', raw)
    api_key = match.group(1)

    URL = r'https://global.morningstar.com/api/v1/en-ca/tools/screener/_data'

    HEADERS = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "apikey": f"{api_key}",
        "cache-control": "no-cache",
        "origin": "https://global.morningstar.com",
        "pragma": "no-cache",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    investment_type = {
        "FO":"Mutual Funds",
        "EQ":"Stocks",
        "FE":"ETF"
    }
    PARAMS = {
        "query":"(((investmentType = 'EQ'))) AND (exchangeCountry in ('CAN', 'ZWE', 'ZMB', 'ZAF', 'VNM', 'VEN', 'USA', 'URY', 'UKR', 'TZA', 'TUN', 'TUR', 'TWN', 'TTO', 'THA', 'SWZ', 'SWE', 'SVN', 'SVK', 'SRB', 'SGP', 'ARE', 'ARG', 'ARM', 'AUS', 'AUT', 'BEL', 'BGD', 'BGR', 'BHR', 'BIH', 'BMU', 'BOL', 'BRA', 'BWA', 'CHE', 'CHN', 'CHL', 'COL', 'CIV', 'CZE', 'CYP', 'FRA', 'FIN', 'EST', 'ESP', 'EGY', 'ECU', 'DNK', 'DEU', 'SAU', 'RUS', 'ROU', 'QAT', 'PSE', 'PRT', 'POL', 'PHL', 'PER', 'PAN', 'PAK', 'OMN', 'NZL', 'NPL', 'NOR', 'NLD', 'NGA', 'NAM', 'MYS', 'MWI', 'MUS', 'MNE', 'MLT', 'MKD', 'MEX', 'MAR', 'LVA', 'LTU', 'LUX', 'LKA', 'LBN', 'KWT', 'KOR', 'KEN', 'KAZ', 'JPN', 'JOR', 'JAM', 'ITA', 'ISR', 'ISL', 'IRQ', 'IRN', 'IRL', 'IND', 'IDN', 'HUN', 'HRV', 'HKG', 'GRC', 'GHA', 'GBR'))",
        "fields":"name,ticker,sector,industry,stockStyleBox,marketCap,dividendYield[trailing],exchange",
        "page":1,
        "limit":500,
        "sort":"name:asc"
    }

    response = requests.get(URL, headers=HEADERS, params=PARAMS)
    data = response.json()
    data
    
    
if __name__ == '__main__':
    extract()
