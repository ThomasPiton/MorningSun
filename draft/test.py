import asyncio
import aiohttp
import re
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
import requests

def draft_extract():
    

    URL = r'https://global.morningstar.com/api/v1/en-ea/tools/screener/_data'

    HEADERS = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": f"{cookie}",
        "newrelic": f"{newrelic}",
        "cache-control": "no-cache",
        # "origin": "https://global.morningstar.com",
        "pragma": "no-cache",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    }
    investment_type = {
        "FO":"Mutual Funds",
        "EQ":"Stocks",
        "FE":"ETF"
    }

    fields = [
        "name",
        "sector",
        "industry",
        "stockStyleBox",
        "marketCap",
        "distributionYield",
        "dividendDistributionFrequency",
        "dividendYield",
        "medalistRating[overall]",
        "sustainabilityRating",
        "fundSize",
        "inceptionDate",
        "morningstarRiskRating",
        "fundStarRating[overall]",
        "brandingName",
        "broadCategoryGroup",
        "morningstarCategory",
        "distributionFundType",
        "fundEquityStyleBox",
        "fundFixedIncomeStyleBox",
        "managementExpenseRatio",
        "minimumInitialInvestment",
        "baseCurrency",
    ]
    
    
    PARAMS = {
        "query":"(((investmentType = 'EQ'))) AND (exchangeCountry in ('CAN', 'ZWE', 'ZMB', 'ZAF', 'VNM', 'VEN', 'USA', 'URY', 'UKR', 'TZA', 'TUN', 'TUR', 'TWN', 'TTO', 'THA', 'SWZ', 'SWE', 'SVN', 'SVK', 'SRB', 'SGP', 'ARE', 'ARG', 'ARM', 'AUS', 'AUT', 'BEL', 'BGD', 'BGR', 'BHR', 'BIH', 'BMU', 'BOL', 'BRA', 'BWA', 'CHE', 'CHN', 'CHL', 'COL', 'CIV', 'CZE', 'CYP', 'FRA', 'FIN', 'EST', 'ESP', 'EGY', 'ECU', 'DNK', 'DEU', 'SAU', 'RUS', 'ROU', 'QAT', 'PSE', 'PRT', 'POL', 'PHL', 'PER', 'PAN', 'PAK', 'OMN', 'NZL', 'NPL', 'NOR', 'NLD', 'NGA', 'NAM', 'MYS', 'MWI', 'MUS', 'MNE', 'MLT', 'MKD', 'MEX', 'MAR', 'LVA', 'LTU', 'LUX', 'LKA', 'LBN', 'KWT', 'KOR', 'KEN', 'KAZ', 'JPN', 'JOR', 'JAM', 'ITA', 'ISR', 'ISL', 'IRQ', 'IRN', 'IRL', 'IND', 'IDN', 'HUN', 'HRV', 'HKG', 'GRC', 'GHA', 'GBR'))",
        "fields":",".join(fields),
        "page":21,
        "limit":500,
        "sort":"name:asc"
    }

    response = requests.get(URL, headers=HEADERS, params=PARAMS)
    data = response.json()
    pages = data["pages"] # provide a number of page to call
    results = data["results"]
    
    list_info = []
    for result in results:
        row = {}
        data_base = result['meta'] # dict format
        data_field = {key:info['value'] for key, info in result['fields']} # dict format
        row.update(**data_base+data_field)         
        list_info.append(row)
        
    df = pd.concat(list_info)
    df.rename(columns={"name":"security_label","securityID":"security_id","performanceID":"performance_id","companyID":"company_id"},inplace=True)
    


if __name__ == "__main__":
    df = draft_extract()
    print(df.head())
