import os
import pandas as pd
import requests

def read_gzip_tickers():
    path = r"C:\Users\Thomas\VS\MorningSun\data"
    file_name = "ticker_index"
    file_path = os.path.join(path, f"{file_name}.parquet.gzip")
    
    df = pd.read_parquet(file_path)
    return df

def extract_tickers(page: int = 1, page_size: int = 100000):
    url = "https://tools.morningstar.co.uk/api/rest.svc/klr5zyak8x/security/screener"

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'origin': 'https://www.morningstar.fr',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': "Windows",
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }

    params = {
        'page': page,
        'pageSize': page_size,
        'sortOrder': 'LegalName asc',
        'outputType': 'json',
        'version': 1,
        'languageId': 'fr-FR',
        'currencyId': '',
        'universeIds': '',#'ETEUR$$ALL',
        'securityDataPoints': 'SecId|Name|Ticker|LegalName|CategoryName|Isin|Universe|Currency|AssetClass',
        'filters': '',
        'term': '',
        'subUniverseId': ''
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")

    data = response.json()

    # Récupération dans un DataFrame
    df = pd.DataFrame(data.get("rows", []))

    if "Ticker" not in df.columns:
        raise Exception("La clé 'Ticker' est absente dans la réponse")

    return df["Ticker"].dropna().unique().tolist()


if __name__ == '__main__':
    # df = read_gzip_tickers()
    # print(df.head())
    extract_tickers()
