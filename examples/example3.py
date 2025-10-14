import requests

import requests
import brotli  # pip install brotli
import re

ticker = "F000016GZH"

url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",  # le serveur renvoie Brotli
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}

resp = requests.get(url, headers=headers)
resp.raise_for_status()

# Le serveur t’envoie du Brotli => on décompresse manuellement
raw = resp.text

match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', raw)
api_key = match.group(1)


# url = "https://api-global.morningstar.com/sal-service/v1/etf/parent/mstarRating/StarRatingFundAsc/F00000IRIL/data"
url = f"https://api-global.morningstar.com/sal-service/v1/etf/factorProfile/{ticker}/data"

params = {
    "languageId": "fr",
    "locale": "fr",
    "clientId": "MDC",
    "benchmarkId": "prospectus_primary",
    "component": "sal-mip-star-rating-breakdown",
    "version": "4.69.0"
}

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "apikey": f"{api_key}",
    "cache-control": "no-cache",
    "origin": "https://global.morningstar.com",
    "pragma": "no-cache",
    # "referer": "https://global.morningstar.com/fr/investissements/etf/F00000IRIL/societe",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}

response = requests.get(url, headers=headers, params=params)
response.raise_for_status()

# Si le serveur renvoie du JSON
data = response.json()
data
