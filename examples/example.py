"""
website_url: https://www.morningstar.com/stocks/xnas/tsla/chart
api_url: https://www.us-api.morningstar.com/QS-markets/chartservice/v2/timeseries
=>
Comments: to get timeseries data
=>
"""

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import requests
from urllib.parse import unquote
import pandas as pd
import re

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Get Cookies

URL = r'https://global.morningstar.com/fr/investissements/etf/0P0001U0G4/graphique'

HEADERS = {
    'Accept': '*/*',
    "Content-Type": "application/json",
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
}

# Récupération de la page HTML
response = requests.get(URL, headers=HEADERS)
msg = response.text

# Regex pour capturer maasToken = "xxxxxx"
match = re.search(r'maasToken\s*[:=]\s*"([^"]+)"', msg)

if match:
    token = match.group(1)
    print("✅ maasToken trouvé:")
    print(token)
else:
    print("❌ maasToken non trouvé dans le HTML brut")
    # Petit debug : afficher un extrait pour vérifier
    print(msg[:500])

URL = r'https://www.us-api.morningstar.com/QS-markets/chartservice/v2/timeseries'

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "authorization": f"Bearer {token}",
    "cache-control": "no-cache",
    "origin": "https://global.morningstar.com",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://global.morningstar.com/fr/investissements/etf/0P0001U0G4/graphique",
    # "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
    "sec-ch-ua-mobile": "?0",
    # "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    # "x-api-requestid": "dc94dc2e-9938-430b-ac83-5c7197e7851f"
}


# PAYLOAD = {
#     'query': 'F00000VYXH:totalReturn,nav,dividend',
#     'frequency': 'd',
#     'startDate':'2015-06-30',
#     'endDate':'2025-09-26',
#     'trackMarketData': '3.6.5',
#     'instid': 'DOTCOM'
# }

PAYLOAD = {
    'query': '0P00014L1G:open,high,low,close,volume,previousClose',
    'frequency': 'd',
    'startDate':'1900-01-01',
    'endDate':'2025-09-26',
    'trackMarketData': '3.6.5',
    'instid': 'DOTCOM'
}

session = requests.Session()

response = session.get(URL,params=PAYLOAD, headers=HEADERS)

msg = response.json()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Proceed Data

data = msg[0]["series"]

df = pd.DataFrame(data)

df