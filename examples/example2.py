import requests
import brotli  # pip install brotli
import re

url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",  # le serveur renvoie Brotli
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://global.morningstar.com/fr/investissements/etf/0P00000HUH/cours",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}

resp = requests.get(url, headers=headers)
resp.raise_for_status()

# Le serveur t’envoie du Brotli => on décompresse manuellement
raw = resp.text

match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', raw)
api_key = match.group(1)

match = re.search(r'tokenRealtime\s*[:=]\s*"([^"]+)"', raw)
tokenRealtime = match.group(1)

