import re
import requests

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
    maas_token = match.group(1)
    print("✅ maasToken trouvé:")
    print(maas_token)
else:
    print("❌ maasToken non trouvé dans le HTML brut")
    # Petit debug : afficher un extrait pour vérifier
    print(msg[:500])
    
    
    