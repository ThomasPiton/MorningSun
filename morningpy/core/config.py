DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.morningstar.com",
    "sec-ch-ua" : '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
}

URLS = {
    # "key_api":"https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js",
    "key_api":"https://global.morningstar.com/assets/quotes/1.0.41/sal-components.umd.min.7516.js",
    "maas_token":"https://www.morningstar.com/api/v2/stores/maas/token"
}

TICKERS_FILE = "tickers.parquet"