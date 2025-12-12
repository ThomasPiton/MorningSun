from morningpy.core.auth import AuthType

class HeadlineNewsConfig:
    
    REQUIRED_AUTH: AuthType = AuthType.BEARER_TOKEN
    
    API_URL = "https://global.morningstar.com/api/v1"
    
    PARAMS = {
        "marketID":"",
        "sectionFallBack":""
    }
    
    COLUMNS = [
        'news', 
        'market', 
        'display_date', 
        'title', 
        'subtitle', 
        'tags', 
        'link', 
        'language'
    ]
    
    MARKET_ID = {
        "All Europe": "eu",
        "Asia": "ea",
        "Austria": "at",
        "Belgium": "be",
        "Canada": "ca",
        "Denmark": "dk",
        "Finland": "fi",
        "France": "fr",
        "Germany": "de",
        "Hong Kong": "hk",
        "Ireland": "ie",
        "Italy": "it",
        "Luxembourg": "lu",
        "Malaysia": "my",
        "Netherlands": "nl",
        "Norway": "no",
        "Nordics": "nord",
        "Portugal": "pt",
        "Singapore": "sg",
        "Spain": "es",
        "Sweden": "se",
        "Switzerland": "ch",
        "Taiwan": "tw",
        "Thailand": "th",
        "United Kingdom": "gb",
        "United States": "us",
    }
    
    EDITION_ID = {
        "Asia": "en-ea",
        "Benelux": "nl",
        "Canada English": "en-ca",
        "Canada French": "fr-ca",
        "Central Europe": "en-eu",
        "France": "fr",
        "Germany": "de",
        "Italy": "it",
        "Japan": "ja",
        "Nordics": "en-nd",
        "Spain": "es",
        "Sweden": "sv",
        "United Kingdom": "en-gb"
    }
        
    ENGLISH_ENDPOINT = {
        "economy":"sections/economy",
        "personal-finance":"sections/personal-finance",
        "sustainable-investing":"sections/sustainable-investing",
        "bonds":"sections/bonds",
        "etfs":"sections/etfs",
        "funds":"sections/funds",
        "stocks":"sections/stocks",
        "markets":"sections/markets"
    }
    
    FRENCH_ENDPOINT = {
        "economy":"sections/economie",
        "personal-finance":"sections/finances-personnelles",
        "sustainable-investing":"sections/investissement-durable",
        "bonds":"sections/obligations",
        "etfs":"sections/etf",
        "funds":"sections/fonds",
        "stocks":"sections/actions",
        "markets":"sections/marches"
    }
    
    ENDPOINT = {
        "Asia": ENGLISH_ENDPOINT,
        "Benelux": ENGLISH_ENDPOINT,
        "Canada English": ENGLISH_ENDPOINT,
        "Canada French": FRENCH_ENDPOINT,
        "Central Europe": ENGLISH_ENDPOINT,
        "France": FRENCH_ENDPOINT,
        "Germany": ENGLISH_ENDPOINT,
        "Italy": ENGLISH_ENDPOINT,
        "Japan": ENGLISH_ENDPOINT,
        "Nordics": ENGLISH_ENDPOINT,
        "Spain": ENGLISH_ENDPOINT,
        "Sweden": ENGLISH_ENDPOINT,
        "United Kingdom": ENGLISH_ENDPOINT
    }
