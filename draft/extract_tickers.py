import requests
import re
import pandas as pd
from tqdm import tqdm


def fetch_api_key():
    """Fetch Morningstar API key from their script."""
    script_url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }

    resp = requests.get(script_url, headers=headers, timeout=15)
    resp.raise_for_status()

    match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', resp.text)
    if not match:
        raise ValueError("❌ API key not found in script.")
    return match.group(1)


def fetch_page(base_url, headers, params, page, inv_label):
    """Fetch one page of Morningstar screener results."""
    params["page"] = page
    try:
        resp = requests.get(base_url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"⚠️ Page {page} failed ({resp.status_code}) for {inv_label}")
            return []
        data = resp.json()
        results = []
        for result in data.get("results", []):
            meta = result.get("meta", {})
            fields = {k: v.get("value") for k, v in result.get("fields", {}).items()}
            results.append({**meta, **fields, "investment_type": inv_label})
        return results
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error on page {page} ({inv_label}): {e}")
        return []


def fetch_investment_type(base_url, headers, inv_code, inv_label, fields):
    """Fetch all pages for a specific investment type."""
    query = (
        f"(((investmentType = '{inv_code}'))) AND "
        "(exchangeCountry in ('CAN', 'ZWE', 'ZMB', 'ZAF', 'VNM', 'VEN', 'USA', 'URY', 'UKR', 'TZA', 'TUN', 'TUR', 'TWN', 'TTO', 'THA', 'SWZ', 'SWE', 'SVN', 'SVK', 'SRB', 'SGP', 'ARE', 'ARG', 'ARM', 'AUS', 'AUT', 'BEL', 'BGD', 'BGR', 'BHR', 'BIH', 'BMU', 'BOL', 'BRA', 'BWA', 'CHE', 'CHN', 'CHL', 'COL', 'CIV', 'CZE', 'CYP', 'FRA', 'FIN', 'EST', 'ESP', 'EGY', 'ECU', 'DNK', 'DEU', 'SAU', 'RUS', 'ROU', 'QAT', 'PSE', 'PRT', 'POL', 'PHL', 'PER', 'PAN', 'PAK', 'OMN', 'NZL', 'NPL', 'NOR', 'NLD', 'NGA', 'NAM', 'MYS', 'MWI', 'MUS', 'MNE', 'MLT', 'MKD', 'MEX', 'MAR', 'LVA', 'LTU', 'LUX', 'LKA', 'LBN', 'KWT', 'KOR', 'KEN', 'KAZ', 'JPN', 'JOR', 'JAM', 'ITA', 'ISR', 'ISL', 'IRQ', 'IRN', 'IRL', 'IND', 'IDN', 'HUN', 'HRV', 'HKG', 'GRC', 'GHA', 'GBR'))"
    )
    params = {
        "query": query,
        "fields": ",".join(fields),
        "page": 1,
        "limit": 500,
        "sort": "name:asc",
    }

    # Première requête pour connaître le nombre total de pages
    resp = requests.get(base_url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    total_pages = data.get("pages", 1)
    first_results = data.get("results", [])
    print(f"🔍 {inv_label}: {total_pages} pages found")

    all_results = []
    # Page 1
    for result in first_results:
        meta = result.get("meta", {})
        fields_data = {k: v.get("value") for k, v in result.get("fields", {}).items()}
        all_results.append({**meta, **fields_data, "investment_type": inv_label})

    # Pages suivantes
    for page in tqdm(range(2, total_pages + 1), desc=f"Fetching {inv_label}"):
        page_results = fetch_page(base_url, headers, params.copy(), page, inv_label)
        all_results.extend(page_results)

    return all_results


def extract():
    """Main extraction function."""
    base_url = "https://global.morningstar.com/api/v1/en-ca/tools/screener/_data"

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }

    api_key = fetch_api_key()
    headers["apikey"] = api_key

    # Champs par type d’investissement
    investment_fields = {
        "EQ": [  # Stocks
            "name", "sector", "industry", "stockStyleBox", "marketCap",
            "dividendYield", "dividendDistributionFrequency", "baseCurrency",
        ],
        "FE": [  # ETFs
            "name", "marketCap", "fundSize", "managementExpenseRatio", "baseCurrency",
            "morningstarCategory", "fundEquityStyleBox", "fundFixedIncomeStyleBox",
        ],
        "FO": [  # Mutual Funds
            "name", "fundSize", "managementExpenseRatio", "inceptionDate",
            "fundStarRating[overall]", "morningstarRiskRating", "brandingName",
            "sustainabilityRating", "medalistRating[overall]", "baseCurrency",
        ],
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
    
    investment_labels = {"EQ": "Stocks", "FE": "ETFs", "FO": "Mutual Funds"}

    all_results = []
    for inv_code, inv_label in investment_labels.items():
        try:
            data = fetch_investment_type(
                base_url, headers, inv_code, inv_label, fields
            )
            all_results.extend(data)
        except Exception as e:
            print(f"⚠️ Error while processing {inv_label}: {e}")

    if not all_results:
        raise ValueError("❌ No data extracted from any investment type.")

    df = pd.DataFrame(all_results)
    df.rename(
        columns={
            "name": "security_label",
            "securityId": "security_id",
            "performanceId": "performance_id",
            "companyId": "company_id",
            "stockStyleBox": "stock_style_box",
            "marketCap": "market_cap",
        },
        inplace=True,
    )

    print(f"✅ Extracted {len(df)} securities from all investment types.")
    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())
