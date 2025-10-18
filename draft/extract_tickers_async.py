import asyncio
import aiohttp
import re
import pandas as pd
from tqdm.asyncio import tqdm_asyncio


async def fetch_api_key(session):
    """Fetch Morningstar API key from their script."""
    script_url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"
    async with session.get(script_url) as resp:
        text = await resp.text()
        match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', text)
        if not match:
            raise ValueError("❌ API key not found in script.")
        return match.group(1)


async def fetch_page(session, base_url, headers, params, page, investment_type):
    """Fetch one page of Morningstar screener results."""
    params["page"] = page
    try:
        async with session.get(base_url, headers=headers, params=params, timeout=20) as resp:
            if resp.status != 200:
                print(f"⚠️ Page {page} failed ({resp.status}) for {investment_type}")
                return []
            data = await resp.json()
            results = []
            for result in data.get("results", []):
                meta = result.get("meta", {})
                fields = {key: info.get("value") for key, info in result.get("fields", {}).items()}
                results.append({**meta, **fields, "investment_type": investment_type})
            return results
    except asyncio.TimeoutError:
        print(f"⚠️ Timeout on page {page} for {investment_type}")
        return []
    except Exception as e:
        print(f"⚠️ Error on page {page} ({investment_type}): {e}")
        return []


async def fetch_investment_type(session, base_url, headers, inv_code, inv_label, fields):
    """Fetch all pages for a specific investment type."""
    query = (
        f"(((investmentType = '{inv_code}'))) AND "
        "(exchangeCountry in ('USA','FRA','GBR','DEU','CAN'))"
    )

    params = {
        "query": query,
        "fields": ",".join(fields),
        "page": 1,
        "limit": 500,
        "sort": "name:asc",
    }

    # Première requête pour récupérer le nombre total de pages
    async with session.get(base_url, headers=headers, params=params) as resp:
        data = await resp.json()
        total_pages = data.get("pages", 1)
        first_results = data.get("results", [])
        print(f"🔍 {inv_label}: {total_pages} pages found")

    all_results = []
    for result in first_results:
        meta = result.get("meta", {})
        fields_data = {key: info.get("value") for key, info in result.get("fields", {}).items()}
        all_results.append({**meta, **fields_data, "investment_type": inv_label})

    # Lancement en parallèle des pages restantes
    tasks = [
        fetch_page(session, base_url, headers, params.copy(), page, inv_label)
        for page in range(2, total_pages + 1)
    ]

    for batch in await tqdm_asyncio.gather(*tasks, desc=f"Fetching {inv_label} pages"):
        all_results.extend(batch)

    return all_results


async def extract_async():
    """Extract Morningstar screener data asynchronously for multiple investment types."""
    base_url = "https://global.morningstar.com/api/v1/en-ca/tools/screener/_data"

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }

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

    async with aiohttp.ClientSession() as session:
        api_key = await fetch_api_key(session)
        headers["apikey"] = api_key

        # Lancer toutes les extractions d'investment types en parallèle
        tasks = [
            fetch_investment_type(
                session, base_url, headers, inv_code, inv_label, investment_fields[inv_code]
            )
            for inv_code, inv_label in {"EQ": "Stocks", "FE": "ETFs", "FO": "Mutual Funds"}.items()
        ]

        all_data_nested = await asyncio.gather(*tasks)

    # Fusionner tous les résultats
    all_results = [item for sublist in all_data_nested for item in sublist]

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


def extract():
    """Wrapper to run asyncio version."""
    return asyncio.run(extract_async())


if __name__ == "__main__":
    df = extract()
    print(df.head())