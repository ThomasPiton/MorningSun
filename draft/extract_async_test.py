import asyncio
import aiohttp
import pandas as pd
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from typing import Optional, Tuple, Dict, List
from tqdm.asyncio import tqdm as async_tqdm
import nest_asyncio

# Allow nested event loops (useful for Jupyter notebooks)
nest_asyncio.apply()


class MorningstarExtractor:
    """
    Async Morningstar data extractor with intelligent query splitting.
    Uses asyncio for concurrent requests without cookie refresh issues.
    """
    
    # List of all sectors
    SECTORS = [
        "Consumer Cyclical",
        "Consumer Defensive",
        "Real Estate",
        "Basic Materials",
        "Communication Services",
        "Financial Services",
        "Utilities",
        "Healthcare",
        "Technology",
        "Industrials",
        "Energy"
    ]
    
    # List of all exchange countries
    COUNTRIES = [
        'CAN', 'ZWE', 'ZMB', 'ZAF', 'VNM', 'VEN', 'USA', 'URY', 'UKR', 'TZA', 
        'TUN', 'TUR', 'TWN', 'TTO', 'THA', 'SWZ', 'SWE', 'SVN', 'SVK', 'SRB', 
        'SGP', 'ARE', 'ARG', 'ARM', 'AUS', 'AUT', 'BEL', 'BGD', 'BGR', 'BHR', 
        'BIH', 'BMU', 'BOL', 'BRA', 'BWA', 'CHE', 'CHN', 'CHL', 'COL', 'CIV', 
        'CZE', 'CYP', 'FRA', 'FIN', 'EST', 'ESP', 'EGY', 'ECU', 'DNK', 'DEU', 
        'SAU', 'RUS', 'ROU', 'QAT', 'PSE', 'PRT', 'POL', 'PHL', 'PER', 'PAN', 
        'PAK', 'OMN', 'NZL', 'NPL', 'NOR', 'NLD', 'NGA', 'NAM', 'MYS', 'MWI', 
        'MUS', 'MNE', 'MLT', 'MKD', 'MEX', 'MAR', 'LVA', 'LTU', 'LUX', 'LKA', 
        'LBN', 'KWT', 'KOR', 'KEN', 'KAZ', 'JPN', 'JOR', 'JAM', 'ITA', 'ISR', 
        'ISL', 'IRQ', 'IRN', 'IRL', 'IND', 'IDN', 'HUN', 'HRV', 'HKG', 'GRC', 
        'GHA', 'GBR'
    ]
    
    # Common fields for all investment types
    FIELDS = [
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
    
    def __init__(self, max_concurrent_requests: int = 10):
        """
        Initialize the extractor.
        
        Args:
            max_concurrent_requests: Maximum number of concurrent requests
        """
        self._cookies = None
        self._newrelic = None
        self._api_key = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._session = None
    
    def get_cookies(self, url: str = "https://global.morningstar.com/en-ea/tools/screener/stocks") -> Tuple[str, Optional[str]]:
        """
        Extract cookies and tokens using Selenium driver (one-time only).
        
        Args:
            url: Base URL to visit for token generation
            
        Returns:
            tuple: (cookies_string, newrelic_token)
        """
        if self._cookies:
            return self._cookies, self._newrelic
        
        print("🔄 Getting cookies (one-time setup)...")
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
        
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(3)
            
            cookies = driver.get_cookies()
            cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            
            self._cookies = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
            
            for name, value in cookies_dict.items():
                if 'mbuddy' in name.lower() or 'newrelic' in name.lower():
                    self._newrelic = value
                    break
            
            print("✅ Cookies obtained successfully")
            return self._cookies, self._newrelic
            
        finally:
            if driver:
                driver.quit()
    
    def fetch_api_key(self) -> str:
        """Fetch Morningstar API key from their script (one-time only)."""
        if self._api_key:
            return self._api_key
            
        script_url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"
        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }

        import requests
        resp = requests.get(script_url, headers=headers, timeout=15)
        resp.raise_for_status()

        match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', resp.text)
        if not match:
            raise ValueError("❌ API key not found in script.")
        
        self._api_key = match.group(1)
        return self._api_key

    async def fetch_page(self, session: aiohttp.ClientSession, base_url: str, 
                        headers: Dict, params: Dict, page: int, label: str) -> List[Dict]:
        """
        Fetch one page of results asynchronously.
        
        Args:
            session: aiohttp session
            base_url: API endpoint URL
            headers: Request headers
            params: Query parameters
            page: Page number
            label: Label for logging
            
        Returns:
            List of result dictionaries
        """
        params["page"] = page
        
        async with self._semaphore:
            try:
                await asyncio.sleep(0.1)  # Small delay to avoid overwhelming server
                
                async with session.get(base_url, headers=headers, params=params, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results = []
                        for result in data.get("results", []):
                            meta = result.get("meta", {})
                            fields = {k: v.get("value") for k, v in result.get("fields", {}).items()}
                            results.append({**meta, **fields})
                        return results
                    else:
                        print(f"⚠️ {label} page {page} failed ({resp.status})")
                        return []
                        
            except asyncio.TimeoutError:
                print(f"⚠️ {label} page {page} timeout")
                return []
            except Exception as e:
                print(f"⚠️ {label} page {page} error: {e}")
                return []

    async def fetch_sector(self, session: aiohttp.ClientSession, base_url: str, 
                          headers: Dict, inv_code: str, inv_label: str, 
                          country_code: str, sector: str, asset_type: str) -> List[Dict]:
        """
        Fetch data for a specific sector within a country asynchronously.
        
        Args:
            session: aiohttp session
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code
            inv_label: Investment type label
            country_code: Country code
            sector: Sector name
            asset_type: Asset type (stock, etf, fund)
            
        Returns:
            List of result dictionaries
        """
        query = f"(((investmentType = '{inv_code}'))) AND (exchangeCountry = '{country_code}') AND (sector = '{sector}')"
        
        params = {
            "query": query,
            "fields": ",".join(self.FIELDS),
            "page": 1,
            "limit": 500,
            "sort": "name:asc",
        }

        try:
            async with self._semaphore:
                async with session.get(base_url, headers=headers, params=params, timeout=30) as resp:
                    if resp.status != 200:
                        return []
                    
                    data = await resp.json()
                    total_pages = data.get("pages", 1)
                    total_results = data.get("count", 0)
                    first_results = data.get("results", [])
                    
                    if total_results == 0:
                        return []
                    
                    print(f"    🔹 {sector}: {total_results} securities, {total_pages} pages")

                    all_results = []
                    
                    # Process first page
                    for result in first_results:
                        meta = result.get("meta", {})
                        fields_data = {k: v.get("value") for k, v in result.get("fields", {}).items()}
                        all_results.append({
                            **meta, 
                            **fields_data, 
                            "investment_type": inv_label, 
                            "country_code": country_code,
                            "sector": sector,
                            "asset_type": asset_type
                        })

                    # Fetch remaining pages concurrently
                    if total_pages > 1:
                        tasks = [
                            self.fetch_page(session, base_url, headers, params.copy(), page, 
                                          f"{inv_label}/{country_code}/{sector}")
                            for page in range(2, total_pages + 1)
                        ]
                        
                        page_results = await asyncio.gather(*tasks)
                        
                        for results in page_results:
                            for result in results:
                                result["investment_type"] = inv_label
                                result["country_code"] = country_code
                                result["sector"] = sector if "sector" not in result else result["sector"]
                                result["asset_type"] = asset_type
                            all_results.extend(results)

                    return all_results
                    
        except Exception as e:
            print(f"⚠️ Error fetching sector {sector} for {country_code}: {e}")
            return []

    async def fetch_country(self, session: aiohttp.ClientSession, base_url: str, 
                           headers: Dict, inv_code: str, inv_label: str, 
                           country_code: str, asset_type: str) -> List[Dict]:
        """
        Fetch data for a specific country asynchronously, splitting by sector if needed.
        
        Args:
            session: aiohttp session
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code
            inv_label: Investment type label
            country_code: Country code
            asset_type: Asset type (stock, etf, fund)
            
        Returns:
            List of result dictionaries
        """
        query = f"(((investmentType = '{inv_code}'))) AND (exchangeCountry = '{country_code}')"
        
        params = {
            "query": query,
            "fields": ",".join(self.FIELDS),
            "page": 1,
            "limit": 500,
            "sort": "name:asc",
        }

        try:
            async with self._semaphore:
                async with session.get(base_url, headers=headers, params=params, timeout=30) as resp:
                    if resp.status != 200:
                        return []
                    
                    data = await resp.json()
                    total_pages = data.get("pages", 1)
                    total_results = data.get("count", 0)
                    first_results = data.get("results", [])
                    
                    if total_results == 0:
                        return []
                    
                    # Check if we need to split by sector
                    if total_results > 9500:
                        print(f"  ⚠️ {country_code}: {total_results} securities (>9500) - Splitting by sector...")
                        
                        # Fetch all sectors concurrently
                        tasks = [
                            self.fetch_sector(session, base_url, headers, inv_code, inv_label, country_code, sector, asset_type)
                            for sector in self.SECTORS
                        ]
                        
                        sector_results = await asyncio.gather(*tasks)
                        all_results = []
                        for results in sector_results:
                            all_results.extend(results)
                        
                        print(f"  ✅ {country_code}: {len(all_results)} securities total (sector-split)")
                        return all_results
                    
                    # Normal processing
                    print(f"  📍 {country_code}: {total_results} securities, {total_pages} pages")

                    all_results = []
                    
                    # Process first page
                    for result in first_results:
                        meta = result.get("meta", {})
                        fields_data = {k: v.get("value") for k, v in result.get("fields", {}).items()}
                        all_results.append({
                            **meta, 
                            **fields_data, 
                            "investment_type": inv_label, 
                            "country_code": country_code,
                            "asset_type": asset_type
                        })

                    # Fetch remaining pages concurrently
                    if total_pages > 1:
                        if total_pages > 20:
                            print(f"⚠️ {country_code} has {total_pages} pages (>10k limit), limiting to 20 pages")
                            total_pages = 20
                        
                        tasks = [
                            self.fetch_page(session, base_url, headers, params.copy(), page, 
                                          f"{inv_label}/{country_code}")
                            for page in range(2, total_pages + 1)
                        ]
                        
                        page_results = await asyncio.gather(*tasks)
                        
                        for results in page_results:
                            for result in results:
                                result["investment_type"] = inv_label
                                result["country_code"] = country_code
                                result["asset_type"] = asset_type
                            all_results.extend(results)

                    return all_results
                    
        except Exception as e:
            print(f"⚠️ Error fetching {country_code}: {e}")
            return []

    async def fetch_investment_type(self, session: aiohttp.ClientSession, base_url: str, 
                                   headers: Dict, inv_code: str, inv_label: str, asset_type: str) -> List[Dict]:
        """
        Fetch all data for a specific investment type asynchronously.
        
        Args:
            session: aiohttp session
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code
            inv_label: Investment type label
            asset_type: Asset type (stock, etf, fund)
            
        Returns:
            List of result dictionaries
        """
        print(f"\n🌍 {inv_label}: Fetching data from {len(self.COUNTRIES)} countries...")
        
        # Fetch all countries concurrently
        tasks = [
            self.fetch_country(session, base_url, headers, inv_code, inv_label, country, asset_type)
            for country in self.COUNTRIES
        ]
        
        country_results = await asyncio.gather(*tasks)
        
        all_results = []
        countries_with_data = 0
        
        for results in country_results:
            if results:
                all_results.extend(results)
                countries_with_data += 1
        
        print(f"✅ {inv_label}: {len(all_results)} securities from {countries_with_data}/{len(self.COUNTRIES)} countries")
        return all_results

    async def extract_async(self, use_api_key: bool = False, region: str = "en-ea") -> List[Dict]:
        """
        Async extraction function for all investment types.
        
        Args:
            use_api_key: If True, uses API key method. If False, uses cookies method.
            region: Region code for API endpoint
            
        Returns:
            List of result dictionaries
        """
        base_url = f"https://global.morningstar.com/api/v1/{region}/tools/screener/_data"
        
        url_cookie_map = {
            "FE": "https://global.morningstar.com/en-ea/tools/screener/etfs",
            "EQ": "https://global.morningstar.com/en-ea/tools/screener/stocks",
            "FO": "https://global.morningstar.com/en-ea/tools/screener/funds"
        }
        
        investment_labels = {
            "EQ": "Stocks", 
            "FE": "ETFs", 
            "FO": "Mutual Funds"
        }
        
        # Asset type mapping for the asset_type column
        asset_type_mapping = {
            "EQ": "stock", 
            "FE": "etf", 
            "FO": "fund"
        }
        
        # Prepare headers
        if use_api_key:
            print("🔑 Using API key authentication...")
            api_key = self.fetch_api_key()
            headers = {
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "apikey": api_key,
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            }
            print(f"✅ API key obtained: {api_key[:20]}...")
        else:
            print("🍪 Using cookie authentication...")
            cookies, newrelic_token = self.get_cookies(url_cookie_map["EQ"])
            headers = {
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "cookie": cookies,
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            }
            if newrelic_token:
                headers["newrelic"] = newrelic_token
            print("✅ Cookies obtained")

        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Fetch all investment types concurrently
            tasks = [
                self.fetch_investment_type(session, base_url, headers, inv_code, inv_label, asset_type_mapping[inv_code])
                for inv_code, inv_label in investment_labels.items()
            ]
            
            results = await asyncio.gather(*tasks)
            
            all_results = []
            for data in results:
                all_results.extend(data)
            
            return all_results

    def extract(self, use_api_key: bool = False, region: str = "en-ea", 
                output_file: str = None) -> pd.DataFrame:
        """
        Main extraction function for all investment types.
        
        Args:
            use_api_key: If True, uses API key method. If False, uses cookies method.
            region: Region code for API endpoint (default: 'en-ea')
            output_file: Optional CSV filename to save results
            
        Returns:
            pandas.DataFrame: Extracted data for all investment types
        """
        print("=" * 80)
        print("MORNINGSTAR ASYNC DATA EXTRACTION")
        print("=" * 80)
        
        start_time = time.time()
        
        # Run async extraction
        all_results = asyncio.run(self.extract_async(use_api_key, region))

        if not all_results:
            raise ValueError("❌ No data extracted from any investment type.")

        # Create DataFrame
        df = pd.DataFrame(all_results)
        
        # Rename columns for consistency
        rename_map = {
            "name": "security_label",
            "securityId": "security_id",
            "performanceId": "performance_id",
            "companyId": "company_id",
            "stockStyleBox": "stock_style_box",
            "marketCap": "market_cap",
        }
        df.rename(columns=rename_map, inplace=True)

        elapsed_time = time.time() - start_time
        print(f"\n✅ Total extracted: {len(df)} securities")
        print(f"⏱️  Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        
        # Save to CSV if output file specified
        if output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if not output_file.endswith('.csv'):
                output_file = f"{output_file}_{timestamp}.csv"
            else:
                output_file = output_file.replace('.csv', f'_{timestamp}.csv')
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"💾 Data saved to: {output_file}")
        
        return df


if __name__ == "__main__":
    # Create extractor with 10 concurrent requests
    extractor = MorningstarExtractor(max_concurrent_requests=10)
    
    # Extract all investment types and save to CSV
    print("=" * 80)
    print("FULL ASYNC EXTRACTION - All Investment Types")
    print("=" * 80)
    
    df_all = extractor.extract(
        use_api_key=False,  # Use cookies (set to True to use API key)
        region="en-ea",
        output_file="morningstar_data.csv"
    )
    
    if df_all is not None and not df_all.empty:
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"\n📊 Data Preview:")
        print(df_all.head(10))
        print(f"\n📈 Total records: {len(df_all):,}")
        print(f"\n📋 Columns: {len(df_all.columns)}")
        print(f"\n💼 Investment type distribution:")
        print(df_all['investment_type'].value_counts())
        if 'country_code' in df_all.columns:
            print(f"\n🌍 Top 10 countries by securities:")
            print(df_all['country_code'].value_counts().head(10))