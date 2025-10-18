import requests
import pandas as pd
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from typing import Optional, Tuple, Dict, List
from tqdm import tqdm


class MorningstarExtractor:
    """
    Morningstar data extractor with intelligent query splitting to avoid API limits.
    Handles 10,000 row limit by splitting queries by country and sector.
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
    
    def __init__(self):
        """Initialize the extractor with cookie and request tracking."""
        self._cookies = None
        self._newrelic = None
        self._api_key = None
        self._request_count = 0
        self._max_requests_before_refresh = 15
        self._last_refresh_time = 0
        self._min_refresh_interval = 5
    
    def get_cookies(self, url: str = "https://global.morningstar.com/en-ea/tools/screener/stocks", 
                    force_refresh: bool = False) -> Tuple[str, Optional[str]]:
        """
        Extract cookies and tokens using Selenium driver.
        
        Args:
            url: Base URL to visit for token generation
            force_refresh: Force refresh even if cookies exist
            
        Returns:
            tuple: (cookies_string, newrelic_token)
        """
        current_time = time.time()
        
        # Return cached cookies if valid
        if not force_refresh and self._cookies and self._request_count < self._max_requests_before_refresh:
            return self._cookies, self._newrelic
        
        # Ensure minimum interval between refreshes
        time_since_last_refresh = current_time - self._last_refresh_time
        if time_since_last_refresh < self._min_refresh_interval:
            wait_time = self._min_refresh_interval - time_since_last_refresh
            print(f"⏳ Waiting {wait_time:.1f}s before refresh...")
            time.sleep(wait_time)
        
        print("🔄 Refreshing cookies and tokens...")
        
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
            
            self._request_count = 0
            self._last_refresh_time = time.time()
            print("✅ Cookies refreshed successfully")
            
            return self._cookies, self._newrelic
            
        finally:
            if driver:
                driver.quit()
    
    def fetch_api_key(self) -> str:
        """Fetch Morningstar API key from their script."""
        if self._api_key:
            return self._api_key
            
        script_url = "https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js"
        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        }

        resp = requests.get(script_url, headers=headers, timeout=15)
        resp.raise_for_status()

        match = re.search(r'keyApigee\s*[:=]\s*"([^"]+)"', resp.text)
        if not match:
            raise ValueError("❌ API key not found in script.")
        
        self._api_key = match.group(1)
        return self._api_key

    def fetch_page(self, base_url: str, headers: Dict, params: Dict, 
                   page: int, label: str, max_retries: int = 3) -> List[Dict]:
        """
        Fetch one page of results with retry logic.
        
        Args:
            base_url: API endpoint URL
            headers: Request headers
            params: Query parameters
            page: Page number
            label: Label for logging (e.g., "Stocks/USA")
            max_retries: Maximum retry attempts
            
        Returns:
            List of result dictionaries
        """
        params["page"] = page
        
        # Proactive cookie refresh
        if self._request_count >= self._max_requests_before_refresh:
            print(f"🔄 Request limit reached ({self._request_count}), refreshing cookies...")
            self._refresh_headers(headers, label)
            time.sleep(2)
        
        # Retry logic
        for attempt in range(max_retries):
            try:
                if self._request_count > 0:
                    time.sleep(0.5)
                
                resp = requests.get(base_url, headers=headers, params=params, timeout=30)
                self._request_count += 1
                
                if resp.status_code == 200:
                    data = resp.json()
                    results = []
                    for result in data.get("results", []):
                        meta = result.get("meta", {})
                        fields = {k: v.get("value") for k, v in result.get("fields", {}).items()}
                        results.append({**meta, **fields})
                    return results
                
                elif resp.status_code == 400:
                    print(f"⚠️ Page {page} failed (400) - Attempt {attempt + 1}/{max_retries}")
                    
                    if attempt < max_retries - 1:
                        print(f"🔄 Forcing cookie refresh...")
                        self._refresh_headers(headers, label)
                        
                        wait_time = 3 + (attempt * 2)
                        print(f"⏳ Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ Max retries reached for page {page}")
                        return []
                else:
                    print(f"⚠️ Page {page} failed ({resp.status_code})")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                    else:
                        return []
                        
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Error on page {page}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return []
        
        return []
    
    def _refresh_headers(self, headers: Dict, label: str):
        """Helper method to refresh cookies in headers."""
        url_map = {
            "Stocks": "https://global.morningstar.com/en-ea/tools/screener/stocks",
            "ETFs": "https://global.morningstar.com/en-ea/tools/screener/etfs",
            "Mutual Funds": "https://global.morningstar.com/en-ea/tools/screener/funds"
        }
        
        inv_type = label.split('/')[0] if '/' in label else label
        cookies, newrelic_token = self.get_cookies(url_map.get(inv_type, url_map["Stocks"]), force_refresh=True)
        
        headers["cookie"] = cookies
        if newrelic_token:
            headers["newrelic"] = newrelic_token

    def fetch_sector(self, base_url: str, headers: Dict, inv_code: str, 
                    inv_label: str, country_code: str, sector: str) -> List[Dict]:
        """
        Fetch data for a specific sector within a country.
        
        Args:
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code (EQ, FE, FO)
            inv_label: Investment type label (Stocks, ETFs, Mutual Funds)
            country_code: Country code (e.g., USA)
            sector: Sector name
            
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
            resp = requests.get(base_url, headers=headers, params=params, timeout=30)
            self._request_count += 1
            
            if resp.status_code != 200:
                return []
            
            data = resp.json()
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
                    "sector": sector
                })

            # Process remaining pages
            for page in range(2, total_pages + 1):
                page_results = self.fetch_page(base_url, headers, params.copy(), page, f"{inv_label}/{country_code}/{sector}")
                for result in page_results:
                    result["investment_type"] = inv_label
                    result["country_code"] = country_code
                    result["sector"] = sector if "sector" not in result else result["sector"]
                all_results.extend(page_results)

            return all_results
            
        except Exception as e:
            print(f"⚠️ Error fetching sector {sector} for {country_code}: {e}")
            return []

    def fetch_country(self, base_url: str, headers: Dict, inv_code: str, 
                     inv_label: str, country_code: str) -> List[Dict]:
        """
        Fetch data for a specific country, splitting by sector if needed.
        
        Args:
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code (EQ, FE, FO)
            inv_label: Investment type label (Stocks, ETFs, Mutual Funds)
            country_code: Country code (e.g., USA)
            
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
            # First request to check total results
            resp = requests.get(base_url, headers=headers, params=params, timeout=30)
            self._request_count += 1
            
            if resp.status_code != 200:
                print(f"⚠️ Country {country_code} failed ({resp.status_code})")
                return []
            
            data = resp.json()
            total_pages = data.get("pages", 1)
            total_results = data.get("count", 0)
            first_results = data.get("results", [])
            
            if total_results == 0:
                return []
            
            # Check if we need to split by sector (approaching 10k limit)
            if total_results > 9500:
                print(f"  ⚠️ {country_code}: {total_results} securities (>9500) - Splitting by sector...")
                all_results = []
                
                for sector in self.SECTORS:
                    sector_results = self.fetch_sector(base_url, headers, inv_code, inv_label, country_code, sector)
                    all_results.extend(sector_results)
                
                print(f"  ✅ {country_code}: {len(all_results)} securities total (sector-split)")
                return all_results
            
            # Normal processing (under limit)
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
                    "country_code": country_code
                })

            # Process remaining pages
            if total_pages > 20:
                print(f"⚠️ {country_code} has {total_pages} pages (>10k limit)")
                return all_results
            
            for page in range(2, total_pages + 1):
                page_results = self.fetch_page(base_url, headers, params.copy(), page, f"{inv_label}/{country_code}")
                for result in page_results:
                    result["investment_type"] = inv_label
                    result["country_code"] = country_code
                all_results.extend(page_results)

            return all_results
            
        except Exception as e:
            print(f"⚠️ Error fetching {country_code}: {e}")
            return []

    def fetch_investment_type(self, base_url: str, headers: Dict, inv_code: str, 
                             inv_label: str) -> List[Dict]:
        """
        Fetch all data for a specific investment type by looping through countries.
        
        Args:
            base_url: API endpoint URL
            headers: Request headers
            inv_code: Investment type code (EQ, FE, FO)
            inv_label: Investment type label (Stocks, ETFs, Mutual Funds)
            
        Returns:
            List of result dictionaries
        """
        print(f"\n🌍 {inv_label}: Fetching data from {len(self.COUNTRIES)} countries...")
        
        all_results = []
        countries_with_data = 0
        
        for country in tqdm(self.COUNTRIES, desc=f"Countries for {inv_label}"):
            country_results = self.fetch_country(base_url, headers, inv_code, inv_label, country)
            if country_results:
                all_results.extend(country_results)
                countries_with_data += 1
        
        print(f"✅ {inv_label}: {len(all_results)} securities from {countries_with_data}/{len(self.COUNTRIES)} countries")
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
        print("MORNINGSTAR DATA EXTRACTION")
        print("=" * 80)
        
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

        # Fetch data for all investment types
        all_results = []
        for inv_code, inv_label in investment_labels.items():
            try:
                print(f"\n📊 Processing {inv_label}...")
                data = self.fetch_investment_type(base_url, headers, inv_code, inv_label)
                all_results.extend(data)
                print(f"✅ {inv_label}: {len(data)} securities extracted")
            except Exception as e:
                print(f"⚠️ Error while processing {inv_label}: {e}")

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

        print(f"\n✅ Total extracted: {len(df)} securities from all investment types.")
        
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
    extractor = MorningstarExtractor()
    
    # Extract all investment types and save to CSV
    print("=" * 80)
    print("FULL EXTRACTION - All Investment Types")
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
        print(f"\n🌍 Top 10 countries by securities:")
        print(df_all['country_code'].value_counts().head(10))