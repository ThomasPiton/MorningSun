from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json

def get_morningstar_calendar(date, category="earnings"):
    """
    Scrape Morningstar calendar data with AWS WAF bypass
    
    Args:
        date: Date string in format "YYYY-MM-DD"
        category: "earnings" or "economic-releases"
    
    Returns:
        JSON data from the API
    """
    
    # Setup Chrome options
    chrome_options = Options()
    # chrome_options.add_argument('--headless')  # Uncomment for headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # First, visit the main page to get the WAF token
        print("Step 1: Loading main page to acquire WAF token...")
        base_url = f"https://www.morningstar.com/markets/calendar?date={date}&category={category}"
        driver.get(base_url)
        
        # Wait for page to load and WAF challenge to complete
        time.sleep(3)
        
        # Now make the API call through the browser
        print("Step 2: Making API request with WAF token...")
        api_url = f"https://www.morningstar.com/api/v2/markets/calendar?date={date}&category={category}"
        
        # Navigate to API endpoint - browser will automatically include WAF token
        driver.get(api_url)
        
        # Wait for JSON response to load
        time.sleep(2)
        
        # Get the page source (which contains the JSON response)
        page_source = driver.page_source
        
        # Extract JSON from the page
        # The browser displays JSON in a <pre> tag
        if '<pre>' in page_source:
            start = page_source.find('<pre>') + 5
            end = page_source.find('</pre>')
            json_text = page_source[start:end]
        else:
            # Try to get it directly
            json_text = driver.find_element('tag name', 'body').text
        
        # Parse JSON
        data = json.loads(json_text)
        print("Successfully retrieved data!")
        return data
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        print("Page source:", driver.page_source[:500])
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        driver.quit()


def get_morningstar_with_session(date, category="earnings"):
    """
    Alternative method: Load page first, extract cookies, then use requests
    """
    import requests
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Load the main page to get cookies and WAF token
        print("Loading page to acquire session and WAF token...")
        base_url = f"https://www.morningstar.com/markets/calendar?date={date}&category={category}"
        driver.get(base_url)
        time.sleep(3)
        
        # Extract all cookies
        cookies = driver.get_cookies()
        session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
        
        # Get localStorage and sessionStorage if WAF token is stored there
        waf_token = driver.execute_script("return localStorage.getItem('aws-waf-token');")
        
        driver.quit()
        
        # Now use requests with the cookies
        print("Making API request with extracted cookies...")
        session = requests.Session()
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "referer": base_url,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        if waf_token:
            headers["x-aws-waf-token"] = waf_token
        
        api_url = "https://www.morningstar.com/api/v2/markets/calendar"
        params = {"date": date, "category": category}
        
        response = session.get(api_url, headers=headers, cookies=session_cookies, params=params)
        
        if response.status_code == 200:
            print("Successfully retrieved data!")
            return response.json()
        else:
            print(f"Status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if driver:
            driver.quit()


# Main execution
if __name__ == "__main__":
    # Method 1: Direct browser navigation (most reliable)
    print("=" * 60)
    print("METHOD 1: Direct Browser Navigation")
    print("=" * 60)
    data = get_morningstar_calendar(date="2025-10-07", category="")
    
    if data:
        print("\nData retrieved successfully!")
        print(f"Number of items: {len(data) if isinstance(data, list) else 'N/A'}")
        print("\nFirst few items:")
        if isinstance(data, dict) and 'items' in data:
            print(json.dumps(data['items'][:2], indent=2))
        elif isinstance(data, list):
            print(json.dumps(data[:2], indent=2))
        else:
            print(json.dumps(data, indent=2)[:500])
    
    print("\n" + "=" * 60)
    print("METHOD 2: Extract Cookies and Use Requests")
    print("=" * 60)
    data2 = get_morningstar_with_session(date="2025-10-08", category="earnings")
    
    if data2:
        print("\nData retrieved successfully!")
        print(f"Number of items: {len(data2) if isinstance(data2, list) else 'N/A'}")