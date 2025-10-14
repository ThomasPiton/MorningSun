from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import requests
import time


def get_waf_token(url="https://www.morningstar.com/markets/calendar"):
    """
    Extract AWS WAF token from Morningstar using Selenium
    
    Args:
        url: Base URL to visit for token generation
        
    Returns:
        tuple: (waf_token, cookies_dict) or (None, None) if failed
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36')
    
    driver = None
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script('return document.readyState') == 'complete'
    )
    
    # Method 3: Check for token in cookies
    cookies = driver.get_cookies()
    cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    for name, value in cookies_dict.items():
        if 'waf' in name.lower() or 'token' in name.lower():
             waf_token = value
    
    return waf_token


if __name__ == "__main__":

    waf_token, cookies = get_waf_token()