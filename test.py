from mitmproxy import http
import subprocess
from selenium import webdriver
import time

TARGET_URL = "https://api-global.morningstar.com/sal-service/v1/stock/header/v2/data/0P000000B7/securityInfo"

def response(flow: http.HTTPFlow) -> None:
    if TARGET_URL in flow.request.url:
        token = flow.request.query.get("access_token")
        if token:
            print(f"Access Token: {token}")

def run_mitmproxy():
    process = subprocess.Popen(
        ['mitmdump', '-s', 'extract_token.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return process

def navigate_to_url(url):
    driver = webdriver.Chrome()
    driver.get(url)
    # Wait for the page to load and network requests to be made
    time.sleep(10)  # Adjust sleep time as needed
    driver.quit()

if __name__ == "__main__":
    target_url = "https://www.morningstar.com.au"  # Change to the page where the request is made
    mitmproxy_process = run_mitmproxy()
    try:
        navigate_to_url(target_url)
    finally:
        mitmproxy_process.terminate()
        stdout, stderr = mitmproxy_process.communicate()
        print(stdout.decode())
        print(stderr.decode())