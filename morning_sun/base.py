from abc import ABC, abstractmethod
import requests

class Extractor(ABC):
    BASE_URL = "https://www.morningstar.com/api/v2/"

    def __init__(self, ticker, headers=None, payload=None, cookies=None):
        self.ticker = ticker
        self.headers = headers or {}
        self.payload = payload or {}
        self.cookies = cookies or {}
        self.session = requests.Session()

    def get(self):
        url = self.build_url()
        response = self.session.get(url=url, headers=self.headers, params=self.payload, cookies=self.cookies)
        self.__check_valid_response(response)
        return self.extract(response)

    def __check_valid_response(self, response):
        if response.status_code != 200:
            raise Exception(f"Error: {response.status_code} - {response.text}")

    @abstractmethod
    def build_url(self):
        pass

    @abstractmethod
    def extract(self, response):
        pass

    def close_session(self):
        self.session.close()

    def __del__(self):
        self.close_session()
