from datetime import datetime
import pandas as pd

from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.news import *
from morningpy.schema.news import *


class HeadlineNewsExtractor(BaseExtractor):
    
    config = HeadlineNewsConfig
    schema = HeadlineNewsSchema
    
    def __init__(self, edition, market, news):
     
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)

        super().__init__(client)

        self.edition = edition
        self.market = market
        self.news = news
        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.market_id = self.config.MARKET_ID
        self.edition_id = self.config.EDITION_ID
        self.endpoint_mapping = self.config.ENDPOINT
        
    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        edition_id = self.edition_id[self.edition]
        market_id = self.market_id[self.market]
        endpoint = self.endpoint_mapping[self.edition][self.news]
        self.url = f"{self.url}/{edition_id}/{endpoint}"
        self.params["marketID"]=market_id
        self.params["sectionFallBack"]=self.news
        
    def _process_response(self,response: dict) -> pd.DataFrame:
        """
        Process the response dict from Morningstar news API
        and return a structured DataFrame.

        Parameters
        ----------
        response : dict
            The raw response dictionary containing news stories.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns:
            ['news_type', 'Country', 'display_date', 'title', 'subtitle', 'Tags', 'link', 'language']
        """
        
        def format_display_date(date_str: str) -> str:
            cleaned = date_str.replace("Z", "")
            try:
                dt = datetime.fromisoformat(cleaned)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return date_str
            
        if not isinstance(response, dict) or not response:
            return pd.DataFrame(columns=['news_type', 'Country', 'display_date', 'title', 'subtitle', 'Tags', 'link', 'language'])
        
        stories = response.get('page', {}).get('stories', [])
        rows = []

        for story in stories:
            news_type = response.get('page', {}).get('title', '')  # e.g. 'Economie'
            display_date = format_display_date(story.get('displayDate', ''))
            title = story.get('headline', {}).get('title', '')
            subtitle = story.get('headline', {}).get('subtitle', '')
            tags = ','.join([section.get('name', '') for section in story.get('tags', [])])
            link = f"https://global.morningstar.com{story.get('canonicalURL', '')}"
            language = story.get('language', '')

            rows.append({
                'news_type': news_type,
                'market': self.market,
                'display_date': display_date,
                'title': title,
                'subtitle': subtitle,
                'tags': tags,
                'link': link,
                'language': language
            })

        df = pd.DataFrame(rows)
        return df
    
# class StoriesCanadaExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE  
#     PAGE_URL = "https://www.morningstar.ca/ca/news/stories.aspx" 
#     BASE_URL = URLS.get("stories_canada", "")
#     BASE_PARAMS = PARAMS.get("stories_canada", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass  # No specific validation needed
    
#     def _build_request(self) -> None:
#         pass  # Uses default BASE_PARAMS
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         # Your existing processing logic
#         return
    
    
# class NewsCanadaBondExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/bond.aspx"
#     BASE_URL = URLS.get("news_canada_bond", "")
#     BASE_PARAMS = PARAMS.get("news_canada_bond", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return 


# class NewsCanadaEtfExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/etf.aspx"
#     BASE_URL = URLS.get("news_canada_etf", "")
#     BASE_PARAMS = PARAMS.get("news_canada_etf", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsCanadaFundExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/fund.aspx"
#     BASE_URL = URLS.get("news_canada_fund", "")
#     BASE_PARAMS = PARAMS.get("news_canada_fund", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return
    

# class NewsCanadaStockExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/stock.aspx"
#     BASE_URL = URLS.get("news_canada_stock", "")
#     BASE_PARAMS = PARAMS.get("news_canada_stock", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsCanadaMarketExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/market.aspx"
#     BASE_URL = URLS.get("news_canada_market", "")
#     BASE_PARAMS = PARAMS.get("news_canada_market", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsCanadaSuistainableExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/sustainable.aspx"
#     BASE_URL = URLS.get("news_canada_sustainable", "")
#     BASE_PARAMS = PARAMS.get("news_canada_sustainable", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsCanadaPersonalFinanceExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/personal-finance.aspx"
#     BASE_URL = URLS.get("news_canada_personal_finance", "")
#     BASE_PARAMS = PARAMS.get("news_canada_personal_finance", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsCanadaEconomyExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.ca/ca/news/economy.aspx"
#     BASE_URL = URLS.get("news_canada_economy", "")
#     BASE_PARAMS = PARAMS.get("news_canada_economy", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsAlternativeInvestmentsExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/alternative-investments"
#     BASE_URL = URLS.get("news_us_alternative_investments", "")
#     BASE_PARAMS = PARAMS.get("news_us_alternative_investments", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsFinancialAdvisorsExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/financial-advisors"
#     BASE_URL = URLS.get("news_us_financial_advisors", "")
#     BASE_PARAMS = PARAMS.get("news_us_financial_advisors", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsRetirementsExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/retirement"
#     BASE_URL = URLS.get("news_us_retirements", "")
#     BASE_PARAMS = PARAMS.get("news_us_retirements", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsPortfoliosExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/portfolios"
#     BASE_URL = URLS.get("news_us_portfolios", "")
#     BASE_PARAMS = PARAMS.get("news_us_portfolios", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsEconomyExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/economy"
#     BASE_URL = URLS.get("news_us_economy", "")
#     BASE_PARAMS = PARAMS.get("news_us_economy", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return


# class NewsUsSustainableInvestingExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/sustainable-investing"
#     BASE_URL = URLS.get("news_us_sustainable_investing", "")
#     BASE_PARAMS = PARAMS.get("news_us_sustainable_investing", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         # Your existing processing logic
#         return


# class NewsUsPersonalFinanceExtractor(BaseExtractor):
#     REQUIRED_AUTH: AuthType = AuthType.NONE
#     PAGE_URL = "https://www.morningstar.com/personal-finance"
#     BASE_URL = URLS.get("news_us_personal_finance", "")
#     BASE_PARAMS = PARAMS.get("news_us_personal_finance", {})
    
#     def __init__(self, client: BaseClient = None):
#         client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
#         super().__init__(client)
#         self.url = self.BASE_URL
#         self.params = self.BASE_PARAMS
    
#     def _check_inputs(self) -> None:
#         pass
    
#     def _build_request(self) -> None:
#         pass
    
#     def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
#         return