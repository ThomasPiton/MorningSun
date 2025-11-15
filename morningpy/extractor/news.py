import asyncio
import pandas as pd

from morningpy.core.client import BaseClient
from morningpy.core.auth import AuthType
from morningpy.core.base_extract import BaseExtractor
from config import URLS,PARAMS

class MarketMoversExtractor(BaseExtractor):
    
    config = MarketMoversConfig
    schema = MarketMoversSchema
    
    def __init__(self, mover_type: Union[str, List[str]] = "gainers"):
     
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.mover_type = mover_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS       

    def _check_inputs(self) -> None:
        """Ensure mover_type is valid and converted to a list."""
        if isinstance(self.mover_type, str):
            if self.mover_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid mover_type '{self.mover_type}', must be one of {self.valid_inputs}"
                )
            self.mover_type = [self.mover_type]
        elif isinstance(self.mover_type, list):
            invalid = [m for m in self.mover_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid mover_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("mover_type must be a str or list[str]")

    def _build_request(self) -> None:
        pass

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar Market Movers response based on selected mover_type."""
        if not response:
            return pd.DataFrame()

        all_rows = []

        for m_type in self.mover_type:
            data = response.get(m_type, [])
            if not data:
                continue

            rows = []
            for item in data:
                row = {}

                for key, value_dict in item.items():
                    if not isinstance(value_dict, dict) or "value" not in value_dict:
                        continue
                    
                    if key not in row:
                        row[key] = value_dict.get("value")

                    props = value_dict.get("properties", {})
                    for prop_key, prop_value in props.items():
                        col_name = f"{key}_{prop_key}"
                        if any(x in prop_key.lower() for x in ["date", "currency"]):
                            continue
                        if col_name not in row:  # avoid duplicates
                            row[col_name] = prop_value.get("value")

                rows.append(row)

            df = pd.DataFrame(rows)
            if df.empty:
                continue

            df["updated_on"] = response.get("updatedOn")
            df["category"] = m_type
            all_rows.append(df)

        if not all_rows:
            return pd.DataFrame()

        df = pd.concat(all_rows, ignore_index=True)
        
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df = df.sort_values("percent_net_change", ascending=False).reset_index(drop=True)

        return df

class StoriesCanadaExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE  
    PAGE_URL = "https://www.morningstar.ca/ca/news/stories.aspx" 
    BASE_URL = URLS.get("stories_canada", "")
    BASE_PARAMS = PARAMS.get("stories_canada", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass  # No specific validation needed
    
    def _build_request(self) -> None:
        pass  # Uses default BASE_PARAMS
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        # Your existing processing logic
        return
    
    
class NewsCanadaBondExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/bond.aspx"
    BASE_URL = URLS.get("news_canada_bond", "")
    BASE_PARAMS = PARAMS.get("news_canada_bond", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return 


class NewsCanadaEtfExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/etf.aspx"
    BASE_URL = URLS.get("news_canada_etf", "")
    BASE_PARAMS = PARAMS.get("news_canada_etf", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsCanadaFundExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/fund.aspx"
    BASE_URL = URLS.get("news_canada_fund", "")
    BASE_PARAMS = PARAMS.get("news_canada_fund", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return
    

class NewsCanadaStockExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/stock.aspx"
    BASE_URL = URLS.get("news_canada_stock", "")
    BASE_PARAMS = PARAMS.get("news_canada_stock", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsCanadaMarketExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/market.aspx"
    BASE_URL = URLS.get("news_canada_market", "")
    BASE_PARAMS = PARAMS.get("news_canada_market", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsCanadaSuistainableExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/sustainable.aspx"
    BASE_URL = URLS.get("news_canada_sustainable", "")
    BASE_PARAMS = PARAMS.get("news_canada_sustainable", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsCanadaPersonalFinanceExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/personal-finance.aspx"
    BASE_URL = URLS.get("news_canada_personal_finance", "")
    BASE_PARAMS = PARAMS.get("news_canada_personal_finance", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsCanadaEconomyExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.ca/ca/news/economy.aspx"
    BASE_URL = URLS.get("news_canada_economy", "")
    BASE_PARAMS = PARAMS.get("news_canada_economy", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsAlternativeInvestmentsExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/alternative-investments"
    BASE_URL = URLS.get("news_us_alternative_investments", "")
    BASE_PARAMS = PARAMS.get("news_us_alternative_investments", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsFinancialAdvisorsExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/financial-advisors"
    BASE_URL = URLS.get("news_us_financial_advisors", "")
    BASE_PARAMS = PARAMS.get("news_us_financial_advisors", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsRetirementsExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/retirement"
    BASE_URL = URLS.get("news_us_retirements", "")
    BASE_PARAMS = PARAMS.get("news_us_retirements", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsPortfoliosExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/portfolios"
    BASE_URL = URLS.get("news_us_portfolios", "")
    BASE_PARAMS = PARAMS.get("news_us_portfolios", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsEconomyExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/economy"
    BASE_URL = URLS.get("news_us_economy", "")
    BASE_PARAMS = PARAMS.get("news_us_economy", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return


class NewsUsSustainableInvestingExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/sustainable-investing"
    BASE_URL = URLS.get("news_us_sustainable_investing", "")
    BASE_PARAMS = PARAMS.get("news_us_sustainable_investing", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        # Your existing processing logic
        return


class NewsUsPersonalFinanceExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.NONE
    PAGE_URL = "https://www.morningstar.com/personal-finance"
    BASE_URL = URLS.get("news_us_personal_finance", "")
    BASE_PARAMS = PARAMS.get("news_us_personal_finance", {})
    
    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = self.BASE_PARAMS
    
    def _check_inputs(self) -> None:
        pass
    
    def _build_request(self) -> None:
        pass
    
    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        return