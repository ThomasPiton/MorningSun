import pandas as pd
import aiohttp
import asyncio
from datetime import datetime
from typing import Any, Dict, Optional

from morningsun.core.client import BaseClient
from morningsun.core.base_extract import BaseExtractor
from morningsun.core.auth import AuthType
from morningsun.extractors.config import *

class MarketCalendarUsInfoExtractor(BaseExtractor):
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/calendar"
    BASE_URL = URLS["market_calendar"]
    BASE_PARAMS = PARAMS["market_calendar"]
    VALID_INFO_TYPES = {"earnings", "economic-releases", "ipos", "splits"}

    def __init__(self, date: str | list[str], info_type: str,client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        
        self.date = date
        self.info_type = info_type.lower().strip()
        self.url = self.BASE_URL
        self.params = None

    def _check_inputs(self) -> None:
        if self.info_type not in self.VALID_INFO_TYPES:
            raise ValueError(f"Invalid info_type '{self.info_type}', must be one of {self.VALID_INFO_TYPES}")
        dates = self.date if isinstance(self.date, list) else [self.date]
        for d in dates:
            datetime.strptime(d, "%Y-%m-%d")  # raises ValueError if invalid

    def _build_request(self) -> None:
        if isinstance(self.date, list):
            self.params = [{**self.BASE_PARAMS, "date": d, "category": self.info_type} for d in self.date]
        else:
            self.params = {**self.BASE_PARAMS, "date": self.date, "category": self.info_type}

    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        
        if not response or "page" not in response or "results" not in response["page"]:
            return pd.DataFrame()

        rows = []
        results = response["page"]["results"]

        for result in results:
            details = result.get("details", {})
            securities = result.get("securities", [])
            info_type = (param or {}).get("info_type", getattr(self, "info_type", "")).lower()

            # Base info for all records
            base_info = {
                "calendar_date": result.get("date"),
                "updated_at": result.get("updatedAt"),
                "vendor": result.get("vendor"),
                "type": result.get("type"),
                "calendar": result.get("calendar"),
                "vendor_id": result.get("vendorId"),
                "date_param": (param or {}).get("date", getattr(self, "date", None)),
                "info_type": info_type,
            }

            # Case 1: EARNINGS
            if info_type == "earnings":
                for sec in securities or [{}]:
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker"),
                        "name": sec.get("name"),
                        "exchange": sec.get("exchange"),
                        "market_cap": sec.get("marketCap"),
                        "isin": sec.get("isin"),
                        "exchange_country": sec.get("exchangeCountry"),
                        # details
                        "quarter_end_date": details.get("quarterEndDate"),
                        "actual_diluted_eps": details.get("actualDilutedEps"),
                        "net_income": details.get("netIncome"),
                        "consensus_estimate": details.get("consensusEstimate"),
                        "percentage_surprise": details.get("percentageSurprise"),
                        "quarterly_sales": details.get("quarterlySales"),
                    }
                    rows.append(row)

            # Case 2: ECONOMIC RELEASES
            elif info_type == "economic-releases":
                row = {
                    **base_info,
                    "release": details.get("release"),
                    "period": details.get("period"),
                    "release_time": details.get("releaseTime"),
                    "consensus_estimate": (details.get("consensusEstimate") or {}).get("value"),
                    "briefing_estimate": (details.get("briefingEstimate") or {}).get("value"),
                    "after_release_actual": (details.get("afterReleaseActual") or {}).get("value"),
                    "prior_release_actual": (details.get("priorReleaseActual") or {}).get("value"),
                }
                rows.append(row)

            # Case 3: IPOS
            elif info_type == "ipos":
                for sec in securities or [{}]:
                    company = details.get("company", {})
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker") or details.get("ticker"),
                        "name": sec.get("name") or company.get("name"),
                        "exchange": sec.get("exchange"),
                        "market_cap": sec.get("marketCap"),
                        "share_value": details.get("shareValue"),
                        "opened_share_value": details.get("openedShareValue"),
                        "lead_underwriter": details.get("leadUnderWriter"),
                        "initial_shares": details.get("initialShares"),
                        "initial_low_range": details.get("initialLowRange"),
                        "initial_high_range": details.get("initialHighRange"),
                        "date_priced": details.get("datePriced"),
                        "week_priced": details.get("weekPriced"),
                        "company_description": company.get("description"),
                    }
                    rows.append(row)

            # Case 4: SPLITS
            elif info_type == "splits":
                for sec in securities or [{}]:
                    company = details.get("company", {})
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker") or details.get("ticker"),
                        "name": sec.get("name") or company.get("name"),
                        "exchange": sec.get("exchange"),
                        "market_cap": sec.get("marketCap"),
                        "share_worth": details.get("shareWorth"),
                        "old_share_worth": details.get("oldShareWorth"),
                        "ex_date": details.get("exDate"),
                        "announce_date": details.get("announceDate"),
                        "payable_date": details.get("payableDate"),
                    }
                    rows.append(row)

            # Default fallback
            else:
                row = {**base_info, **details}
                rows.append(row)

        return pd.DataFrame(rows)
    
class MarketFairValueExtractor(BaseExtractor):
    """Extracts Morningstar fair value data for undervaluated or overvaluated stocks."""

    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/fair-value"
    BASE_URL = URLS["market_fair_value"]
    VALID_INFO_TYPES = {
        "undervaluated": "undervaluedStocks",
        "overvaluated": "overvaluedStocks",
    }

    def __init__(self, value_type: str, client: BaseClient = None):
        client = client or BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.value_type = value_type.lower()

    def _check_inputs(self) -> None:
        """Ensure value_type is valid."""
        if self.value_type not in self.VALID_INFO_TYPES:
            raise ValueError(
                f"Invalid value_type '{self.value_type}'. "
                f"Must be one of: {', '.join(self.VALID_INFO_TYPES.keys())}"
            )

    def _build_request(self) -> None:
        """No params for this endpoint."""
        self.params = None

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar fair value data."""
        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        key = self.VALID_INFO_TYPES.get(self.value_type)
        if key not in components:
            return pd.DataFrame()

        payload = components[key].get("payload", {})
        results = payload.get("results", [])
        if not results:
            return pd.DataFrame()

        rows = []

        for item in results:
            fields = item.get("fields", {})
            meta = item.get("meta", {})

            row = {
                "securityID": meta.get("securityID"),
                "performanceID": meta.get("performanceID"),
                "companyID": meta.get("companyID"),
                "exchange": meta.get("exchange"),
                "ticker": meta.get("ticker"),
            }

            for field_key, field_data in fields.items():
                if not isinstance(field_data, dict) or "value" not in field_data:
                    continue

                # main value
                row[field_key] = field_data.get("value")

                # nested properties — skip date and currency
                props = field_data.get("properties", {})
                for prop_key, prop_val in props.items():
                    if "date" in prop_key.lower() or "currency" in prop_key.lower():
                        continue
                    row[f"{field_key}_{prop_key}"] = prop_val.get("value")

            rows.append(row)

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        # Sort by stockStarRating or priceToFairValue if available
        sort_cols = []
        if "stockStarRating" in df.columns:
            sort_cols.append("stockStarRating")
        if "priceToFairValue" in df.columns:
            sort_cols.append("priceToFairValue")

        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=[False, True]).reset_index(drop=True)

        return df
    
class MarketIndexesExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/indexes"
    BASE_URL = URLS["market_indexes"]
    VALID_INFO_TYPES = {"americas", "asia", "europe", "private", "sector", "us", "all"}
    MAPPING_INDEXES = {
        "americas": "americasIndexes",
        "asia": "asiaIndexes",
        "europe": "europeIndexes",
        "private": "privateIndexes",
        "sector": "sectorIndexes",
        "us": "usIndexes",
        "all": None,
    }

    def __init__(self, index_type: str = "all", client: BaseClient = None):
        client = client or BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.index_type = index_type.lower()

    def _check_inputs(self) -> None:
        if self.index_type not in self.VALID_INFO_TYPES:
            raise ValueError(
                f"Invalid index_type '{self.index_type}'. Must be one of: {', '.join(self.VALID_INFO_TYPES)}"
            )

    def _build_request(self) -> None:
        self.params = None

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar index data depending on selected index_type."""

        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        rows = []

        # Select which sections to process
        if self.index_type == "all":
            selected_keys = list(self.MAPPING_INDEXES.values())
            selected_keys.remove(None)  # remove None from 'all'
        else:
            key = self.MAPPING_INDEXES.get(self.index_type)
            selected_keys = [key] if key in components else []

        # Flatten all selected index sections
        for key in selected_keys:
            comp = components.get(key, {})
            payload = comp.get("payload", [])
            if not isinstance(payload, list):
                continue

            for item in payload:
                row = {**item, "category": key.replace("Indexes", "").capitalize()}
                rows.append(row)

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        # Sort results by category then percentNetChange if available
        sort_cols = [col for col in ["category", "percentNetChange"] if col in df.columns]
        df = df.sort_values(by=sort_cols, ascending=[True, False]).reset_index(drop=True)

        return df

class MarketExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets"
    BASE_URL = URLS["market"]
    VALID_INFO_TYPES = {
        "commodities": "commodities",
        "currencies": "currencies",
        "global_barometer": "globalBarometer",
        "global_indexes": "globalIndexes",
        "market_news": "marketNews",
        "sectors": "sectors",
        "topics": "topics",
        "us_barometer": "usBarometer",
        "valuation_chart": "valuationChart"
    }

    def __init__(self, info_type: str = "commodities", client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.info_type = info_type

    def _check_inputs(self) -> None:
        if self.info_type not in self.VALID_INFO_TYPES:
            raise ValueError(f"Invalid info_type '{self.info_type}'. Must be one of {list(self.VALID_INFO_TYPES.keys())}")

    def _build_request(self) -> None:
        # No additional parameters for now
        self.params = None

    def _process_response(self, response: Dict[str, Any]) -> pd.DataFrame:
        """Convert API response to a DataFrame, filling missing fields with None."""
        payload = response.get("components", {}).get(self.VALID_INFO_TYPES[self.info_type], {}).get("payload", [])

        # Normalize into a DataFrame
        if isinstance(payload, list):
            # Flatten nested dicts, fill missing with None
            def flatten(item):
                flat_item = {}
                for k, v in item.items():
                    if isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            if isinstance(sub_v, dict) and "value" in sub_v:
                                flat_item[f"{k}_{sub_k}"] = sub_v.get("value", None)
                            else:
                                flat_item[f"{k}_{sub_k}"] = sub_v
                    else:
                        flat_item[k] = v
                return flat_item

            df = pd.DataFrame([flatten(x) for x in payload])
        elif isinstance(payload, dict):
            # If payload is a dict, create DataFrame with keys as rows
            df = pd.DataFrame([{"key": k, **v} for k, v in payload.items()])
        else:
            df = pd.DataFrame()

        return df
     
class MarketMoversExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/movers"
    BASE_URL = URLS["market_movers"]
    VALID_INFO_TYPES = {"gainers", "losers", "actives"}

    def __init__(self, info_type: str = "gainers", client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.info_type = info_type
        self.url = self.BASE_URL

    def _check_inputs(self) -> None:
        if self.info_type not in {"gainers", "losers", "actives"}:
            raise ValueError("Category must be one of: 'gainers', 'losers', 'active'")

    def _build_request(self) -> None:
        pass

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar Market Movers response based on selected info_type."""

        # Validate and extract the relevant list
        if not response or self.info_type not in response:
            return pd.DataFrame()

        data = response[self.info_type]
        rows = []

        for item in data:
            row = {}

            # iterate over all data points in each item
            for key, value_dict in item.items():
                if not isinstance(value_dict, dict) or "value" not in value_dict:
                    continue

                # main value
                row[key] = value_dict.get("value")

                # extract nested props (skipping date and currency)
                props = value_dict.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" in prop_key.lower() or "currency" in prop_key.lower():
                        continue
                    row[f"{key}_{prop_key}"] = prop_value.get("value")

            rows.append(row)

        # Build DataFrame
        df = pd.DataFrame(rows)

        # Reorder columns for readability (optional)
        preferred_order = [
            "ticker", "name", "exchange", "lastPrice", "percentNetChange",
            "netChange", "marketCap", "volume"
        ]
        df = df[[c for c in preferred_order if c in df.columns] + 
                [c for c in df.columns if c not in preferred_order]]

        # Sort by percent change if available
        if "percentNetChange" in df.columns:
            df = df.sort_values("percentNetChange", ascending=False).reset_index(drop=True)

        return df
       
class MarketCommoditiesExtractor(BaseExtractor):
    
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/commodities"
    BASE_URL = URLS["market_commodities"]

    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL
        self.params = None

    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        pass

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar commodities response into a clean DataFrame, skipping date and currency fields."""
        
        if not response or "page" not in response or "commodities" not in response["page"]:
            return pd.DataFrame()

        commodities = response["page"]["commodities"]
        rows = []

        for item in commodities:
            base_info = {
                "id": item.get("id"),
                "instrument": item.get("instrument"),
                "instrumentID": item.get("instrumentID"),
                "name": item.get("name"),
                "category": item.get("category"),
                "exchange": item.get("exchange"),
            }

            data_points = item.get("dataPoints", {})
            for key, dp in data_points.items():
                base_info[f"{key}_value"] = dp.get("value")

                # Extract properties except date and currency
                props = dp.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" not in prop_key.lower() and "currency" not in prop_key.lower():
                        base_info[f"{key}_{prop_key}"] = prop_value.get("value")

                # Include only exchange info if present (no date or currency)
                if "exchange" in dp:
                    base_info[f"{key}_exchange"] = dp["exchange"].get("value")

            rows.append(base_info)

        df = pd.DataFrame(rows)
        
        if "category" in df.columns:
            df = df.sort_values(by="category", ascending=True).reset_index(drop=True)
        
        return df
    
class MarketCurrenciesExtractor(BaseExtractor):
    REQUIRED_AUTH: AuthType = AuthType.WAF_TOKEN
    PAGE_URL = "https://www.morningstar.com/markets/currencies"
    BASE_URL = URLS["market_currencies"]

    def __init__(self, client: BaseClient = None):
        client = BaseClient(auth_type=self.REQUIRED_AUTH, url=self.PAGE_URL)
        super().__init__(client)
        self.url = self.BASE_URL

    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        self.params = None

    def _process_response(self, response: dict) -> pd.DataFrame:
        
        if not response or "page" not in response or "currencies" not in response["page"]:
            return pd.DataFrame()

        currencies = response["page"]["currencies"]
        rows = []

        for item in currencies:
            base_info = {
                "id": item.get("id"),
                "instrumentID": item.get("instrumentID"),
                "label": item.get("label"),
                "name": item.get("name"),
                "category": item.get("category"),
                "bidPriceDecimals": item.get("bidPriceDecimals"),
            }

            data_points = item.get("dataPoints", {})
            for key, dp in data_points.items():
                # main numeric value
                base_info[f"{key}_value"] = dp.get("value")

                # nested properties — skip date and currency
                props = dp.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" not in prop_key.lower() and "currency" not in prop_key.lower():
                        base_info[f"{key}_{prop_key}"] = prop_value.get("value")

                # include exchange if present
                if "exchange" in dp:
                    base_info[f"{key}_exchange"] = dp["exchange"].get("value")

            rows.append(base_info)

        df = pd.DataFrame(rows)

        # Order by category if present
        if "category" in df.columns:
            df = df.sort_values(by="category", ascending=True).reset_index(drop=True)

        return df



    

    