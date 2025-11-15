import pandas as pd
from datetime import datetime
from typing import Any, Dict, List,Union

from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.market import *
from morningpy.schema.market import *

class MarketCalendarUsInfoExtractor(BaseExtractor):
    
    config = MarketCalendarUsInfoConfig
    schema = MarketCalendarUsInfoSchema

    def __init__(self, date: str | list[str], info_type: str):
        
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.date = date
        self.info_type = info_type.lower().strip()
        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.valid_inputs = self.config.VALID_INPUTS 
        self.rename_columns = self.config.RENAME_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        if self.info_type not in self.valid_inputs:
            raise ValueError(f"Invalid info_type '{self.info_type}', must be one of {self.valid_inputs}")
        dates = self.date if isinstance(self.date, list) else [self.date]
        for d in dates:
            datetime.strptime(d, "%Y-%m-%d") 

    def _build_request(self) -> None:
        if isinstance(self.date, list):
            self.params = [{**self.params, "date": d, "category": self.info_type} for d in self.date]
        else:
            self.params = {**self.params, "date": self.date, "category": self.info_type}

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

    config = MarketFairValueConfig
    schema = MarketFairValueSchema

    def __init__(self, value_type:Union[str, List[str]]= "overvaluated"):
        
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.value_type = value_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.mapping_inputs = self.config.MAPPING_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        """Ensure value_type is valid."""
        if isinstance(self.value_type, str):
            if self.value_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid index_type '{self.value_type}', must be one of {self.index_type}"
                )
            self.value_type = [self.value_type]
        elif isinstance(self.value_type, list):
            invalid = [m for m in self.value_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid index_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("index_type must be a str or list[str]")

    def _build_request(self) -> None:
        """No params for this endpoint."""
        pass

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar fair value data."""
        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        all_rows = []

        # Flatten all selected value types
        for key in self.value_type:
            comp_key = self.mapping_inputs.get(key)
            if not comp_key:
                continue

            comp = components.get(comp_key, {})
            payload = comp.get("payload", [])

            # payload could be a dict or list
            if isinstance(payload, dict):
                results = payload.get("results", [])
            elif isinstance(payload, list):
                results = payload
            else:
                continue

            if not results:
                continue

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
                    "category":comp_key
                }

                for field_key, field_data in fields.items():
                    if not isinstance(field_data, dict) or "value" not in field_data:
                        continue

                    # main value
                    row[field_key] = field_data.get("value")

                    # nested properties
                    props = field_data.get("properties", {})
                    for prop_key, prop_val in props.items():
                        if any(x in prop_key.lower() for x in ["date", "currency"]):
                            continue
                        row[f"{field_key}_{prop_key}"] = prop_val.get("value")

                rows.append(row)

            all_rows.extend(rows)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df.dropna(subset=["security_id"]).reset_index(drop=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
   
        return df
    
class MarketIndexesExtractor(BaseExtractor):
    
    config = MarketIndexesConfig
    schema = MarketIndexesSchema

    def __init__(self, index_type:Union[str, List[str]]= "americas"):
        
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.index_type = index_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.mapping_inputs = self.config.MAPPING_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        if isinstance(self.index_type, str):
            if self.index_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid index_type '{self.index_type}', must be one of {self.index_type}"
                )
            self.index_type = [self.index_type]
        elif isinstance(self.index_type, list):
            invalid = [m for m in self.index_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid index_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("index_type must be a str or list[str]")

    def _build_request(self) -> None:
        pass

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar index data depending on selected index_type."""

        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        rows = []

        # Flatten all selected index sections
        for key in self.index_type:
            comp = components.get(self.mapping_inputs[key], {})
            payload = comp.get("payload", [])
            if not isinstance(payload, list):
                continue

            for item in payload:
                row = {**item, "category": key.replace("Indexes", "").capitalize()}
                rows.append(row)

        df = pd.DataFrame(rows)
        
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df = df.sort_values(by=["category", "percent_net_change"], ascending=[True, False]).reset_index(drop=True)

        return df

class MarketExtractor(BaseExtractor):
    
    config = MarketConfig
    schema = MarketSchema

    def __init__(self, info_type:Union[str, List[str]] = "commodities"):
        
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.info_type = info_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.mapping_inputs = self.config.MAPPING_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        if isinstance(self.info_type, str):
            if self.info_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid index_type '{self.info_type}', must be one of {self.valid_inputs}"
                )
            self.info_type = [self.info_type]
        elif isinstance(self.info_type, list):
            invalid = [m for m in self.info_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid index_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("index_type must be a str or list[str]")

    def _build_request(self) -> None:
        pass

    def _process_response(self, response: Dict[str, Any]) -> pd.DataFrame:
        """Convert API response to a flattened DataFrame, filling missing fields with None."""
        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        all_rows = []

        for key in self.info_type:
            comp_key = self.mapping_inputs.get(key)
            if not comp_key:
                continue

            payload = components.get(comp_key, {}).get("payload", {})
            if not isinstance(payload, dict):
                continue

            # Flatten each country/item
            for country_code, item in payload.items():
                flat_item = {}
                for k, v in item.items():
                    if isinstance(v, dict) and "value" in v:
                        flat_item[k] = v.get("value")
                    elif isinstance(v, dict):
                        for sub_k, sub_v in v.items():
                            if isinstance(sub_v, dict) and "value" in sub_v:
                                flat_item[f"{k}_{sub_k}"] = sub_v.get("value")
                            else:
                                flat_item[f"{k}_{sub_k}"] = sub_v
                    else:
                        flat_item[k] = v

                # Optionally add country code
                flat_item["country_code"] = country_code.upper()
                flat_item["category"] = key
                all_rows.append(flat_item)

            
        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)

        # Rename columns and keep only final columns
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]

        # Fill string and numeric columns
        df[self.str_columns] = df[self.str_columns].fillna("N/A")
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)

        df.reset_index(drop=True,inplace=True)
        
        return df
     
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
       
class MarketCommoditiesExtractor(BaseExtractor):
    
    config = MarketCommoditiesConfig
    schema = MarketCommoditiesSchema
    
    def __init__(self):

        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.url = self.config.API_URL
        self.rename_columns = self.config.RENAME_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

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

                props = dp.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" not in prop_key.lower() and "currency" not in prop_key.lower():
                        base_info[f"{key}_{prop_key}"] = prop_value.get("value")

                # Include only exchange info if present (no date or currency)
                if "exchange" in dp:
                    base_info[f"{key}_exchange"] = dp["exchange"].get("value")

            rows.append(base_info)

        df = pd.DataFrame(rows)
        
        df.rename(columns=self.rename_columns,inplace=True)
        df = df[self.final_columns]
        df = df.sort_values(by="category", ascending=True).reset_index(drop=True)
        
        return df
    
class MarketCurrenciesExtractor(BaseExtractor):

    config = MarketCurrenciesConfig
    schema = MarketCurrenciesSchema
    
    def __init__(self):

        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )

        super().__init__(client)

        self.url = self.config.API_URL
        self.rename_columns = self.config.RENAME_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        pass

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
        
        df.rename(columns=self.rename_columns,inplace=True)
        df = df[self.final_columns]
        df = df.sort_values(by="category", ascending=True).reset_index(drop=True)
        
        return df



    

    