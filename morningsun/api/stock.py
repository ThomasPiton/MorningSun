from typing import Union, List
import pandas as pd

def get_stocks_details(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stocks details for one or multiple securities."""
    return StockDetailsExtractor(id_sec).run()

def get_stock_sustainability(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock sustainability data."""
    return StockSustainabilityExtractor(id_sec).run()

def get_stock_esg_risk(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock ESG risk data."""
    return StockEsgRiskExtractor(id_sec).run()

def get_stock_company_profile(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock company profile."""
    return StockCompanyProfileExtractor(id_sec).run()

def get_stock_overview(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock overview."""
    return StockOverviewExtractor(id_sec).run()

def get_stock_trailingTotalReturns(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock trailing total returns."""
    return StockTrailingTotalReturnsExtractor(id_sec).run()

def get_stock_info(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock info."""
    return StockInfoExtractor(id_sec).run()

def get_stock_key_ratios(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock key ratios."""
    return StockKeyRatiosExtractor(id_sec).run()

def get_stock_trading_information(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock trading information."""
    return StockTradingInformationExtractor(id_sec).run()

def get_stock_summary(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock summary."""
    return StockSummaryExtractor(id_sec).run()

def get_stock_valuation(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock valuation."""
    return StockValuationExtractor(id_sec).run()

def get_stock_growth_table(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock growth table."""
    return StockGrowthTableExtractor(id_sec).run()

def get_stock_profitability_and_efficiency(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock profitability and efficiency."""
    return StockProfitabilityAndEfficiencyExtractor(id_sec).run()

def get_stock_financial_health(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock financial health."""
    return StockFinancialHealthExtractor(id_sec).run()

def get_stock_cash_flow(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock cash flow."""
    return StockCashFlowExtractor(id_sec).run()

def get_stock_balance_sheet_detail(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock balance sheet details."""
    return StockBalanceSheetDetailExtractor(id_sec).run()

def get_stock_cash_flow_detail(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock cash flow details."""
    return StockCashFlowDetailExtractor(id_sec).run()

def get_stock_income_statement_detail(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock income statement details."""
    return StockIncomeStatementDetailExtractor(id_sec).run()

def get_stock_dividends(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock dividends."""
    return StockDividendsExtractor(id_sec).run()

def get_stock_split(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock split data."""
    return StockSplitExtractor(id_sec).run()

def get_stock_insider_transaction(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock insider transactions."""
    return StockInsiderTransactionExtractor(id_sec).run()

def get_stock_insider_key_executives(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock key executives."""
    return StockInsiderKeyExecutivesExtractor(id_sec).run()

def get_stock_transaction_history(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock transaction history."""
    return StockTransactionHistoryExtractor(id_sec).run()

def get_stock_ownership_mutualfund(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock mutual fund ownership."""
    return StockOwnershipMutualFundExtractor(id_sec).run()

def get_stock_ownership_institution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock institutional ownership."""
    return StockOwnershipInstitutionExtractor(id_sec).run()

def get_stock_concentrated_owners_institution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock concentrated institutional owners."""
    return StockConcentratedOwnersInstitutionExtractor(id_sec).run()

def get_stock_concentrated_owners_mutualfund(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock concentrated mutual fund owners."""
    return StockConcentratedOwnersMutualFundExtractor(id_sec).run()

def get_stock_ownership_buyers_mutualfund(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock mutual fund buyers."""
    return StockOwnershipBuyersMutualFundExtractor(id_sec).run()

def get_stock_ownership_buyers_institution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock institutional buyers."""
    return StockOwnershipBuyersInstitutionExtractor(id_sec).run()

def get_stock_ownership_sellers_institution(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock institutional sellers."""
    return StockOwnershipSellersInstitutionExtractor(id_sec).run()

def get_stock_ownership_sellers_mutualfund(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock mutual fund sellers."""
    return StockOwnershipSellersMutualFundExtractor(id_sec).run()

def get_stock_board_directors(id_sec: Union[str, List[str]]) -> pd.DataFrame:
    """Get stock board of directors."""
    return StockBoardDirectorsExtractor(id_sec).run()