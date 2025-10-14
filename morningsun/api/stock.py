import asyncio
from typing import Union, List

from morningsun.core.interchange import DataFrameInterchange
from morningsun.extractors.stock import *


def get_stocks_details(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stocks details for one or multiple securities."""
    extractor = StockDetailsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_sustainability(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock sustainability data."""
    extractor = StockSustainabilityExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_esg_risk(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock ESG risk data."""
    extractor = StockEsgRiskExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_company_profile(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock company profile."""
    extractor = StockCompanyProfileExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_overview(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock overview."""
    extractor = StockOverviewExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_trailingTotalReturns(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock trailing total returns."""
    extractor = StockTrailingTotalReturnsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_info(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock info."""
    extractor = StockInfoExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_key_ratios(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock key ratios."""
    extractor = StockKeyRatiosExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_trading_information(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock trading information."""
    extractor = StockTradingInformationExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_summary(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock summary."""
    extractor = StockSummaryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_valuation(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock valuation."""
    extractor = StockValuationExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_growth_table(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock growth table."""
    extractor = StockGrowthTableExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_profitability_and_efficiency(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock profitability and efficiency."""
    extractor = StockProfitabilityAndEfficiencyExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_financial_health(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock financial health."""
    extractor = StockFinancialHealthExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_cash_flow(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock cash flow."""
    extractor = StockCashFlowExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_balance_sheet_detail(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock balance sheet details."""
    extractor = StockBalanceSheetDetailExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_cash_flow_detail(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock cash flow details."""
    extractor = StockCashFlowDetailExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_income_statement_detail(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock income statement details."""
    extractor = StockIncomeStatementDetailExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_dividends(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock dividends."""
    extractor = StockDividendsExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_split(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock split data."""
    extractor = StockSplitExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_insider_transaction(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock insider transactions."""
    extractor = StockInsiderTransactionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_insider_key_executives(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock key executives."""
    extractor = StockInsiderKeyExecutivesExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_transaction_history(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock transaction history."""
    extractor = StockTransactionHistoryExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_mutualfund(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock mutual fund ownership."""
    extractor = StockOwnershipMutualFundExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_institution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock institutional ownership."""
    extractor = StockOwnershipInstitutionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_concentrated_owners_institution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock concentrated institutional owners."""
    extractor = StockConcentratedOwnersInstitutionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_concentrated_owners_mutualfund(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock concentrated mutual fund owners."""
    extractor = StockConcentratedOwnersMutualFundExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_buyers_mutualfund(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock mutual fund buyers."""
    extractor = StockOwnershipBuyersMutualFundExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_buyers_institution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock institutional buyers."""
    extractor = StockOwnershipBuyersInstitutionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_sellers_institution(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock institutional sellers."""
    extractor = StockOwnershipSellersInstitutionExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_ownership_sellers_mutualfund(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock mutual fund sellers."""
    extractor = StockOwnershipSellersMutualFundExtractor(id_sec).run()
    return asyncio.run(extractor)


def get_stock_board_directors(id_sec: Union[str, List[str]]) -> DataFrameInterchange:
    """Get stock board of directors."""
    extractor = StockBoardDirectorsExtractor(id_sec).run()
    return asyncio.run(extractor)