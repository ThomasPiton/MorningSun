"""
Security Data Example for MorningPy
"""
from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

def run():
    # Financial statement
    income_statement = get_financial_statement(
        statement_type="Income Statement",
        report_frequency="Quarterly",
        security_id=["0P000115U4"]
    )
    print("Income Statement:")
    print(income_statement.head())

    # Holdings
    holding_info = get_holding_info(
        performance_id=["0P0001PU03", "0P0001BG3E"]
    )
    print("\nHolding Info:")
    print(holding_info.head())

    holding = get_holding(
        performance_id=["0P0001PU03", "0P00013Z57"]
    )
    print("\nHolding Data:")
    print(holding.head())

if __name__ == "__main__":
    run()
