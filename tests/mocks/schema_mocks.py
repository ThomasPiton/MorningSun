# tests/mocks/schema_mocks.py

from datetime import datetime

from morningpy.schema import (
    MarketCalendarUsInfoSchema,
    MarketFairValueSchema,
    MarketIndexesSchema,
    MarketSchema,
    MarketMoversSchema,
    MarketCommoditiesSchema,
    MarketCurrenciesSchema,
    HeadlineNewsSchema,
    FinancialStatementSchema,
    HoldingSchema,
    HoldingInfoSchema,
    TickerSchema,
    IntradayTimeseriesSchema,
    HistoricalTimeseriesSchema,
)

# ---------------------------
# One mock instance per schema
# ---------------------------

SCHEMA_MOCKS = {
    MarketCalendarUsInfoSchema: MarketCalendarUsInfoSchema(
        name="US Markets"
    ),

    MarketFairValueSchema: MarketFairValueSchema(
        category="Equity",
        security_id="ABC123",
        ticker="ABC",
        name="Test Asset",
        evaluated_price=102.3,
        fair_value=100.0,
    ),

    MarketIndexesSchema: MarketIndexesSchema(
        category="Index",
        ticker="SPX",
        last_price=5000.5,
        volume=1000000,
    ),

    MarketSchema: MarketSchema(
        name="Global Markets"
    ),

    MarketMoversSchema: MarketMoversSchema(
        category="Gainers",
        ticker="AAPL",
        last_price=198.1,
        percent_net_change=2.5,
    ),

    MarketCommoditiesSchema: MarketCommoditiesSchema(
        name="Gold",
        last_price=2301.4,
        percent_net_change=0.15,
    ),

    MarketCurrenciesSchema: MarketCurrenciesSchema(
        label="EUR/USD",
        bid_price=1.083,
        percent_net_change=-0.2,
    ),

    HeadlineNewsSchema: HeadlineNewsSchema(
        title="Market Rally Continues",
        language="EN",
        link="https://example.com",
    ),

    FinancialStatementSchema: FinancialStatementSchema(
        statement_type="income_statement",
    ),

    HoldingSchema: HoldingSchema(
        parent_security_id="FND123",
        child_security_id="STK456",
        ticker="MSFT",
        weighting=0.042,
    ),

    HoldingInfoSchema: HoldingInfoSchema(
        master_portfolio_id="PORT789",
        security_id="ABC123",
        number_of_holding=150,
    ),

    TickerSchema: TickerSchema(),

    IntradayTimeseriesSchema: IntradayTimeseriesSchema(
        security_id="XYZ",
        date="2024-01-01",
        open=10.5,
        high=11.0,
        low=10.2,
        close=10.8,
    ),

    HistoricalTimeseriesSchema: HistoricalTimeseriesSchema(
        security_id="XYZ",
        date=datetime(2024, 1, 1),
        open=100.1,
        high=102.5,
        low=99.5,
        close=101.0,
    ),
}