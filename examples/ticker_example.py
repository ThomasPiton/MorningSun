"""
Ticker Data Example for MorningPy
"""
from morningpy.api.ticker import (
    search_tickers,
    batch_convert,
    convert
)

def run():
    # ETFs
    us_stock_tickers = search_tickers(security_type="stock", country_id="USA",exact_match=True)

    # Search for specific tickers
    tech_tickers = search_tickers(ticker=["AAPL", "MSFT", "GOOGL"])

    # Search for active ETFs in technology sector
    tech_etf = search_tickers(
        security_type="etf",
        sector="Technology",
        is_active=True
    )
    
    five_stars_funds = search_tickers(
        security_type="fund",
        fund_star_rating=5
    )

    # Complex multi-criteria search
    eq_etf = search_tickers(
        security_type="etf",
        asset_class="Equity",
        region="North America",
        is_active=True,
        esg_index=True,
        exact_match=False  # Partial matching for text fields
    )

    # Search by partial name match
    tech_label_security = search_tickers(
        security_label="Technology",  # Finds all securities with "Technology" in name
        exact_match=False
    )
    
    # Batch conversion
    sec_bath = batch_convert(["AAPL", "MSFT", "GOOGL", "AMZN"], from_field="ticker", to_field="isin")

    
    # Single conversion
    isin = convert(performance_id="0P0001PU03", convert_to="isin")


    security_id = convert(isin="US0378331005", convert_to="security_id")

    return "Correctly extracted !"
    
if __name__ == "__main__":
    print(run())
