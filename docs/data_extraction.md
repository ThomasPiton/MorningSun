# Data Extraction Guide

This document provides detailed information about the types of data that can be extracted using the MorningSun library and their formats.

## Types of Data

MorningSun supports extracting various types of financial data from Morningstar, including but not limited to:

1. **Stock Data**
    - Historical prices
    - Dividends
    - Splits
    - Daily closing prices
    - Volume traded

2. **Mutual Fund Data**
    - NAV (Net Asset Value)
    - Expense ratios
    - Fund performance
    - Holdings

3. **Financial Metrics**
    - P/E ratios (Price-to-Earnings)
    - P/B ratios (Price-to-Book)
    - Market capitalization
    - Earnings per share (EPS)
    - Dividend yield

4. **Company Data**
    - Company profile
    - Key executives
    - Financial statements (income statement, balance sheet, cash flow)

## Data Formats

### Stock Data

Historical stock data is typically provided in a tabular format with the following columns:

- `Date`: The date of the data point.
- `Open`: The opening price.
- `High`: The highest price during the trading session.
- `Low`: The lowest price during the trading session.
- `Close`: The closing price.
- `Volume`: The number of shares traded.

Example:

| Date       | Open  | High  | Low   | Close | Volume   |
|------------|-------|-------|-------|-------|----------|
| 2024-01-01 | 100.0 | 105.0 | 99.0  | 104.0 | 5000000  |
| 2024-01-02 | 104.0 | 108.0 | 103.0 | 107.0 | 6000000  |

### Mutual Fund Data

Mutual fund data includes information about NAV, expense ratios, performance, and holdings:

- `Date`: The date of the data point.
- `NAV`: Net Asset Value.
- `Expense Ratio`: The fund's expense ratio.
- `Performance`: Performance metrics over various periods (e.g., 1-year, 5-year).
- `Holdings`: List of the top holdings in the fund.

Example:

| Date       | NAV   | Expense Ratio | 1-Year Performance | Top Holdings          |
|------------|-------|---------------|--------------------|-----------------------|
| 2024-01-01 | 20.50 | 0.50%         | 10%                | AAPL, MSFT, AMZN      |
| 2024-01-02 | 20.75 | 0.50%         | 11%                | AAPL, MSFT, AMZN, GOOGL|

### Financial Metrics

Financial metrics provide key ratios and financial data points:

- `Date`: The date of the data point.
- `P/E Ratio`: Price-to-Earnings ratio.
- `P/B Ratio`: Price-to-Book ratio.
- `Market Cap`: Market capitalization.
- `EPS`: Earnings per share.
- `Dividend Yield`: Dividend yield.

Example:

| Date       | P/E Ratio | P/B Ratio | Market Cap   | EPS  | Dividend Yield |
|------------|-----------|-----------|--------------|------|----------------|
| 2024-01-01 | 25.0      | 3.5       | 1,000,000,000| 4.00 | 2.5%           |
| 2024-01-02 | 26.0      | 3.6       | 1,050,000,000| 4.10 | 2.6%           |

### Company Data

Company data includes detailed information about the company's profile, executives, and financial statements:

- `Company Name`: The name of the company.
- `Profile`: Brief description of the company.
- `Key Executives`: List of key executives with their titles.
- `Financial Statements`: Income statement, balance sheet, and cash flow.

Example:

#### Company Profile

```json
{
  "company_name": "Example Corp",
  "profile": "Example Corp is a leading provider of example services and products.",
  "key_executives": [
    {
      "name": "John Doe",
      "title": "CEO"
    },
    {
      "name": "Jane Smith",
      "title": "CFO"
    }
  ]
}
