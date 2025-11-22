# Quick Start

Welcome!  
This guide will help you install **MorningPy**, configure your environment, and make your first request in just a few minutes.

MorningPy is designed to be simple to use while remaining powerful and fully extensible.

---

## ✅ 1. Install MorningPy

If you haven’t installed MorningPy yet, run:

```bash
pip install morningpy
Or refer to the full installation guide:

👉 Installation

🔑 2. Set Your API Key (If Required)
MorningPy automatically detects your API credentials from:

Environment variables

A .env file

Local configuration files

Example using environment variables:

bash
Copier le code
export MORNINGPY_API_KEY="your_api_key"
On Windows (PowerShell):

powershell
Copier le code
setx MORNINGPY_API_KEY "your_api_key"
If your project uses .env:

ini
Copier le code
MORNINGPY_API_KEY=your_api_key
🚀 3. Your First Request
Here is the simplest example to fetch the latest price for a stock:

python
Copier le code
import morningpy as mp

price = mp.Price("AAPL").latest()
print(price)
You should receive a validated PriceSchema object with the most recent value.

📈 4. Fetch Historical Prices
You can easily request historical data:

python
Copier le code
import morningpy as mp

history = mp.Price("MSFT").history(start="2020-01-01", end="2024-01-01")
print(history.head())
MorningPy returns a tidy pandas DataFrame ready for analysis.

📊 5. Access Fundamentals
Retrieve fundamental financial metrics for any instrument:

python
Copier le code
from morningpy import Fundamentals

f = Fundamentals("AAPL").overview()
print(f)
📰 6. Retrieve Latest News
python
Copier le code
from morningpy import News

news = News("AAPL").latest(limit=5)
for article in news:
    print(article.title)
🧩 7. Explore the API Modules
The main modules available in MorningPy include:

Price — real-time & historical data

Fundamentals — ratios, financials, metadata

ETF — composition, analytics

News — aggregates & filters market news

Portfolio — analytics & helper tools

More details here:

👉 API Reference

🧠 8. Explore Example Notebooks
If you prefer learning interactively:

👉 Notebooks

These include examples for:

price analytics

portfolio construction

fundamentals screening

ETF data extraction