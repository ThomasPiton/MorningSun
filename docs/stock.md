# 📈 Stock Module

The **Stock** module of `MorningSun` provides a simple and unified interface for fetching, transforming, and analyzing stock market data from various providers.

It allows you to:
- Retrieve stock prices, fundamentals, and metadata  
- Compute derived indicators (returns, volatility, moving averages)  
- Access both static and real-time data through a consistent API  

---

## 🚀 Quick Start

```python
from morningsun.api.stock import StockAPI

# Initialize the stock interface
api = StockAPI()

# Fetch daily prices for Apple
df = api.get_prices("AAPL", start="2024-01-01", end="2024-12-31")

# Display first rows
print(df.head())

# Plot prices
df["close"].plot(title="AAPL Daily Prices")
