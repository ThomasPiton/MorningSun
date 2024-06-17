# Usage

This section provides examples of how to use MorningSun to extract, clean, and analyze financial data.

## Example 1: Extracting and Analyzing Data

Here's a basic example of how to use MorningSun.

```python
from morning_sun import MorningSun 

# Initialize MorningSun
ms = MorningSun(tickers="", method="web")

# Extract historical data
data = ms.extract_data()

