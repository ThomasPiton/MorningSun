import pandas as pd
import json
import os
import gzip

# Configure these
PARQUET_FILE = "morningpy/data/tickers.parquet"
OUTPUT_DIR = "docs/docs/tickers_table"

# Read data
df = pd.read_parquet(PARQUET_FILE)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Convert to JSON (compressed with gzip)
data = df.to_dict('records')
json_path = os.path.join(OUTPUT_DIR, "data.json.gz")
with gzip.open(json_path, 'wt', encoding='utf-8') as f:
    json.dump(data, f)

# Dynamically create columns from dataframe
columns = [{"data": col, "title": col} for col in df.columns]

# Create HTML with dynamic columns
html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Ticker Table</title>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/pako@2.1.0/dist/pako.min.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        #table {{ width: 100% !important; }}
    </style>
</head>
<body>
    <h2>Ticker Table ({len(df):,} rows)</h2>
    <table id="table" class="display"></table>
    <script>
    $(document).ready(function() {{
        fetch('data.json.gz')
            .then(response => response.arrayBuffer())
            .then(buffer => {{
                const decompressed = pako.ungzip(new Uint8Array(buffer), {{ to: 'string' }});
                const data = JSON.parse(decompressed);
                $('#table').DataTable({{
                    data: data,
                    columns: {json.dumps(columns)},
                    pageLength: 50,
                    lengthMenu: [[25, 50, 100, 500], [25, 50, 100, 500]]
                }});
            }});
    }});
    </script>
</body>
</html>
"""

with open(os.path.join(OUTPUT_DIR, "table.html"), 'w') as f:
    f.write(html)

# Create index with column list
column_list = ", ".join([f"`{col}`" for col in df.columns])
index = f"""# Ticker Table

**Total rows:** {len(df):,}  
**Columns:** {len(df.columns)}

**Column names:** {column_list}

## Interactive Table

<iframe src="table.html" width="100%" height="600px" frameborder="0"></iframe>

[Open full page](table.html)

## Sample Data

{df.head(10).to_markdown(index=False)}
"""

with open(os.path.join(OUTPUT_DIR, "index.md"), 'w') as f:
    f.write(index)

print(f"✓ Generated {len(df):,} rows × {len(df.columns)} columns")
print(f"✓ Output: {OUTPUT_DIR}")
print(f"✓ Compressed JSON: data.json.gz")
print(f"✓ Columns: {', '.join(df.columns[:5])}...")