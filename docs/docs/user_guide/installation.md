# Installation

This page will help you install **MorningPy** quickly and correctly, whether you're using it for data analysis, quantitative research, or backend development.

---

## Requirements

Before installing MorningPy, make sure you have:

- **Python 3.10+**
- **pip** (Python package manager)
- A stable internet connection for API calls

(Optional but recommended)

- **virtualenv** or **conda** to manage isolated environments

---

## Install MorningPy

To install the latest stable version from PyPI:

```bash
pip install morningpy
```

To upgrade an existing installation:

```bash
pip install --upgrade morningpy
```

## Install Development Version (Optional)

If you want access to the latest features and fixes, you can install the development build directly from GitHub:

```bash
pip install git+https://github.com/ThomasPiton/morningpy.git
```

## Test Your Installation

Run the following command to confirm everything works:

```bash
from morningpy.api.ticker import get_all_etfs

etfs = get_all_etfs()
print(etfs.head())
```