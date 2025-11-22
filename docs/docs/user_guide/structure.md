# Project Structure

This page provides an overview of the **MorningPy** project architecture.  
Understanding the folder layout will help you navigate the codebase, extend modules, and contribute effectively.

MorningPy is organized into clearly defined components that separate configuration, data models, core logic, API communication, and data storage.

---

## Directory Overview

morningpy/
├── config/
├── schema/
├── core/
├── api/
└── data/

Each folder has a dedicated responsibility and follows the principles of clean, modular, and maintainable architecture.

---

## `config/` — Global Configuration

The `config/` folder contains all settings and configuration utilities used across the library.

Typical contents include:

- Environment variable loaders
- API keys and authentication settings
- Global constants (endpoints, base URLs)
- Logging configuration
- Cache settings

Example files:

config/
├── settings.py
├── environment.py
└── logging.yaml

## `schema/` — Data Models & Validation

The `schema/` directory contains all data models that define the structure of returned data.

MorningPy typically uses:

- **Pydantic models** for validation
- Strong typing for reliability
- Schema definitions for financial instruments (Equity, ETF, Fund, etc.)

Example:

schema/
├── price.py
├── fundamentals.py
└── metadata.py

These models ensure that every API response is **clean, typed, and validated** before being returned to the user.

---

## `core/` — Core Logic of the Library

This is the heart of MorningPy.  
The `core/` folder contains internal logic, utilities, and abstractions.

Common responsibilities:

- HTTP request managers
- Response parsing
- Error handling
- Caching mechanisms
- Shared helpers used across modules

Example:

The core layer powers all higher-level functionality.

---

## `api/` — Morningstar Endpoint Wrappers

The `api/` folder contains all modules that expose public-facing methods and data access endpoints.

This is where users typically interact:

- Price data extraction
- Fundamentals & ratios
- ETF analytics
- News aggregation
- Portfolio tools

Example layout:

api/
├── prices.py
├── fundamentals.py
├── etf.py
├── news.py
└── portfolios.py

Each module builds on `core/` and returns clean, validated `schema/` objects.

---

## `data/` — Local Data Storage (Optional)

The `data/` folder is used to store:

- Cached API responses (if enabled)
- Local datasets
- User-generated files (exports, snapshots)
- Temporary files

Example structure:

data/
├── cache/
└── samples/

Depending on the user’s configuration, the `data/` directory may be created automatically.

---

## 🗺 How Everything Fits Together

MorningPy follows a clean architecture:

This separation ensures:

- Maintainability  
- Testability  
- Clear extension points  
- Predictable behavior  
- Strong data validation  

---

## Extending the Project

To add a new feature:

 User Code
     ↓
   api/
     ↓
   core/
     ↓
 schema/
     ↓
  config/
     ↓
   data/

1. Define the data model → `schema/`
2. Implement HTTP logic → `core/`
3. Add public wrapper → `api/`
4. Optionally configure settings → `config/`
5. (If needed) store data → `data/`

This consistent flow keeps the project clean and scalable.

---

If you want, I can also generate:

✅ a **developer guide**  
✅ a **contributing.md**  
✅ auto-generated folder diagrams  
Just ask!