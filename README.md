# 🗃️ Historical Asset Prices 🗃️

A Python script to retrieve historical price files for stocks, options, crypto, and forex at minute intervals using the [~~Polygon.io~~ Massive](https://github.com/massive-com/client-python) library.

## Overview

This repository downloads raw minute-level price data files from S3 containing all tickers. Files are stored locally in `files/{asset_type}/` directory.

Note that the data contains raw historical prices that are not adjusted for inflation, dividends, stock splits, etc.

## Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set Massive API credentials:
   ```bash
   export MASSIVE_API_KEY=your_api_key_here
   export MASSIVE_AWS_ACCESS_KEY_ID=your_aws_access_key_id_here
   ```

4. Configure retrieval parameters in `src/constants.py`:
   ```python
   # asset type retrieval flags
   RETRIEVE_STOCKS = True
   RETRIEVE_OPTIONS = True
   RETRIEVE_CRYPTO = True
   RETRIEVE_FOREX = True

   # date range for file retrieval
   DATE_START = "2025-01-01"     # start date, inclusive (YYYY-MM-DD)
   DATE_END = "2025-01-03"       # end date, exclusive (YYYY-MM-DD)
   ```

## Usage

Run the main script:
```bash
python main.py
```

The script will:
- Download daily flat files from S3 containing all tickers (cached in `files/{asset_type}/`)
- Compare local files with S3 using ETag matching to avoid re-downloading unchanged files
- Track and display counts of downloaded, updated, and skipped files
- Create `.empty` marker files for days with no data (weekends/holidays) to avoid redundant API calls
- Skip files that are already up-to-date (idempotent)

## Data Structure

Data is organized in the `files/` directory:
```
files/
├── stocks/
│   ├── YYYY-MM-DD.csv.gz        (daily flat file with all stocks)
│   └── YYYY-MM-DD.csv.gz.empty  (marker for no data)
├── options/
│   └── ...
├── crypto/
│   └── ...
└── forex/
    └── ...
```

Each `.csv.gz` file contains minute-level price data for all tickers of that asset type for that day. Files are stored as raw bytes directly from S3 (no decompression/recompression).

### Loading Data

Load the gzipped CSV files using pandas:

```python
import glob
import pandas as pd

# Load all stock data for a specific date range
stock_files = glob.glob("./files/stocks/2025-01-*.csv.gz")
stocks = pd.concat([pd.read_csv(f, compression="gzip") for f in stock_files], ignore_index=True)

# Load all crypto data
crypto_files = glob.glob("./files/crypto/2025-01-*.csv.gz")
crypto = pd.concat([pd.read_csv(f, compression="gzip") for f in crypto_files], ignore_index=True)

# Filter for specific tickers after loading
spy_data = stocks[stocks["ticker"] == "SPY"]
```

The CSV files contain columns: `window_start`, `ticker`, `open`, `close`, `low`, `high`, `volume`. The `window_start` column contains Unix timestamps in nanoseconds (UTC).

<img width="1027" height="545" alt="SPY Closing Price" src="https://github.com/user-attachments/assets/6cec4049-c3f0-446a-a4aa-09a0224883f3" />

## Data Availability

All prices are retrieved via [Minute Aggregates Flat Files REST API](https://massive.com/docs/flat-files):
- **Stocks**: [Stock Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)
- **Options**: [Option Minute Aggregates](https://massive.com/docs/flat-files/options/minute-aggregates)
- **Crypto**: [Crypto Minute Aggregates](https://massive.com/docs/flat-files/crypto/minute-aggregates)
- **Forex**: [Forex Minute Aggregates](https://massive.com/docs/flat-files/forex/minute-aggregates)

A dataset of all historical prices up to 2026 is available on ✨ [my dropbox](https://www.dropbox.com/scl/fo/xd5a5s5cwa0imf6gvplzv/AL1ffzRw3_AEfeEwRoKLQms?rlkey=ah6c8ps5zvco29npoeoro831k&st=zd6g4y7x&dl=0) ✨
