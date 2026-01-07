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

# load all stock data for a specific date range
stock_files = glob.glob("./files/stocks/2025-01-*.csv.gz")
stocks = pd.concat([pd.read_csv(f, compression="gzip") for f in stock_files], ignore_index=True)

# filter for specific tickers after loading
df = stocks[stocks["ticker"] == "SPY"].copy()

# convert window_start to datetime and set as index
df["timestamp"] = pd.to_datetime(df["window_start"], unit="ns")
df = df.set_index("timestamp").sort_index()

df.head()
```

The CSV files contain columns: `window_start`, `ticker`, `open`, `close`, `low`, `high`, `volume`, `transactions`. The `window_start` column contains Unix timestamps in nanoseconds (UTC).

| timestamp            | ticker | volume | open   | close  | high   | low    | window_start          | transactions |
|---------------------|--------|--------|--------|--------|--------|--------|-----------------------|--------------|
| 2025-01-03 09:00:00 | SPY    | 4401   | 586.00 | 586.01 | 586.05 | 585.89 | 1735894800000000000   | 37           |
| 2025-01-03 09:01:00 | SPY    | 4318   | 586.04 | 585.98 | 586.04 | 585.98 | 1735894860000000000   | 26           |
| 2025-01-03 09:02:00 | SPY    | 4236   | 586.10 | 586.10 | 586.16 | 586.10 | 1735894920000000000   | 22           |
| 2025-01-03 09:03:00 | SPY    | 1415   | 586.09 | 586.23 | 586.23 | 586.09 | 1735894980000000000   | 31           |
| 2025-01-03 09:05:00 | SPY    | 343    | 586.37 | 586.37 | 586.37 | 586.37 | 1735895100000000000   | 11           |

<img width="1184" height="584" alt="Image" src="https://github.com/user-attachments/assets/0f575af5-27f0-4fdc-a132-5bae7c7fbabb" />

## Data Availability

All prices are retrieved via [Minute Aggregates Flat Files REST API](https://massive.com/docs/flat-files):
- **Stocks**: [Stock Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)
- **Options**: [Option Minute Aggregates](https://massive.com/docs/flat-files/options/minute-aggregates)
- **Crypto**: [Crypto Minute Aggregates](https://massive.com/docs/flat-files/crypto/minute-aggregates)
- **Forex**: [Forex Minute Aggregates](https://massive.com/docs/flat-files/forex/minute-aggregates)

✨ [Pre-2026 data](https://www.dropbox.com/scl/fo/xd5a5s5cwa0imf6gvplzv/AL1ffzRw3_AEfeEwRoKLQms?rlkey=ah6c8ps5zvco29npoeoro831k&st=zd6g4y7x&dl=0) ✨ -- all available daily files for all assets up to 2026 🎉
