# Historical Asset Prices

A Python script to retrieve historical prices for stocks, options, crypto, and forex at minute intervals using the [massive](https://github.com/massive-com/client-python) library.

## Overview

This repository retrieves historical price data for stocks, options, crypto, and forex at a minute interval and saves the data to the `/data` directory, organized by asset type and ticker in Parquet format.

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
   STOCK_TICKERS = ["SPY"]       # List of stock tickers to retrieve
   CRYPTO_TICKERS = ["BTC-USD"]  # List of crypto tickers to retrieve
   FOREX_TICKERS = ["EUR-USD"]   # List of forex tickers to retrieve
   DATE_START = "2025-01-01"     # Start date, inclusive (YYYY-MM-DD)
   DATE_END = "2025-02-01"       # End date, exclusive (YYYY-MM-DD)
   ```

## Usage

Run the main script:
```bash
python main.py
```

The script will:
- Download daily flat files from S3 containing all tickers (cached in `data/files/`)
- Extract and save per-ticker data as Parquet files (in `data/prices/`)
- Skip days that already have data files (idempotent)
- Create `.empty` marker files for days with no data (weekends/holidays) to avoid redundant downloads

## Data Structure

Data is organized in the `data/` directory:
```
data/
├── files/                      # Cached raw flat files from S3
│   ├── stocks/
│   │   ├── YYYY-MM-DD.csv.gz        (daily flat file with all stocks)
│   │   └── YYYY-MM-DD.csv.gz.empty  (marker for no data)
│   ├── options/
│   │   └── ...
│   ├── crypto/
│   │   └── ...
│   └── forex/
│       └── ...
└── prices/                     # Extracted per-ticker data
    ├── stocks/
    │   └── TICKER/
    │       ├── YYYY-MM-DD.parquet        (trading days with data)
    │       └── YYYY-MM-DD.parquet.empty  (weekends/holidays)
    ├── options/
    │   └── ...
    ├── crypto/
    │   └── ...
    └── forex/
        └── ...
```

Each Parquet file contains minute-level price data for that ticker and day, with the `timestamp` column as the index.

### Data Schema

**Stocks and Options:**
- Index: `timestamp` (datetime64[ns, America/New_York])
- Columns: `ticker`, `open`, `close`, `low`, `high`, `volume`

```python
# stock prices
                          ticker    open   close     low    high  volume
timestamp
2025-01-02 04:00:00-05:00    SPY  588.22  588.80  588.12  589.07  2374.0
2025-01-02 04:01:00-05:00    SPY  589.10  589.16  589.10  589.25   976.0
2025-01-02 04:02:00-05:00    SPY  589.03  588.93  588.85  589.03  1018.0
2025-01-02 04:03:00-05:00    SPY  588.90  588.90  588.90  588.90   441.0
2025-01-02 04:04:00-05:00    SPY  589.00  589.00  589.00  589.00   924.0

# options prices
                                         ticker   open  close    low   high  volume
timestamp
2025-01-02 09:30:00-05:00  O:SPY250124P00604000  16.21  16.22  16.21  16.22       2
2025-01-02 09:30:00-05:00  O:SPY250124P00540000   0.68   0.68   0.68   0.68      12
2025-01-02 09:30:00-05:00  O:SPY250124P00505000   0.30   0.30   0.30   0.30       6
2025-01-02 09:30:00-05:00  O:SPY250124P00500000   0.30   0.29   0.29   0.30       2
2025-01-02 09:30:00-05:00  O:SPY250124P00450000   0.18   0.18   0.18   0.18      36
```

### Loading Data

Use `glob.glob()` with the `*.parquet` pattern to load data files while excluding `.empty` marker files:

```python
import glob
import pandas as pd

# Load all stock data for a ticker
stocks = pd.read_parquet(glob.glob("./data/prices/stocks/SPY/*.parquet")).sort_index()

# Load all option data for a ticker
options = pd.read_parquet(glob.glob("./data/prices/options/SPY/*.parquet")).sort_index()

# Load all crypto data for a ticker
crypto = pd.read_parquet(glob.glob("./data/prices/crypto/BTC-USD/*.parquet")).sort_index()

# Load all forex data for a ticker
forex = pd.read_parquet(glob.glob("./data/prices/forex/EUR-USD/*.parquet")).sort_index()
```

See `load_data.ipynb` for a more complete example.

<img width="1027" height="545" alt="SPY Closing Price" src="https://github.com/user-attachments/assets/6cec4049-c3f0-446a-a4aa-09a0224883f3" />

## Data Availability

All prices are retrieved via [Minute Aggregates Flat Files](https://massive.com/docs/flat-files):
- **Stocks**: [Stock Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)
- **Options**: [Option Minute Aggregates](https://massive.com/docs/flat-files/options/minute-aggregates)
- **Crypto**: [Crypto Minute Aggregates](https://massive.com/docs/flat-files/crypto/minute-aggregates)
- **Forex**: [Forex Minute Aggregates](https://massive.com/docs/flat-files/forex/minute-aggregates)

A sample dataset of pre-retrieved historical prices for 2024 is available for download: [Dropbox Shared Folder](https://www.dropbox.com/scl/fo/2hfetk4k4n3z139jyqhb3/APwMO_XOVTuaObJUWAAzH5o?rlkey=gphwsbuo1knb4d5popfd29k4t&st=2nv3atqg&dl=0)
