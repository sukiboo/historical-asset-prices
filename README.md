# Historical Asset Prices

A Python script to retrieve historical stock and option prices at minute intervals using the [massive](https://github.com/massive-com/client-python) library.

## Overview

This repository retrieves historical stock and options price data for specified tickers at a minute interval and saves the data to the `/data` directory, organized by ticker and month in Parquet format.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set Massive API credentials:
   ```bash
   export MASSIVE_API_KEY=your_api_key_here
   export MASSIVE_AWS_ACCESS_KEY_ID=your_aws_access_key_id_here
   ```

3. Configure tickers and date range in `src/constants.py`

## Usage

Run the main script:
```bash
python main.py
```

The script will:
- Retrieve minute-level stock and option prices for all configured tickers
- Skip days that already have data files (idempotent)
- Save stock prices as Parquet files in `data/stocks/{TICKER}/{YYYY-MM-DD}.parquet`
- Save option prices as Parquet files in `data/options/{TICKER}/{YYYY-MM-DD}.parquet`
- Create `.empty` marker files for days with no data (weekends/holidays) to avoid redundant API calls

## Data Structure

Data is organized in the `data/` directory:
```
data/
├── stocks/
│   └── TICKER/
│       ├── YYYY-MM-DD.parquet  (trading days with data)
│       └── YYYY-MM-DD.empty    (weekends/holidays)
└── options/
    └── TICKER/
        ├── YYYY-MM-DD.parquet  (trading days with data)
        └── YYYY-MM-DD.empty    (weekends/holidays)
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

Use `glob.glob()` with the `*.parquet` pattern to load only data files and exclude `.empty` marker files:

```python
import glob
import pandas as pd

# Load all stock data for a ticker (automatically sorted by timestamp)
ticker = "SPY"
stocks = pd.read_parquet(glob.glob(f"./data/stocks/{ticker}/*.parquet")).sort_index()

# Load all option data for a ticker
options = pd.read_parquet(glob.glob(f"./data/options/{ticker}/*.parquet")).sort_index()

# Plot closing prices (timestamp is already the index)
stocks["close"].plot(title=f"{ticker} Closing Price")
```

<img width="1027" height="545" alt="SPY Closing Price" src="https://github.com/user-attachments/assets/6cec4049-c3f0-446a-a4aa-09a0224883f3" />

## Data Availability

Stock prices are retrieved via the [Custom Bars REST API](https://massive.com/docs/rest/stocks/aggregates/custom-bars), which provides two years of historical data on a free plan.

Option prices are retrieved via the [Minute Aggregates Flat Files](https://massive.com/docs/flat-files/options/minute-aggregates), which requires the Options Starter plan for the two years of historical data.

## Sample Data

A sample dataset of pre-retrieved historical prices is available for download: [Dropbox Shared Folder](https://www.dropbox.com/scl/fo/2hfetk4k4n3z139jyqhb3/APwMO_XOVTuaObJUWAAzH5o?rlkey=gphwsbuo1knb4d5popfd29k4t&st=2nv3atqg&dl=0)
