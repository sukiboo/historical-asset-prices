# Historical Asset Prices

A Python script to retrieve historical stock and option prices at minute intervals using the [massive](https://github.com/massive-com/client-python) library.

## Overview

This repository retrieves historical stock price data for specified tickers at a minute interval and saves the data to the `/data` directory, organized by ticker and month in Parquet format.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set your Massive API key:
   ```bash
   export MASSIVE_API_KEY=your_api_key_here
   ```

3. Configure tickers and date range in `src/constants.py`

## Usage

Run the main script:
```bash
python main.py
```

The script will:
- Retrieve minute-level stock prices for all configured tickers
- Skip months that already have data files (idempotent)
- Save data as Parquet files in `data/{TICKER}/{YYYY-MM}.parquet`

(options prices are not yet implemented but I'm working on it)

## Data Structure

Data is organized in the `data/` directory:
```
data/
  {TICKER}/
    YYYY-MM.parquet
```

Each Parquet file contains minute-level price data for that ticker and month.
