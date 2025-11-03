# Quant Developer – End-to-End Analytics App

A local one-command analytics app that ingests Binance NDJSON data, stores it in SQLite, resamples to bars, computes pairs-trading analytics (spread, z-score, correlation, ADF test), and shows interactive charts.

## Quick Start

python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python app.py

Open the Streamlit URL shown in the terminal (usually http://localhost:8501).

## Collect NDJSON Data

1. Open tools/binance_browser_collector_save_test.html in your browser.
2. Enter symbols, for example: btcusdt,ethusdt.
3. Click Start, wait 30–60 seconds, then click Stop and Download NDJSON.

## Ingest Data

1. In the Streamlit app, expand “Ingest data”.
2. Upload the NDJSON file you downloaded.
3. Click “Ingest uploaded file ➜ DB”.

## Run Analytics

1. Choose Symbol X and Y (for example BTCUSDT and ETHUSDT).
2. Choose timeframe (1S or 1Min).
3. View price, spread, z-score, and correlation charts.

## Export Results

Use the download buttons in the app to save CSV files of results.
