# Quant Developer – End-to-End Analytics App

A small, local, **one-command** analytics app that ingests Binance trade NDJSON, resamples to bars, computes **pairs-trading** helper analytics (OLS hedge ratio spread, z-score, ADF p-value, rolling correlation), shows interactive charts, and emits simple z-score alerts.

## Quick Start

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
python app.py
