from __future__ import annotations
import io
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
import yaml
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backend.storage import Storage
from backend.ingest import ingest_ndjson_file, load_ndjson_lines
from backend.resample import resample_ohlcv
from backend.analytics import compute_spread_and_zscore, rolling_correlation, adf_pvalue
from backend.alerts import evaluate_zscore_alerts

st.set_page_config(page_title="Quant Developer Analytics", layout="wide")

CFG_PATH = Path("config/settings.yaml")
if CFG_PATH.exists():
    cfg = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
else:
    cfg = dict(db_path="data/ticks.db", default_symbols=["btcusdt","ethusdt"],
               default_timeframe="1Min", z_lookback=120, corr_window=60,
               z_alert_upper=2.0, z_alert_lower=-2.0)

storage = Storage(cfg.get("db_path", "data/ticks.db"))

st.title("Quant Developer – Real-time Analytics (Local)")

with st.expander("1) Ingest data (NDJSON from Binance browser collector)", expanded=True):
    f = st.file_uploader("Upload NDJSON file (one JSON object per line)", type=["json","ndjson","txt"])
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Ingest uploaded file ➜ DB", use_container_width=True, disabled=f is None):
            try:
                if f is not None:
                    df = load_ndjson_lines(io.StringIO(f.getvalue().decode("utf-8")))
                    if not df.empty:
                        inserted = storage.insert_ticks(df)
                        st.success(f"Ingested {inserted} ticks for {df['symbol'].nunique()} symbols.")
                    else:
                        st.warning("No valid tick lines found.")
            except Exception as e:
                st.error(f"Ingestion failed: {e}")
    with col_b:
        st.markdown("Or put your file in `tools/` and ingest via code.")
        st.code("from backend.ingest import ingest_ndjson_file\n"
                "from backend.storage import Storage\n"
                "s=Storage('data/ticks.db'); ingest_ndjson_file('tools/sample.ndjson', s)")

st.subheader("2) Analysis")
all_syms = storage.symbols()
default_syms = [s for s in cfg.get("default_symbols", []) if s in all_syms]
if len(default_syms) < 2 and len(all_syms) >= 2:
    default_syms = all_syms[:2]

c1, c2, c3, c4 = st.columns([1,1,1,1])
sym_x = c1.selectbox("Symbol X", options=all_syms or ["btcusdt"], index=0 if all_syms else 0)
sym_y = c2.selectbox("Symbol Y", options=all_syms or ["ethusdt"], index=1 if len(all_syms)>1 else 0)
tf = c3.selectbox("Timeframe", options=["1S","1Min","5Min"], index=["1S","1Min","5Min"].index(cfg.get("default_timeframe","1Min")))
lookback = int(c4.number_input("Z-score lookback", min_value=20, max_value=2000, value=int(cfg.get("z_lookback",120))))

@st.cache_data(show_spinner=False)
def _load_and_resample(symbol: str, timeframe: str) -> pd.DataFrame:
    ticks = storage.load_ticks(symbol)
    bars = resample_ohlcv(ticks, {"1S":"1S","1Min":"1Min","5Min":"5Min"}[timeframe])
    return bars

bars_x = _load_and_resample(sym_x, tf)
bars_y = _load_and_resample(sym_y, tf)

if bars_x.empty or bars_y.empty:
    st.info("Not enough data yet. Ingest NDJSON trades first.")
    st.stop()

px_x = bars_x.set_index("ts")["close"]
px_y = bars_y.set_index("ts")["close"]
aligned = pd.concat([px_x.rename("x"), px_y.rename("y")], axis=1).dropna()

ana = compute_spread_and_zscore(aligned["x"], aligned["y"], lookback=lookback)
corr = rolling_correlation(aligned["x"], aligned["y"], window=int(cfg.get("corr_window",60)))
pval = adf_pvalue(ana["spread"])

st.write("### Price")
fig_price = px.line(aligned.reset_index(), x="ts", y=["x","y"], labels={"value":"Price","ts":"Time","variable":"Series"})
st.plotly_chart(fig_price, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.write("### Spread & Z-score")
    df_plot = pd.concat([ana[["spread","z"]]], axis=1).reset_index().rename(columns={"index":"ts"})
    fig_spread = px.line(df_plot, x="ts", y=["spread","z"], labels={"value":"Value","ts":"Time","variable":"Metric"})
    st.plotly_chart(fig_spread, use_container_width=True)

with col2:
    st.write("### Rolling correlation")
    fig_corr = px.line(corr.reset_index().rename(columns={0:"corr","index":"ts"}), x="ts", y="corr",
                       labels={"corr":"Corr","ts":"Time"})
    st.plotly_chart(fig_corr, use_container_width=True)

st.write("### Alerts")
upper = float(cfg.get("z_alert_upper", 2.0))
lower = float(cfg.get("z_alert_lower", -2.0))
alerts_df = evaluate_zscore_alerts(ana["z"], upper=upper, lower=lower)
st.dataframe(alerts_df.tail(20), use_container_width=True, height=240)

st.write("### Stats")
cA, cB, cC = st.columns(3)
cA.metric("Last z-score", f"{ana['z'].dropna().iloc[-1]:.2f}" if ana['z'].notna().any() else "n/a")
cB.metric("ADF p-value (spread)", f"{pval:.4f}" if pval is not None else "n/a")
cC.metric("Obs (aligned)", f"{len(aligned)}")

exp_col1, exp_col2 = st.columns(2)
with exp_col1:
    csv_spread = ana.reset_index().to_csv(index=False).encode("utf-8")
    st.download_button("Download spread/z CSV", csv_spread, file_name=f"{sym_y}-{sym_x}-{tf}-spread.csv")
with exp_col2:
    merged = bars_x.merge(bars_y, on="ts", suffixes=(f"_{sym_x}", f"_{sym_y}"))
    csv_bars = merged.to_csv(index=False).encode("utf-8")
    st.download_button("Download merged bars CSV", csv_bars, file_name=f"{sym_y}-{sym_x}-{tf}-bars.csv")

st.caption("Tip: Use the provided browser collector (tools/binance_browser_collector_save_test.html) to save NDJSON, then ingest above.")

