from __future__ import annotations
import pandas as pd

def resample_ohlcv(ticks: pd.DataFrame, rule: str = "1S") -> pd.DataFrame:

    if ticks.empty:
        return pd.DataFrame(columns=["ts","symbol","open","high","low","close","volume","count"])

    df = ticks.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts")

    def _agg(g: pd.DataFrame) -> pd.DataFrame:
        o = g["price"].resample(rule).first()
        h = g["price"].resample(rule).max()
        l = g["price"].resample(rule).min()
        c = g["price"].resample(rule).last()
        v = g["size"].resample(rule).sum().fillna(0.0)
        n = g["price"].resample(rule).count().astype(int)
        out = pd.concat([o.rename("open"), h.rename("high"), l.rename("low"),
                         c.rename("close"), v.rename("volume"), n.rename("count")], axis=1)
        out = out.reset_index().rename(columns={"ts":"ts"})
        return out

    parts = []
    for sym, g in df.groupby("symbol"):
        agg = _agg(g)
        agg["symbol"] = sym
        parts.append(agg)
    bars = pd.concat(parts, ignore_index=True).sort_values(["symbol","ts"]).reset_index(drop=True)
    return bars
