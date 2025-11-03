from __future__ import annotations
import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
from statsmodels.tsa.stattools import adfuller

def compute_hedge_ratio_ols(x: pd.Series, y: pd.Series) -> float:
    
    df = pd.concat([x.rename("x"), y.rename("y")], axis=1).dropna()
    if len(df) < 5:
        return 1.0
    X = add_constant(df["x"].values.astype(float))
    model = OLS(df["y"].values.astype(float), X).fit()
    beta = float(model.params[1]) if len(model.params) > 1 else 1.0
    return beta

def compute_spread_and_zscore(x: pd.Series, y: pd.Series, lookback: int = 120) -> pd.DataFrame:
    
    df = pd.concat([x.rename("x"), y.rename("y")], axis=1).dropna()
    if len(df) < max(lookback, 10):
        return pd.DataFrame(index=df.index, data={"spread": np.nan, "spread_mean": np.nan,
                                                  "spread_std": np.nan, "z": np.nan})
    beta = compute_hedge_ratio_ols(df["x"].iloc[-lookback:], df["y"].iloc[-lookback:])
    spread = df["y"] - beta * df["x"]
    roll = spread.rolling(lookback, min_periods=max(10, lookback // 3))
    z = (spread - roll.mean()) / (roll.std(ddof=1))
    out = pd.DataFrame(
        {"spread": spread, "spread_mean": roll.mean(), "spread_std": roll.std(ddof=1), "z": z}
    )
    return out

def adf_pvalue(series: pd.Series, maxlag: int | None = None) -> float | None:
   
    s = pd.Series(series).dropna()
    if len(s) < 20:
        return None
    try:
        res = adfuller(s.values, maxlag=maxlag, autolag="AIC")
        return float(res[1])
    except Exception:
        return None

def rolling_correlation(a: pd.Series, b: pd.Series, window: int = 60) -> pd.Series:
    df = pd.concat([a.rename("a"), b.rename("b")], axis=1).dropna()
    if len(df) < window:
        return pd.Series(index=df.index, dtype=float)
    return df["a"].rolling(window).corr(df["b"])
