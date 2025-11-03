from __future__ import annotations
import pandas as pd

def evaluate_zscore_alerts(z: pd.Series, upper: float = 2.0, lower: float = -2.0) -> pd.DataFrame:
    
    if z is None or len(z) == 0:
        return pd.DataFrame(columns=["ts", "z", "side"])

    z = z.dropna()
    ups = z[z > upper]
    dns = z[z < lower]

    ups_df = pd.DataFrame({"ts": ups.index, "z": ups.values, "side": "short"})
    dns_df = pd.DataFrame({"ts": dns.index, "z": dns.values, "side": "long"})
    out = pd.concat([ups_df, dns_df], ignore_index=True).sort_values("ts").reset_index(drop=True)
    return out

