from __future__ import annotations
import json
from pathlib import Path
from typing import Iterable, Dict, Any, List
import pandas as pd

from .storage import Storage

def _map_record(obj: Dict[str, Any]) -> Dict[str, Any]:

    if {"s","T","p","q"}.issubset(obj.keys()):
        symbol = str(obj["s"])
        ts = pd.to_datetime(int(obj["T"]), unit="ms", utc=True)
        price = float(obj["p"])
        size = float(obj["q"])
        return {"symbol": symbol, "ts": ts, "price": price, "size": size}

    symbol = str(obj.get("symbol") or obj.get("S") or obj.get("s", "UNKNOWN"))
    raw_ts = obj.get("ts") or obj.get("T")
    if isinstance(raw_ts, (int, float)):
       
        ts = pd.to_datetime(int(raw_ts), unit="ms", utc=True)
        if ts.year < 2005:  
            ts = pd.to_datetime(int(raw_ts), unit="s", utc=True)
    else:
        ts = pd.to_datetime(raw_ts, utc=True)
    price = float(obj.get("price") or obj.get("p"))
    size = float(obj.get("size") or obj.get("q", 0.0))
    return {"symbol": symbol, "ts": ts, "price": price, "size": size}

def load_ndjson_lines(lines: Iterable[str]) -> pd.DataFrame:
    recs: List[Dict[str, Any]] = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            obj = json.loads(ln)
            recs.append(_map_record(obj))
        except Exception:
          
            continue
    if not recs:
        return pd.DataFrame(columns=["symbol", "ts", "price", "size"])
    df = pd.DataFrame.from_records(recs)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    return df

def ingest_ndjson_file(path: str | Path, storage: Storage) -> pd.DataFrame:

    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        df = load_ndjson_lines(f)
    if not df.empty:
        storage.insert_ticks(df)
    return df
