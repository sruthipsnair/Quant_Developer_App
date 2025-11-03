from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional
import pandas as pd

_DB_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,           -- ISO8601 UTC
    price REAL NOT NULL,
    size REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);
CREATE TABLE IF NOT EXISTS bars (
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    timeframe TEXT NOT NULL,    -- e.g., '1S','1Min','5Min'
    "open" REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY(symbol, ts, timeframe)
);
CREATE INDEX IF NOT EXISTS idx_bars_symbol_tf_ts ON bars(symbol, timeframe, ts);
"""

class Storage:
    def __init__(self, db_path: str | Path = "data/ticks.db") -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path.as_posix(), detect_types=sqlite3.PARSE_DECLTYPES)
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(_DB_SCHEMA)
        self._conn.commit()

    def insert_ticks(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        d = df.copy()
        d["ts"] = pd.to_datetime(d["ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        rows = list(d[["symbol","ts","price","size"]].itertuples(index=False, name=None))
        self._conn.executemany(
            "INSERT INTO ticks(symbol, ts, price, size) VALUES (?,?,?,?)", rows
        )
        self._conn.commit()
        return len(rows)

    def insert_bars(self, bars: pd.DataFrame, timeframe: str) -> int:
        if bars is None or bars.empty:
            return 0
        b = bars.copy()
        b["ts"] = pd.to_datetime(b["ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        b["timeframe"] = timeframe
        rows = list(b[["symbol","ts","timeframe","open","high","low","close","volume","count"]]
                    .itertuples(index=False, name=None))
        self._conn.executemany(
            """INSERT OR REPLACE INTO bars(symbol, ts, timeframe, "open", high, low, close, volume, count)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            rows
        )
        self._conn.commit()
        return len(rows)

    def symbols(self) -> List[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM ticks ORDER BY symbol;")
        return [r[0] for r in cur.fetchall()]

    def load_ticks(self, symbol: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        q = "SELECT symbol, ts, price, size FROM ticks WHERE symbol=?"
        params: List[object] = [symbol]
        if start:
            q += " AND ts >= ?"; params.append(start)
        if end:
            q += " AND ts <= ?"; params.append(end)
        q += " ORDER BY ts"
        df = pd.read_sql_query(q, self._conn, params=params, parse_dates=["ts"])
        return df

    def load_bars(self, symbol: str, timeframe: str) -> pd.DataFrame:
        q = """SELECT symbol, ts, timeframe, "open", high, low, close, volume, count
               FROM bars WHERE symbol=? AND timeframe=? ORDER BY ts"""
        df = pd.read_sql_query(q, self._conn, params=[symbol, timeframe], parse_dates=["ts"])
        return df

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
