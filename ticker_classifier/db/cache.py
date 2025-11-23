import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List


class TickerCache:
    def __init__(self, db_name: str, hours_to_expire: int):
        self.db_name = db_name
        self.hours_to_expire = hours_to_expire
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tickers (
                    symbol TEXT PRIMARY KEY,
                    data TEXT,
                    updated_at TEXT 
                )
            """
            )

    def get_many(self, symbols: List[str]) -> Dict[str, Any]:
        if not symbols:
            return {}
        cutoff = (datetime.now() - timedelta(hours=self.hours_to_expire)).isoformat()
        results = {}

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" * len(symbols))
            query = f"SELECT symbol, data FROM tickers WHERE symbol IN ({placeholders}) AND updated_at > ?"
            cursor.execute(query, symbols + [cutoff])
            for s, d in cursor.fetchall():
                results[s] = json.loads(d)
                results[s]["source"] = "cache"
        return results

    def save_many(self, items: Dict[str, Any]):
        if not items:
            return
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            data_tuples = []
            for s, d in items.items():
                if d.get("category") != "Unknown":
                    clean = {k: v for k, v in d.items() if k != "source"}
                    data_tuples.append((s, json.dumps(clean), now))
            if data_tuples:
                cursor.executemany(
                    "INSERT OR REPLACE INTO tickers (symbol, data, updated_at) VALUES (?, ?, ?)",
                    data_tuples,
                )
