import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List


class TickerCache:
    def __init__(self, db_name: str, hours_to_expire: int):
        """Initialize the ticker cache.

        Parameters
        ----------
        db_name : str
            Path or name of the SQLite database file used for caching.
        hours_to_expire : int
            Number of hours after which cached entries are considered expired.
        """
        self.db_name = db_name
        self.hours_to_expire = hours_to_expire
        self._init_db()

    def _init_db(self):
        """Create the `tickers` table if it does not already exist.

        The table stores `symbol` as the primary key, the JSON-serialized
        `data` blob and an ISO-formatted `updated_at` timestamp.
        """
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
        """Retrieve multiple cached ticker entries that are not expired.

        Parameters
        ----------
        symbols : list[str]
            List of symbol strings to fetch from cache. If empty, returns an
            empty dict.

        Returns
        -------
        dict[str, Any]
            Mapping of symbol -> deserialized cache object. Each returned
            object will have a `source` key set to `'cache'`.
        """
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
        """Save multiple items to the cache.

        Parameters
        ----------
        items : dict[str, Any]
            Mapping of symbol -> item dict. Items with `category == 'Unknown'`
            are not persisted. The optional `source` key is stripped before
            saving.
        """
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
