import json
import sqlite3
from datetime import datetime, timedelta

import yfinance as yf
from pycoingecko import CoinGeckoAPI

# Assuming your constants are in a file named constants.py
# If running this alone, replace these imports with the actual dict/set definitions
from constants import major_forex, minor_forex, shortcuts


class MarketClassifier:
    def __init__(self, db_name: str = "ticker_cache.db", hours_to_expire: int = 24):
        self.cg = CoinGeckoAPI()

        # Database Setup
        self.db_name = db_name
        self.hours_to_expire = hours_to_expire
        self._init_db()

        # HARDCODED COMMODITIES / SHORTCUTS
        self.shortcuts = shortcuts
        self.major_forex = major_forex
        self.minor_forex = minor_forex

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            # We store 'updated_at' as an ISO formatted string
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tickers (
                    symbol TEXT PRIMARY KEY,
                    data TEXT,
                    updated_at TEXT 
                )
            """
            )
            conn.commit()

    def _get_from_cache(self, symbol):
        """
        Retrieves ticker data ONLY if it is newer than the expiration limit.
        """
        # Calculate the "Cutoff" time. Any data older than this is "dead".
        cutoff_time = datetime.now() - timedelta(hours=self.hours_to_expire)
        cutoff_str = cutoff_time.isoformat()

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()

            # SQL Query: Get data WHERE symbol matches AND date > cutoff
            cursor.execute(
                """
                SELECT data FROM tickers 
                WHERE symbol = ? AND updated_at > ?
            """,
                (symbol, cutoff_str),
            )

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])  # Found fresh data

        return None  # Found nothing, or data was too old (expired)

    def _save_to_cache(self, symbol, data):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            json_data = json.dumps(data)
            now = datetime.now().isoformat()

            # INSERT OR REPLACE updates the 'updated_at' timestamp automatically
            cursor.execute(
                """
                INSERT OR REPLACE INTO tickers (symbol, data, updated_at)
                VALUES (?, ?, ?)
            """,
                (symbol, json_data, now),
            )
            conn.commit()

    def classify(self, symbol):
        symbol = symbol.upper().strip()

        # 1. FASTEST: Check Hardcoded Shortcuts
        # (We check this before DB because it's instant memory lookup)
        if symbol in self.shortcuts:
            return self.shortcuts[symbol]

        # 2. FAST: Check Database Cache
        cached_result = self._get_from_cache(symbol)
        if cached_result:
            # Optional: Add a flag so you know it came from cache
            cached_result["source"] = "cache"
            return cached_result

        # --- START EXPENSIVE API LOGIC ---

        scores = {"stock": 0, "crypto": 0, "forex": 0}
        details = {}

        # 3. Forex Check
        if symbol in self.major_forex:
            scores["forex"] = 100_000_000_000_000
        elif symbol in self.minor_forex:
            scores["forex"] = 50_000_000

        # 4. Stock/ETF/Index Check
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            qtype = info.get("quoteType")

            if qtype in ["EQUITY", "ETF", "INDEX", "MUTUALFUND", "FUTURE"]:
                mcap = info.get("marketCap", 0)

                if qtype == "INDEX":
                    mcap = 50_000_000_000
                if qtype == "FUTURE":
                    mcap = 10_000_000_000

                scores["stock"] = mcap
                details["stock"] = {"type": qtype, "name": info.get("shortName")}
        except:
            pass

        # 5. Crypto Check
        try:
            results = self.cg.search(query=symbol)
            for coin in results.get("coins", []):
                if coin["symbol"].upper() == symbol:
                    rank = coin.get("market_cap_rank")
                    est_mcap = 0
                    if rank:
                        if rank <= 10:
                            est_mcap = 100_000_000_000
                        elif rank <= 500:
                            est_mcap = 100_000_000
                        else:
                            est_mcap = 500_000

                    if est_mcap > scores["crypto"]:
                        scores["crypto"] = est_mcap
                        details["crypto"] = {"type": "Crypto", "name": coin.get("name")}
        except:
            pass

        # 6. Resolve Winner
        winner = max(scores, key=scores.get)

        if scores[winner] == 0:
            return {"type": "Unknown", "ticker": symbol}

        final_data = details.get(winner, {})
        alternatives = [k for k, v in scores.items() if v > 0 and k != winner]

        result = {
            "category": (winner if winner != "stock" else final_data.get("type")),
            "ticker": symbol,
            "name": final_data.get("name"),
            "yahoo_lookup": symbol if winner != "crypto" else f"{symbol}-USD",
            "alternatives": alternatives,
        }

        # 7. SAVE TO CACHE
        if result["category"] != "Unknown":
            self._save_to_cache(symbol, result)
        else:
            # Don't cache failures
            pass

        result["source"] = "api"
        return result

    def prune_database(self):
        """
        Call this occasionally (e.g. once a week) to physically delete
        old rows and keep the file size small.
        """
        cutoff_time = datetime.now() - timedelta(hours=self.hours_to_expire)
        cutoff_str = cutoff_time.isoformat()

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM tickers WHERE updated_at <= ?", (cutoff_str,))
            deleted_count = cursor.rowcount
            conn.commit()
            print(f"Cleaned up {deleted_count} expired records.")


if __name__ == "__main__":
    resolver = MarketClassifier()
    tests = ["NVDA", "EUR", "ALL", "PEPE", "CUBE", "SPY"]

    print(f"{'INPUT':<8} | {'SOURCE':<8} | {'CATEGORY':<10} | {'NAME'}")
    print("-" * 60)

    # Run twice to prove caching works
    for _ in range(2):
        for t in tests:
            res = resolver.classify(t)
            source = res.get("source", "shortcut")
            name = res.get("name", "N/A")
            print(f"{t:<8} | {source:<8} | {res['category']:<10} | {name}")
        print("-" * 60)
        print("Running again... (Should be instant and say 'cache')")
        print("-" * 60)
