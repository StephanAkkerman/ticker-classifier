import json
import sqlite3
from collections import defaultdict  # <--- Added for easier list handling
from datetime import datetime, timedelta
from typing import Any, Dict, List

import yfinance as yf
from pycoingecko import CoinGeckoAPI

# Import your constants
from constants import major_forex, minor_forex, shortcuts


class MarketClassifier:
    def __init__(self, db_name: str = "ticker_cache.db", hours_to_expire: int = 24):
        self.cg = CoinGeckoAPI()
        self.db_name = db_name
        self.hours_to_expire = hours_to_expire
        self._init_db()

        self.shortcuts = shortcuts
        self.major_forex = major_forex
        self.minor_forex = minor_forex

        # Changed: Maps Symbol -> LIST of IDs (to handle collisions)
        self._crypto_map: Dict[str, List[str]] = None

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
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

    def _get_many_from_cache(self, symbols: List[str]) -> Dict[str, Any]:
        """Returns a dict {symbol: data} for found, fresh items."""
        cutoff_time = datetime.now() - timedelta(hours=self.hours_to_expire)
        cutoff_str = cutoff_time.isoformat()
        results = {}
        if not symbols:
            return results

        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" * len(symbols))
            query = f"SELECT symbol, data FROM tickers WHERE symbol IN ({placeholders}) AND updated_at > ?"
            cursor.execute(query, symbols + [cutoff_str])
            for symbol, data_str in cursor.fetchall():
                results[symbol] = json.loads(data_str)
                results[symbol]["source"] = "cache"
        return results

    def _save_many_to_cache(self, items: Dict[str, Any]):
        if not items:
            return
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            data_tuples = []
            for sym, data in items.items():
                if data.get("category") != "Unknown":
                    clean_data = {k: v for k, v in data.items() if k != "source"}
                    data_tuples.append((sym, json.dumps(clean_data), now))
            if data_tuples:
                cursor.executemany(
                    "INSERT OR REPLACE INTO tickers (symbol, data, updated_at) VALUES (?, ?, ?)",
                    data_tuples,
                )
                conn.commit()

    # --- UPDATED CRYPTO MAPPING ---
    def _get_crypto_map(self):
        """
        Maps Symbol -> LIST of IDs.
        Example: 'BTC' -> ['bitcoin', 'batcat', 'bitcoin-2']
        """
        if self._crypto_map is not None:
            return self._crypto_map

        # print("Fetching CoinGecko Master List...")
        try:
            coins = self.cg.get_coins_list()
            self._crypto_map = defaultdict(list)
            for coin in coins:
                sym = coin["symbol"].upper()
                c_id = coin["id"]
                # Optimization: CoinGecko has 10k+ junk coins.
                # If we have 100 collisions for 'ETH', checking all of them is slow.
                # We append all, but later we can limit request size if needed.
                self._crypto_map[sym].append(c_id)
        except Exception as e:
            print(f"Warning: Could not fetch crypto list: {e}")
            self._crypto_map = {}

        return self._crypto_map

    def classify_bulk(self, symbols: List[str]) -> List[Dict[str, Any]]:
        # 1. Deduplicate
        unique_symbols = list({s.upper().strip() for s in symbols if s.strip()})
        results_map = {}
        to_process = []

        # 2. Check Shortcuts & Cache
        cached_data = self._get_many_from_cache(unique_symbols)

        for sym in unique_symbols:
            if sym in self.shortcuts:
                results_map[sym] = self.shortcuts[sym]
                results_map[sym]["source"] = "shortcut"
            elif sym in cached_data:
                results_map[sym] = cached_data[sym]
            else:
                to_process.append(sym)

        if not to_process:
            return [results_map.get(s.upper().strip()) for s in symbols]

        # 3. PREPARE DATA STRUCTURE
        duel_data = {
            s: {"stock": 0, "crypto": 0, "forex": 0, "details": {}} for s in to_process
        }

        # --- A. FOREX ---
        for sym in to_process:
            if sym in self.major_forex:
                duel_data[sym]["forex"] = 100_000_000_000_000
            elif sym in self.minor_forex:
                duel_data[sym]["forex"] = 50_000_000

        # --- B. STOCKS (Yahoo) ---
        if to_process:
            try:
                batch_str = " ".join(to_process)
                tickers = yf.Tickers(batch_str)
                for sym, ticker_obj in tickers.tickers.items():
                    sym_clean = sym.upper()
                    if sym_clean in duel_data:
                        try:
                            info = ticker_obj.info
                            qtype = info.get("quoteType")
                            if qtype in [
                                "EQUITY",
                                "ETF",
                                "INDEX",
                                "MUTUALFUND",
                                "FUTURE",
                            ]:
                                mcap = info.get("marketCap", 0)
                                if qtype == "INDEX":
                                    mcap = 50_000_000_000
                                if qtype == "FUTURE":
                                    mcap = 10_000_000_000
                                duel_data[sym_clean]["stock"] = mcap
                                duel_data[sym_clean]["details"]["stock"] = {
                                    "type": qtype,
                                    "name": info.get("shortName"),
                                }
                        except:
                            pass
            except:
                pass

        # --- C. CRYPTO (Revised for Collisions) ---
        crypto_map = self._get_crypto_map()

        # 1. Gather ALL candidate IDs for our symbols
        # e.g. for "BTC", gather ["bitcoin", "batcat", ...]
        all_candidate_ids = []
        id_to_parent_symbol = {}  # map 'batcat' -> 'BTC'

        for sym in to_process:
            if sym in crypto_map:
                candidates = crypto_map[sym]
                # SAFETY LIMIT: If a symbol has 50+ clones, only check the first 10
                # + any that exactly match the symbol name (heuristic for "real" one)
                # CoinGecko usually puts the oldest/biggest first, but not always.
                candidates = candidates[:10]

                for c_id in candidates:
                    all_candidate_ids.append(c_id)
                    id_to_parent_symbol[c_id] = sym

        # 2. Batch fetch prices for ALL candidates
        if all_candidate_ids:
            chunk_size = 200  # CoinGecko can handle large URL params usually
            for i in range(0, len(all_candidate_ids), chunk_size):
                chunk = all_candidate_ids[i : i + chunk_size]
                try:
                    data = self.cg.get_price(
                        ids=chunk, vs_currencies="usd", include_market_cap="true"
                    )

                    for c_id, val in data.items():
                        parent_sym = id_to_parent_symbol.get(c_id)
                        if parent_sym:
                            mcap = val.get("usd_market_cap", 0)

                            # LOGIC: Does this coin beat the current best crypto for this symbol?
                            current_best = duel_data[parent_sym]["crypto"]
                            if mcap > current_best:
                                duel_data[parent_sym]["crypto"] = mcap
                                duel_data[parent_sym]["details"]["crypto"] = {
                                    "type": "Crypto",
                                    "name": c_id.title(),
                                    "id": c_id,  # Store the specific ID (e.g. 'bitcoin')
                                }
                except Exception as e:
                    print(f"CoinGecko Batch Error: {e}")

        # --- D. RESOLVE ---
        processed_results = {}
        for sym in to_process:
            scores = duel_data[sym]
            winner = max(["stock", "crypto", "forex"], key=lambda k: scores[k])

            if scores[winner] == 0:
                final_res = {"category": "Unknown", "ticker": sym}
            else:
                details = scores["details"].get(winner, {})
                alternatives = [
                    k
                    for k in ["stock", "crypto", "forex"]
                    if scores[k] > 0 and k != winner
                ]

                # Setup Yahoo Lookup
                y_lookup = sym
                if winner == "crypto":
                    # If Crypto wins, we need the specific pair.
                    # Best guess is Symbol-USD, but ideally we'd use the ID if we had a map.
                    y_lookup = f"{sym}-USD"
                elif winner == "forex":
                    y_lookup = f"{sym}USD=X"

                final_res = {
                    "category": winner if winner != "stock" else details.get("type"),
                    "ticker": sym,
                    "name": details.get("name"),
                    "yahoo_lookup": y_lookup,
                    "alternatives": alternatives,
                    "source": "api",
                }

            results_map[sym] = final_res
            processed_results[sym] = final_res

        self._save_many_to_cache(processed_results)
        return [results_map.get(s.upper().strip()) for s in symbols]

    def classify(self, symbol: str) -> Dict[str, Any]:
        return self.classify_bulk([symbol])[0]


if __name__ == "__main__":
    resolver = MarketClassifier()
    # BTC = Bitcoin (Huge Mcap), not Batcat (Tiny Mcap)
    # PEPE = Pepe (Medium Mcap), not the 50 other fake Pepes
    test_tickers = ["BTC", "PEPE", "NVDA"]

    print("Classifying...")
    res = resolver.classify_bulk(test_tickers)
    for r in res:
        print(f"{r['ticker']} -> {r['name']} ({r['category']})")
