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
                # Shortcuts usually don't have mcap data unless we hardcode it
                results_map[sym]["market_cap"] = None
            elif sym in cached_data:
                results_map[sym] = cached_data[sym]
            else:
                to_process.append(sym)

        if not to_process:
            return [results_map.get(s.upper().strip()) for s in symbols]

        # 3. PREPARE DATA STRUCTURE
        # Structure: scores used for the "Duel", details used for the "Result"
        duel_data = {
            s: {"stock": 0, "crypto": 0, "forex": 0, "details": {}} for s in to_process
        }

        # --- A. FOREX ---
        for sym in to_process:
            if sym in self.major_forex:
                duel_data[sym]["forex"] = 100_000_000_000_000  # Heuristic Score
                duel_data[sym]["details"]["forex"] = {
                    "type": "Forex",
                    "name": f"{sym} Currency",
                    "market_cap": None,
                }
            elif sym in self.minor_forex:
                duel_data[sym]["forex"] = 50_000_000  # Heuristic Score
                duel_data[sym]["details"]["forex"] = {
                    "type": "Forex",
                    "name": f"{sym} Currency",
                    "market_cap": None,
                }

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

                                raw_mcap = info.get("marketCap")  # The REAL number
                                ranking_score = raw_mcap if raw_mcap else 0

                                # Boost logic for things that might lack Mcap in Yahoo but are "Big"
                                if qtype == "INDEX":
                                    ranking_score = (
                                        50_000_000_000  # 50B boost for ranking
                                    )
                                if qtype == "FUTURE":
                                    ranking_score = (
                                        10_000_000_000  # 10B boost for ranking
                                    )

                                duel_data[sym_clean]["stock"] = ranking_score
                                duel_data[sym_clean]["details"]["stock"] = {
                                    "type": qtype,
                                    "name": info.get("shortName"),
                                    "market_cap": raw_mcap,  # Save the real number (or None)
                                }
                        except:
                            pass
            except:
                pass

        # --- C. CRYPTO (With Mcap Capture) ---
        crypto_map = self._get_crypto_map()

        all_candidate_ids = []
        id_to_parent_symbol = {}

        for sym in to_process:
            if sym in crypto_map:
                candidates = crypto_map[sym][:10]  # Top 10 collisions
                for c_id in candidates:
                    all_candidate_ids.append(c_id)
                    id_to_parent_symbol[c_id] = sym

        if all_candidate_ids:
            chunk_size = 200
            for i in range(0, len(all_candidate_ids), chunk_size):
                chunk = all_candidate_ids[i : i + chunk_size]
                try:
                    data = self.cg.get_price(
                        ids=chunk, vs_currencies="usd", include_market_cap="true"
                    )

                    for c_id, val in data.items():
                        parent_sym = id_to_parent_symbol.get(c_id)
                        if parent_sym:
                            raw_mcap = val.get("usd_market_cap", 0)

                            # Compare against current best crypto candidate for this symbol
                            current_best_score = duel_data[parent_sym]["crypto"]

                            if raw_mcap > current_best_score:
                                duel_data[parent_sym]["crypto"] = raw_mcap  # Score
                                duel_data[parent_sym]["details"]["crypto"] = {
                                    "type": "Crypto",
                                    "name": c_id.title(),
                                    "id": c_id,
                                    "market_cap": raw_mcap,  # Real Number
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

                y_lookup = sym
                if winner == "crypto":
                    y_lookup = f"{sym}-USD"
                elif winner == "forex":
                    y_lookup = f"{sym}USD=X"

                final_res = {
                    "category": winner if winner != "stock" else details.get("type"),
                    "ticker": sym,
                    "name": details.get("name"),
                    "market_cap": details.get("market_cap"),  # <--- NEW FIELD
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
    tickers = ["NVDA", "BTC", "EUR"]
    results = resolver.classify_bulk(tickers)
    for r in results:
        mcap = r.get("market_cap")
        # Simple helper to format nicely
        mcap_str = f"${mcap:,.0f}" if isinstance(mcap, (int, float)) else "N/A"

        print(f"{r['ticker']} | {r['category']} | Cap: {mcap_str}")
