import asyncio
from typing import Dict, List

import aiohttp

from .apis.coingecko import CoinGeckoClient
from .apis.yahoo import YahooClient
from .constants import MAJOR_FOREX, MINOR_FOREX, SHORTCUTS
from .db.cache import TickerCache


class MarketClassifier:
    def __init__(self, db_name: str = "ticker_cache.db", hours_to_expire: int = 24):
        """Create a MarketClassifier instance.

        Parameters
        ----------
        db_name : str, optional
            SQLite filename for the `TickerCache`, by default "ticker_cache.db".
        hours_to_expire : int, optional
            Hours after which cached entries expire, by default 24.
        """
        self.cache = TickerCache(db_name, hours_to_expire)
        self.yahoo = YahooClient()
        self.cg = CoinGeckoClient()

    # --- CORE LOGIC (Shared) ---
    def _process_duel(
        self, to_process: List[str], yahoo_data: Dict, crypto_data: Dict
    ) -> Dict:
        """Resolve competing category signals for each symbol.

        The classifier considers three possible sources for each symbol:
        stock (Yahoo), crypto (CoinGecko), and forex (heuristics). Each source
        receives a numeric score (market cap or heuristic weight) and the
        highest-scoring source determines the final classification.

        Parameters
        ----------
        to_process : list[str]
            Uppercase symbols to evaluate.
        yahoo_data : dict
            Mapping of symbol -> Yahoo quote dict (as returned by `YahooClient`).
        crypto_data : dict
            Mapping of symbol -> CoinGecko-derived dict containing at least
            a `market_cap` key.

        Returns
        -------
        dict
            Mapping of symbol -> final classification dict containing keys
            such as `category`, `ticker`, `name`, `market_cap`, and
            `yahoo_lookup`.
        """
        processed = {}
        # Init structure
        duel = {
            s: {"stock": 0, "crypto": 0, "forex": 0, "details": {}} for s in to_process
        }

        for sym in to_process:
            # 1. Forex Heuristics
            if sym in MAJOR_FOREX:
                duel[sym]["forex"] = 100_000_000_000_000
                duel[sym]["details"]["forex"] = {
                    "type": "Forex",
                    "name": f"{sym} Currency",
                    "market_cap": None,
                }
            elif sym in MINOR_FOREX:
                duel[sym]["forex"] = 50_000_000
                duel[sym]["details"]["forex"] = {
                    "type": "Forex",
                    "name": f"{sym} Currency",
                    "market_cap": None,
                }

            # 2. Stock Data
            if sym in yahoo_data:
                info = yahoo_data[sym]
                qtype = info.get("quoteType", "UNKNOWN")
                raw_mcap = info.get("marketCap", 0)
                score = raw_mcap
                if qtype == "INDEX":
                    score = 50_000_000_000
                if qtype == "FUTURE":
                    score = 10_000_000_000

                duel[sym]["stock"] = score
                duel[sym]["details"]["stock"] = {
                    "type": qtype,
                    "name": info.get("shortName") or info.get("longName"),
                    "market_cap": raw_mcap,
                }

            # 3. Crypto Data
            if sym in crypto_data:
                info = crypto_data[sym]
                mcap = info.get("market_cap", 0)
                duel[sym]["crypto"] = mcap
                duel[sym]["details"]["crypto"] = {
                    "type": "Crypto",
                    "name": info.get("name"),
                    "market_cap": mcap,
                }

            # 4. Resolve
            scores = duel[sym]
            winner = max(["stock", "crypto", "forex"], key=lambda k: scores[k])

            if scores[winner] == 0:
                final = {"category": "Unknown", "ticker": sym}
            else:
                details = scores["details"].get(winner, {})
                alternatives = [
                    k
                    for k in ["stock", "crypto", "forex"]
                    if scores[k] > 0 and k != winner
                ]

                y_look = sym
                if winner == "crypto":
                    y_look = f"{sym}-USD"
                elif winner == "forex":
                    y_look = f"{sym}USD=X"

                final = {
                    "category": winner if winner != "stock" else details.get("type"),
                    "ticker": sym,
                    "name": details.get("name"),
                    "market_cap": details.get("market_cap"),
                    "yahoo_lookup": y_look,
                    "alternatives": alternatives,
                    "source": "api",
                }
            processed[sym] = final
        return processed

    # --- PUBLIC METHODS ---

    def classify_bulk(self, symbols: List[str]) -> List[Dict]:
        """Synchronous Bulk Classification"""
        """Synchronously classify a list of ticker-like symbols.

        Parameters
        ----------
        symbols : list[str]
            Iterable of symbols (may contain duplicates or mixed case). The
            returned list preserves the order of the input list with each
            element replaced by its classification dict or `None`.

        Returns
        -------
        list[dict]
            List of classification dictionaries aligned with the input order.
        """
        unique = list({s.upper().strip() for s in symbols if s.strip()})
        results_map = {}
        to_process = []

        # Cache check
        cached = self.cache.get_many(unique)
        for sym in unique:
            if sym in SHORTCUTS:
                results_map[sym] = {**SHORTCUTS[sym], "source": "shortcut"}
            elif sym in cached:
                results_map[sym] = cached[sym]
            else:
                to_process.append(sym)

        if to_process:
            y_res = self.yahoo.get_quotes_sync(to_process)
            c_res = self.cg.get_prices_sync(to_process)
            processed = self._process_duel(to_process, y_res, c_res)
            self.cache.save_many(processed)
            results_map.update(processed)

        return [results_map.get(s.upper().strip()) for s in symbols]

    async def classify_bulk_async(self, symbols: List[str]) -> List[Dict]:
        """Asynchronous Bulk Classification"""
        """Asynchronously classify a list of ticker-like symbols.

        Parameters
        ----------
        symbols : list[str]
            List of symbols to classify. Input order is preserved in the
            returned list.

        Returns
        -------
        list[dict]
            Classification results aligned with the input list; entries may
            be `None` for unknown symbols.
        """
        unique = list({s.upper().strip() for s in symbols if s.strip()})
        results_map = {}
        to_process = []

        # Cache Read (Run in thread to avoid blocking loop)
        loop = asyncio.get_running_loop()
        cached = await loop.run_in_executor(None, self.cache.get_many, unique)

        for sym in unique:
            if sym in SHORTCUTS:
                results_map[sym] = {**SHORTCUTS[sym], "source": "shortcut"}
            elif sym in cached:
                results_map[sym] = cached[sym]
            else:
                to_process.append(sym)

        if to_process:
            async with aiohttp.ClientSession() as session:
                task_y = self.yahoo.get_quotes_async(session, to_process)
                task_c = self.cg.get_prices_async(session, to_process)
                y_res, c_res = await asyncio.gather(task_y, task_c)

            processed = self._process_duel(to_process, y_res, c_res)

            # Cache Write (Run in thread)
            await loop.run_in_executor(None, self.cache.save_many, processed)
            results_map.update(processed)

        return [results_map.get(s.upper().strip()) for s in symbols]
