import asyncio
import re
from typing import Dict, List

import aiohttp

from .apis.coingecko import CoinGeckoClient
from .apis.yahoo import YahooClient
from .constants import MAJOR_FOREX, MINOR_FOREX, SHORTCUTS
from .db.cache import TickerCache
from .sectors import EquitySectorLookup


class TickerClassifier:
    def __init__(self, db_name: str = "ticker_cache.db", hours_to_expire: int = 24):
        """Create a TickerClassifier instance.

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
        self.sectors = EquitySectorLookup()

    @staticmethod
    def _normalize_token(value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", (value or "").upper())

    def _search_query_candidates(self, symbol: str) -> List[str]:
        normalized = (symbol or "").strip().upper()
        if not normalized:
            return []

        candidates = [normalized]
        if normalized.isalpha() and 6 <= len(normalized) <= 12:
            # Generic fallback for concatenated index names (e.g. HANGSENG).
            for idx in range(3, len(normalized) - 2):
                candidates.append(f"{normalized[:idx]} {normalized[idx:]}")

        # Deduplicate while preserving order.
        seen = set()
        unique: List[str] = []
        for cand in candidates:
            if cand in seen:
                continue
            seen.add(cand)
            unique.append(cand)
        return unique

    def _score_search_quote(self, symbol: str, quote: Dict, query: str) -> float:
        qtype = str(quote.get("quoteType") or "").upper()
        symbol_norm = self._normalize_token(symbol)
        query_norm = self._normalize_token(query)
        found_symbol = self._normalize_token(str(quote.get("symbol") or ""))
        name = (
            str(quote.get("shortname") or "")
            or str(quote.get("longname") or "")
            or str(quote.get("name") or "")
        )
        name_norm = self._normalize_token(name)

        score = 0.0
        if qtype == "INDEX":
            score += 8.0
        elif qtype == "FUTURE":
            score += 7.0
        elif qtype == "ETF":
            score += 6.0
        elif qtype == "EQUITY":
            score += 5.0
        elif qtype == "CRYPTOCURRENCY":
            score += 4.0
        else:
            score -= 2.0

        if query_norm and name_norm and query_norm in name_norm:
            score += 3.0
        if symbol_norm and name_norm and symbol_norm in name_norm:
            score += 2.5
        if query_norm and found_symbol and query_norm in found_symbol:
            score += 2.0

        raw_mcap = quote.get("marketCap")
        if isinstance(raw_mcap, (int, float)) and raw_mcap > 0:
            score += 0.5

        return score

    def _best_search_quote(
        self, symbol: str, quotes: List[Dict], query: str
    ) -> Dict | None:
        best: tuple[float, Dict] | None = None
        for quote in quotes:
            score = self._score_search_quote(symbol, quote, query)
            if best is None or score > best[0]:
                best = (score, quote)
        return best[1] if best else None

    def _build_from_yahoo_info(
        self, symbol: str, info: Dict, *, lookup_symbol: str, source: str
    ) -> Dict:
        qtype = str(info.get("quoteType") or "UNKNOWN").upper()
        market_cap = info.get("marketCap", 0)

        company_profile = {}
        if qtype in ["EQUITY", "ETF"]:
            company_profile = self.sectors.get_profile(lookup_symbol)

        sector = info.get("sector") or info.get("sectorDisp")
        industry = info.get("industry") or info.get("industryDisp")
        if qtype in ["EQUITY", "ETF"] and (not sector or not industry):
            if not sector:
                sector = company_profile.get("sector")
            if not industry:
                industry = company_profile.get("industry")

        return {
            "category": qtype,
            "ticker": symbol,
            "name": info.get("shortName")
            or info.get("longName")
            or info.get("displayName"),
            "market_cap": market_cap,
            "sector": sector,
            "industry": industry,
            "company_profile": company_profile or None,
            "yahoo_lookup": lookup_symbol,
            "alternatives": [],
            "source": source,
        }

    @staticmethod
    def _is_equity_like_quote(info: Dict) -> bool:
        qtype = str(info.get("quoteType") or "").upper()
        return qtype in {"EQUITY", "ETF", "INDEX", "FUTURE", "MUTUALFUND"}

    def _resolve_unknown_sync(self, symbol: str) -> Dict | None:
        best_score = float("-inf")
        best_symbol = ""
        for query in self._search_query_candidates(symbol):
            quotes = self.yahoo.search_quotes_sync(query, quotes_count=12)
            if not quotes:
                continue

            for quote in quotes:
                score = self._score_search_quote(symbol, quote, query)
                lookup_symbol = str(quote.get("symbol") or "").strip().upper()
                if not lookup_symbol:
                    continue
                if score > best_score:
                    best_score = score
                    best_symbol = lookup_symbol

        if not best_symbol:
            return None

        quote_map = self.yahoo.get_quotes_sync([best_symbol])
        info = quote_map.get(best_symbol)
        if not info:
            return None

        qtype = str(info.get("quoteType") or "").upper()
        if qtype in {"INDEX", "FUTURE", "ETF", "EQUITY", "MUTUALFUND"}:
            return self._build_from_yahoo_info(
                symbol, info, lookup_symbol=best_symbol, source="api-search"
            )

        return None

    async def _resolve_unknown_async(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> Dict | None:
        best_score = float("-inf")
        best_symbol = ""
        for query in self._search_query_candidates(symbol):
            quotes = await self.yahoo.search_quotes_async(
                session, query, quotes_count=12
            )
            if not quotes:
                continue

            for quote in quotes:
                score = self._score_search_quote(symbol, quote, query)
                lookup_symbol = str(quote.get("symbol") or "").strip().upper()
                if not lookup_symbol:
                    continue
                if score > best_score:
                    best_score = score
                    best_symbol = lookup_symbol

        if not best_symbol:
            return None

        quote_map = await self.yahoo.get_quotes_async(session, [best_symbol])
        info = quote_map.get(best_symbol)
        if not info:
            return None

        qtype = str(info.get("quoteType") or "").upper()
        if qtype in {"INDEX", "FUTURE", "ETF", "EQUITY", "MUTUALFUND"}:
            return self._build_from_yahoo_info(
                symbol, info, lookup_symbol=best_symbol, source="api-search"
            )

        return None

    def _hydrate_equity_metadata(self, symbol: str, item: Dict) -> Dict:
        """Backfill profile metadata for cached equity-like rows when missing."""
        category = str(item.get("category", "")).upper()
        if category not in {"EQUITY", "ETF"}:
            return item

        has_sector = bool(item.get("sector"))
        has_industry = bool(item.get("industry"))
        has_profile = isinstance(item.get("company_profile"), dict)

        if has_sector and has_industry and has_profile:
            return item

        profile = self.sectors.get_profile(symbol)
        if not profile:
            return item

        updated = dict(item)
        updated_profile = dict(updated.get("company_profile") or {})
        updated_profile.update(profile)

        if not updated.get("sector"):
            updated["sector"] = updated_profile.get("sector")
        if not updated.get("industry"):
            updated["industry"] = updated_profile.get("industry")
        updated["company_profile"] = updated_profile
        return updated

    @staticmethod
    def _should_refresh_cached_crypto_alias(symbol: str, item: Dict) -> bool:
        """Return True when a cached crypto entry likely came from a name alias.

        Examples: BITCOIN, ETHEREUM. These should map to canonical symbols
        (BTC/ETH) and can be stale in older caches.
        """

        category = str(item.get("category") or "").upper()
        if category != "CRYPTO":
            return False

        cached_ticker = str(item.get("ticker") or "").upper()
        normalized = str(symbol or "").upper()
        if cached_ticker != normalized:
            return False

        # Generic heuristic: long alphabetic tokens are often names, not
        # canonical crypto symbols.
        return normalized.isalpha() and len(normalized) >= 6

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
            `yahoo_lookup`, plus optional equity metadata (`sector`,
            `industry`, `company_profile`).
        """
        processed = {}
        # Init structure
        duel = {
            s: {"stock": 0, "crypto": 0, "forex": 0, "details": {}} for s in to_process
        }

        # 1 MILLION USD THRESHOLD
        # If a crypto is smaller than this, we treat it as "Noise" if it clashes with a stock ticker.
        MIN_CRYPTO_MCAP = 1_000_000

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
                company_profile = {}
                if qtype in ["EQUITY", "ETF"]:
                    company_profile = self.sectors.get_profile(sym)

                sector = info.get("sector") or info.get("sectorDisp")
                industry = info.get("industry") or info.get("industryDisp")

                if qtype in ["EQUITY", "ETF"] and (not sector or not industry):
                    db_sector = company_profile.get("sector")
                    db_industry = company_profile.get("industry")
                    if not sector:
                        sector = db_sector
                    if not industry:
                        industry = db_industry

                # Boost logic
                if qtype == "INDEX":
                    score = 50_000_000_000
                if qtype == "FUTURE":
                    score = 10_000_000_000
                if qtype == "ETF" and score < 100_000_000:
                    score = 100_000_000

                # If we found a valid stock object but mcap is missing/0,
                # give it a base score so it beats tiny cryptos.
                if score == 0 and qtype in ["EQUITY", "ETF"]:
                    score = 250_000  # Assume at least micro-cap stock

                duel[sym]["stock"] = score
                duel[sym]["details"]["stock"] = {
                    "type": qtype,
                    "name": info.get("shortName") or info.get("longName"),
                    "market_cap": raw_mcap,
                    "sector": sector,
                    "industry": industry,
                    "company_profile": company_profile or None,
                }

            # 3. Crypto Data
            if sym in crypto_data:
                info = crypto_data[sym]
                mcap = info.get("market_cap", 0)
                duel[sym]["crypto"] = mcap
                duel[sym]["details"]["crypto"] = {
                    "type": "Crypto",
                    "name": info.get("name"),
                    "symbol": str(info.get("symbol") or "").upper() or sym,
                    "market_cap": mcap,
                }

            # 4. Resolve
            scores = duel[sym]
            winner = max(["stock", "crypto", "forex"], key=lambda k: scores[k])

            # If Crypto won, but it's tiny (< $1M), and we tried to look up a Stock...
            # It's highly likely this is a "Fake" token or the Yahoo lookup failed.
            if winner == "crypto":
                mcap = scores["crypto"]
                if mcap < MIN_CRYPTO_MCAP:
                    # If the stock score was 0 (Yahoo failed), we'd rather return "Unknown"
                    # than return a $1,000 junk token for "NVDA".
                    winner = "unknown"

            # Construct Result
            if winner == "unknown" or scores[winner] == 0:
                final = {"category": "Unknown", "ticker": sym}
            else:
                details = scores["details"].get(winner, {})
                alternatives = [
                    k
                    for k in ["stock", "crypto", "forex"]
                    if scores[k] > 0 and k != winner
                ]

                ticker_out = sym
                y_look = sym
                if winner == "crypto":
                    ticker_out = str(details.get("symbol") or sym).upper()
                    y_look = f"{ticker_out}-USD"
                elif winner == "forex":
                    y_look = f"{sym}USD=X"

                final = {
                    "category": winner if winner != "stock" else details.get("type"),
                    "ticker": ticker_out,
                    "name": details.get("name"),
                    "market_cap": details.get("market_cap"),
                    "sector": details.get("sector"),
                    "industry": details.get("industry"),
                    "company_profile": details.get("company_profile"),
                    "yahoo_lookup": y_look,
                    "alternatives": alternatives,
                    "source": "api",
                }
            processed[sym] = final
        return processed

    def classify(self, symbols: List[str]) -> List[Dict]:
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
        cache_updates = {}
        cached_crypto = {}

        # Cache check
        cached = self.cache.get_many(unique)
        for sym in unique:
            if sym in SHORTCUTS:
                results_map[sym] = {**SHORTCUTS[sym], "source": "shortcut"}
            elif sym in cached:
                cached_item = cached[sym]
                if str(cached_item.get("category") or "").upper() == "UNKNOWN":
                    to_process.append(sym)
                    continue
                if self._should_refresh_cached_crypto_alias(sym, cached_item):
                    to_process.append(sym)
                    continue

                if str(cached_item.get("category") or "").upper() == "CRYPTO":
                    cached_crypto[sym] = cached_item
                    continue

                hydrated = self._hydrate_equity_metadata(sym, cached_item)
                results_map[sym] = hydrated
                if hydrated != cached_item:
                    cache_updates[sym] = hydrated
            else:
                to_process.append(sym)

        if cached_crypto:
            crypto_yahoo = self.yahoo.get_quotes_sync(list(cached_crypto))
            for sym, cached_item in cached_crypto.items():
                info = crypto_yahoo.get(sym)
                if info and self._is_equity_like_quote(info):
                    to_process.append(sym)
                    continue

                hydrated = self._hydrate_equity_metadata(sym, cached_item)
                results_map[sym] = hydrated
                if hydrated != cached_item:
                    cache_updates[sym] = hydrated

        if cache_updates:
            self.cache.save_many(cache_updates)

        if to_process:
            y_res = self.yahoo.get_quotes_sync(to_process)
            c_res = self.cg.get_prices_sync(to_process)
            processed = self._process_duel(to_process, y_res, c_res)

            unknowns = [
                sym
                for sym, item in processed.items()
                if str(item.get("category") or "").upper() == "UNKNOWN"
            ]
            for sym in unknowns:
                resolved = self._resolve_unknown_sync(sym)
                if resolved:
                    processed[sym] = resolved

            self.cache.save_many(processed)
            results_map.update(processed)

        return [results_map.get(s.upper().strip()) for s in symbols]

    async def classify_async(self, symbols: List[str]) -> List[Dict]:
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
        cache_updates = {}
        cached_crypto = {}

        # Cache Read (Run in thread to avoid blocking loop)
        loop = asyncio.get_running_loop()
        cached = await loop.run_in_executor(None, self.cache.get_many, unique)

        for sym in unique:
            if sym in SHORTCUTS:
                results_map[sym] = {**SHORTCUTS[sym], "source": "shortcut"}
            elif sym in cached:
                cached_item = cached[sym]
                if str(cached_item.get("category") or "").upper() == "UNKNOWN":
                    to_process.append(sym)
                    continue
                if self._should_refresh_cached_crypto_alias(sym, cached_item):
                    to_process.append(sym)
                    continue

                if str(cached_item.get("category") or "").upper() == "CRYPTO":
                    cached_crypto[sym] = cached_item
                    continue

                hydrated = self._hydrate_equity_metadata(sym, cached_item)
                results_map[sym] = hydrated
                if hydrated != cached_item:
                    cache_updates[sym] = hydrated
            else:
                to_process.append(sym)

        if cached_crypto:
            crypto_yahoo = await loop.run_in_executor(
                None, self.yahoo.get_quotes_sync, list(cached_crypto)
            )
            for sym, cached_item in cached_crypto.items():
                info = crypto_yahoo.get(sym)
                if info and self._is_equity_like_quote(info):
                    to_process.append(sym)
                    continue

                hydrated = self._hydrate_equity_metadata(sym, cached_item)
                results_map[sym] = hydrated
                if hydrated != cached_item:
                    cache_updates[sym] = hydrated

        if cache_updates:
            await loop.run_in_executor(None, self.cache.save_many, cache_updates)

        if to_process:
            async with aiohttp.ClientSession() as session:
                task_y = self.yahoo.get_quotes_async(session, to_process)
                task_c = self.cg.get_prices_async(session, to_process)
                y_res, c_res = await asyncio.gather(task_y, task_c)

                processed = self._process_duel(to_process, y_res, c_res)
                unknowns = [
                    sym
                    for sym, item in processed.items()
                    if str(item.get("category") or "").upper() == "UNKNOWN"
                ]
                for sym in unknowns:
                    resolved = await self._resolve_unknown_async(session, sym)
                    if resolved:
                        processed[sym] = resolved

            # Cache Write (Run in thread)
            await loop.run_in_executor(None, self.cache.save_many, processed)
            results_map.update(processed)

        return [results_map.get(s.upper().strip()) for s in symbols]
