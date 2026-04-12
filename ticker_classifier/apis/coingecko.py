import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional

import aiohttp
import requests

from .tradingview import get_tradingview_crypto_fallback


class CoinGeckoClient:
    def __init__(self):
        """Initialize CoinGecko client.

        Sets up base endpoints used for retrieving the coin list and simple
        price information and initializes an internal cache for symbol -> id
        mappings.

        Notes
        -----
        The client keeps an in-memory `_crypto_map` that maps uppercase
        symbols to a list of CoinGecko ids. This map is populated lazily by
        `_load_map_sync` or `_load_map_async` when price lookup is requested.
        """
        self.list_url = "https://api.coingecko.com/api/v3/coins/list"
        self.price_url = "https://api.coingecko.com/api/v3/simple/price"
        self._crypto_map = None  # { 'BTC': ['bitcoin', 'bitcoin-token'], ... }
        self._name_map = None  # { 'BITCOIN': ['bitcoin'], ... }
        self._id_meta = None  # { 'bitcoin': {'symbol': 'BTC', 'name': 'Bitcoin'}, ... }

    @staticmethod
    def _normalize_name(value: str) -> str:
        if not value:
            return ""
        return "".join(ch for ch in value.upper() if ch.isalnum())

    def _load_map_sync(self):
        """Load the CoinGecko symbol->id map synchronously.

        This method fetches the full coin list from the CoinGecko API and
        populates the in-memory `_crypto_map` mapping uppercase symbol strings
        to lists of CoinGecko ids. If the map is already loaded this is a
        no-op.

        Errors
        ------
        Any exceptions raised while fetching/parsing are caught and the map
        falls back to an empty dict.
        """
        if self._crypto_map:
            return
        try:
            resp = requests.get(self.list_url, timeout=10)
            data = resp.json()
            self._crypto_map = defaultdict(list)
            self._name_map = defaultdict(list)
            self._id_meta = {}
            for coin in data:
                coin_id = str(coin.get("id") or "").strip()
                if not coin_id:
                    continue

                symbol = str(coin.get("symbol") or "").upper().strip()
                name = str(coin.get("name") or "").strip()
                name_norm = self._normalize_name(name)

                if symbol:
                    self._crypto_map[symbol].append(coin_id)
                if name_norm:
                    self._name_map[name_norm].append(coin_id)

                self._id_meta[coin_id] = {
                    "symbol": symbol,
                    "name": name,
                }
        except Exception:
            self._crypto_map = {}
            self._name_map = {}
            self._id_meta = {}

    async def _load_map_async(self, session: aiohttp.ClientSession):
        """Asynchronously load the CoinGecko symbol->id map.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used for making the HTTP request.

        Notes
        -----
        This is the async counterpart to `_load_map_sync`. If the internal map
        is already populated this method returns immediately. Exceptions are
        caught and the map will be set to an empty dict on failure.
        """
        if self._crypto_map:
            return
        try:
            async with session.get(self.list_url) as resp:
                data = await resp.json()
                self._crypto_map = defaultdict(list)
                self._name_map = defaultdict(list)
                self._id_meta = {}
                for coin in data:
                    coin_id = str(coin.get("id") or "").strip()
                    if not coin_id:
                        continue

                    symbol = str(coin.get("symbol") or "").upper().strip()
                    name = str(coin.get("name") or "").strip()
                    name_norm = self._normalize_name(name)

                    if symbol:
                        self._crypto_map[symbol].append(coin_id)
                    if name_norm:
                        self._name_map[name_norm].append(coin_id)

                    self._id_meta[coin_id] = {
                        "symbol": symbol,
                        "name": name,
                    }
        except Exception:
            self._crypto_map = {}
            self._name_map = {}
            self._id_meta = {}

    def _get_candidate_ids(
        self, symbols: List[str]
    ) -> tuple[List[str], Dict[str, str]]:
        """Return candidate CoinGecko ids for a list of symbols.

        Parameters
        ----------
        symbols : list[str]
            Uppercase ticker symbols to map to CoinGecko ids.

        Returns
        -------
        ids : list[str]
            Flat list of candidate CoinGecko ids (limited to first 10
            collisions per symbol).
        id_to_parent : dict
            Mapping of coin id -> original symbol (parent) used to group
            results later.
        """
        ids = []
        id_to_parent = {}
        if not self._crypto_map:
            return ids, id_to_parent

        seen_ids = set()

        for sym in symbols:
            name_norm = self._normalize_name(sym)

            # Name-first fallback for tokens like "BITCOIN" (coin name) that are
            # not canonical ticker symbols.
            if self._name_map and name_norm in self._name_map:
                for cid in self._name_map[name_norm][:10]:
                    if cid in seen_ids:
                        continue
                    ids.append(cid)
                    id_to_parent[cid] = sym
                    seen_ids.add(cid)

            if sym in self._crypto_map:
                # Top 10 collisions only
                for cid in self._crypto_map[sym][:10]:
                    if cid in seen_ids:
                        continue
                    ids.append(cid)
                    id_to_parent[cid] = sym
                    seen_ids.add(cid)
        return ids, id_to_parent

    def _canonical_symbol_hint(self, token: str) -> str:
        symbol = str(token or "").upper().strip()
        if not symbol:
            return ""

        if self._crypto_map and symbol in self._crypto_map:
            return symbol

        name_norm = self._normalize_name(symbol)
        if self._name_map and name_norm in self._name_map:
            for cid in self._name_map[name_norm]:
                meta = (
                    self._id_meta.get(cid, {})
                    if isinstance(self._id_meta, dict)
                    else {}
                )
                canonical = str(meta.get("symbol") or "").upper().strip()
                if canonical:
                    return canonical

        return symbol

    def _name_hint(self, token: str, canonical_symbol: str) -> str:
        name_norm = self._normalize_name(token)
        if self._name_map and name_norm in self._name_map:
            for cid in self._name_map[name_norm]:
                meta = (
                    self._id_meta.get(cid, {})
                    if isinstance(self._id_meta, dict)
                    else {}
                )
                name = str(meta.get("name") or "").strip()
                if name:
                    return name

        return str(canonical_symbol or token).title()

    def _tradingview_fallback_sync(self, token: str) -> Optional[Dict[str, Any]]:
        canonical = self._canonical_symbol_hint(token)
        if not canonical:
            return None

        return get_tradingview_crypto_fallback(
            token=token,
            canonical_symbol=canonical,
            display_name=self._name_hint(token, canonical),
        )

    async def _tradingview_fallback_async(self, token: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(self._tradingview_fallback_sync, token)

    def _fill_missing_with_tradingview(
        self, symbols: List[str], results: Dict[str, Dict]
    ) -> None:
        for symbol in symbols:
            if symbol in results:
                continue
            fallback = self._tradingview_fallback_sync(symbol)
            if fallback:
                results[symbol] = fallback

    async def _fill_missing_with_tradingview_async(
        self, symbols: List[str], results: Dict[str, Dict]
    ) -> None:
        missing = [symbol for symbol in symbols if symbol not in results]
        if not missing:
            return

        tasks = [self._tradingview_fallback_async(symbol) for symbol in missing]
        fallback_results = await asyncio.gather(*tasks, return_exceptions=True)
        for symbol, payload in zip(missing, fallback_results):
            if isinstance(payload, dict):
                results[symbol] = payload

    def get_prices_sync(self, symbols: List[str]) -> Dict[str, Dict]:
        """Synchronous price lookup for a list of symbols using CoinGecko.

        This method ensures the internal symbol->id map is loaded, finds
        candidate CoinGecko ids for the requested symbols, and retrieves USD
        prices and market caps in chunks. The highest market cap candidate is
        selected per symbol in `_process_response`.

        Parameters
        ----------
        symbols : list[str]
            Uppercase ticker symbols to look up.

        Returns
        -------
        dict[str, dict]
            Mapping of symbol -> {"market_cap": ..., "name": ..., "id": ...}
            for matches found. Returns an empty dict if nothing matched.
        """
        self._load_map_sync()
        results = {}
        ids, id_map = self._get_candidate_ids(symbols)
        if not ids:
            self._fill_missing_with_tradingview(symbols, results)
            return results

        chunk_size = 200
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            try:
                resp = requests.get(
                    self.price_url,
                    params={
                        "ids": ",".join(chunk),
                        "vs_currencies": "usd",
                        "include_market_cap": "true",
                    },
                    timeout=10,
                )
                data = resp.json()
                self._process_response(data, id_map, results)
            except Exception:
                pass

        self._fill_missing_with_tradingview(symbols, results)
        return results

    async def get_prices_async(
        self, session: aiohttp.ClientSession, symbols: List[str]
    ) -> Dict[str, Dict]:
        """Asynchronously retrieve prices and market caps for symbols.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used to make HTTP requests.
        symbols : list[str]
            Uppercase ticker symbols to query.

        Returns
        -------
        dict[str, dict]
            Mapping of symbol -> {"market_cap": ..., "name": ..., "id": ...}.

        Notes
        -----
        Uses the async map loader `_load_map_async` and requests CoinGecko in
        chunks. Failures for a chunk are swallowed and processing continues.
        """
        await self._load_map_async(session)
        results = {}
        ids, id_map = self._get_candidate_ids(symbols)
        if not ids:
            await self._fill_missing_with_tradingview_async(symbols, results)
            return results

        chunk_size = 200
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            try:
                params = {
                    "ids": ",".join(chunk),
                    "vs_currencies": "usd",
                    "include_market_cap": "true",
                }
                async with session.get(self.price_url, params=params) as resp:
                    data = await resp.json()
                    self._process_response(data, id_map, results)
            except Exception:
                pass

        await self._fill_missing_with_tradingview_async(symbols, results)
        return results

    def _process_response(self, data, id_map, results):
        """Process a CoinGecko price response and update results.

        Parameters
        ----------
        data : dict
            JSON-decoded response from the CoinGecko simple/price endpoint.
        id_map : dict
            Mapping of coin id -> parent symbol used to group results.
        results : dict
            Mutable mapping that will be updated in-place with the best
            candidate per parent symbol (highest market cap wins).
        """
        for cid, val in data.items():
            parent = id_map.get(cid)
            if parent:
                mcap = val.get("usd_market_cap", 0)
                if mcap > results.get(parent, {}).get("market_cap", 0):
                    meta = (
                        self._id_meta.get(cid, {})
                        if isinstance(self._id_meta, dict)
                        else {}
                    )
                    results[parent] = {
                        "market_cap": mcap,
                        "name": meta.get("name") or cid.title(),
                        "symbol": (meta.get("symbol") or "").upper() or None,
                        "id": cid,
                    }
