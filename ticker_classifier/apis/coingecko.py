from collections import defaultdict
from typing import Dict, List

import aiohttp
import requests


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
            for coin in data:
                self._crypto_map[coin["symbol"].upper()].append(coin["id"])
        except Exception:
            self._crypto_map = {}

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
                for coin in data:
                    self._crypto_map[coin["symbol"].upper()].append(coin["id"])
        except Exception:
            self._crypto_map = {}

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

        for sym in symbols:
            if sym in self._crypto_map:
                # Top 10 collisions only
                for cid in self._crypto_map[sym][:10]:
                    ids.append(cid)
                    id_to_parent[cid] = sym
        return ids, id_to_parent

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
                    results[parent] = {
                        "market_cap": mcap,
                        "name": cid.title(),
                        "id": cid,
                    }
