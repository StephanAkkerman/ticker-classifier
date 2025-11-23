from collections import defaultdict
from typing import Dict, List

import aiohttp
import requests


class CoinGeckoClient:
    def __init__(self):
        self.list_url = "https://api.coingecko.com/api/v3/coins/list"
        self.price_url = "https://api.coingecko.com/api/v3/simple/price"
        self._crypto_map = None  # { 'BTC': ['bitcoin', 'bitcoin-token'], ... }

    def _load_map_sync(self):
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

    def _get_candidate_ids(self, symbols: List[str]) -> (List[str], Dict[str, str]):
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
