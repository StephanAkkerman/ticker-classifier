from typing import Dict, List

import aiohttp
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
}


class YahooClient:
    def __init__(self):
        self.base_url = "https://query2.finance.yahoo.com/v7/finance/quote"

    def get_quotes_sync(self, symbols: List[str]) -> Dict[str, Dict]:
        results = {}
        if not symbols:
            return results

        # Yahoo accepts comma separated
        try:
            resp = requests.get(
                self.base_url,
                params={"symbols": ",".join(symbols)},
                headers=HEADERS,
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                if "quoteResponse" in data:
                    for item in data["quoteResponse"]["result"]:
                        results[item["symbol"].upper()] = item
        except Exception as e:
            print(f"Yahoo Sync Error: {e}")
        return results

    async def get_quotes_async(
        self, session: aiohttp.ClientSession, symbols: List[str]
    ) -> Dict[str, Dict]:
        results = {}
        if not symbols:
            return results

        try:
            async with session.get(
                self.base_url, params={"symbols": ",".join(symbols)}, headers=HEADERS
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "quoteResponse" in data:
                        for item in data["quoteResponse"]["result"]:
                            results[item["symbol"].upper()] = item
        except Exception as e:
            print(f"Yahoo Async Error: {e}")
        return results
