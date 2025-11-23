"""Yahoo Finance API helpers.

This module provides a small client for obtaining the cookie/crumb
credentials required by Yahoo Finance endpoints and for fetching quote
data. Both synchronous (requests) and asynchronous (aiohttp) helpers
are provided.

Notes
-----
Docstrings follow the NumPy documentation style.
"""

from typing import Any, Dict, List, Optional

import aiohttp
import requests

API_BASE = "https://query2.finance.yahoo.com"
COOKIE_URL = "https://fc.yahoo.com"
CRUMB_URL = API_BASE + "/v1/test/getcrumb"
QUOTE_URL = API_BASE + "/v7/finance/quote"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


class YahooClient:
    def __init__(self):
        """Initialize a new :class:`YahooClient`.

        The client maintains an in-memory cache of authentication
        credentials (cookie + crumb) so repeated requests do not need to
        re-authenticate on every call.

        Returns
        -------
        None
        """

        self.credentials: Optional[Dict[str, Any]] = None

    def _get_credentials_sync(self):
        """Fetch cookie and crumb synchronously.

        The method performs the two-step Yahoo flow synchronously:
        1. Request the cookie from ``COOKIE_URL`` (allowing redirects).
        2. Request the crumb token from ``CRUMB_URL`` using the
           returned cookies.

        Returns
        -------
        dict or None
            Dictionary with keys ``'cookie'`` and ``'crumb'`` when
            successful, otherwise ``None``.
        """
        if self.credentials:
            return self.credentials

        try:
            # 1. Get Cookies (allow redirects)
            # fc.yahoo.com redirects to a consent page or main page, setting cookies along the way
            response_cookie = requests.get(COOKIE_URL, headers=HEADERS, timeout=5)
            cookies = response_cookie.cookies

            # 2. Get Crumb (using the cookies)
            response_crumb = requests.get(
                CRUMB_URL, headers=HEADERS, cookies=cookies, timeout=5
            )
            crumb = response_crumb.text

            if crumb:
                self.credentials = {"cookie": cookies, "crumb": crumb}
        except Exception as e:
            print(f"Yahoo Auth Error (Sync): {e}")

        return self.credentials

    def get_quotes_sync(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get quotes synchronously for the provided symbols.

        Parameters
        ----------
        symbols : list of str
            List of ticker symbols to query (e.g. ``['AAPL', 'MSFT']``).

        Returns
        -------
        dict
            Mapping from upper-case symbol to the quote data dictionary
            returned by Yahoo. Returns an empty dict if no symbols are
            provided or if a failure occurs.
        """

        results = {}
        if not symbols:
            return results

        creds = self._get_credentials_sync()
        if not creds:
            print("Skipping Yahoo Sync: No credentials.")
            return results

        try:
            params = {"symbols": ",".join(symbols), "crumb": creds["crumb"]}

            resp = requests.get(
                QUOTE_URL,
                params=params,
                cookies=creds["cookie"],
                headers=HEADERS,
                timeout=5,
            )

            if resp.status_code == 200:
                data = resp.json()
                if "quoteResponse" in data and "result" in data["quoteResponse"]:
                    for item in data["quoteResponse"]["result"]:
                        results[item["symbol"].upper()] = item
            elif resp.status_code == 401:
                # Credentials expired? Clear them for next time.
                self.credentials = None
                print("Yahoo 401 Unauthorized (Sync). Credentials cleared.")

        except Exception as e:
            print(f"Yahoo Sync Request Error: {e}")

        return results

    async def _get_credentials_async(self, session: aiohttp.ClientSession):
        """Fetch cookie and crumb asynchronously.

        Parameters
        ----------
        session : aiohttp.ClientSession
            An aiohttp session used to make the requests. The session's
            cookie jar will be used to collect cookies from Yahoo's
            redirects.

        Returns
        -------
        dict or None
            Dictionary with keys ``'cookie'`` and ``'crumb'`` when
            successful, otherwise ``None``.
        """
        if self.credentials:
            return self.credentials

        try:
            # 1. Get Cookies
            # aiohttp session handles cookies automatically in its cookie_jar if we share the session.
            # However, to be safe and explicit (since we pass cookies manually later), we extract them.
            async with session.get(COOKIE_URL, headers=HEADERS) as resp:
                await resp.read()  # Read body to ensure cookies are processed
                # Access cookies from the response history or the session cookie jar
                pass

            # 2. Get Crumb
            # The session now holds the cookies from step 1
            async with session.get(CRUMB_URL, headers=HEADERS) as resp:
                crumb = await resp.text()

                if crumb:
                    # We grab the cookies directly from the session's cookie jar to save them
                    # Dictionary comprehension to convert to standard dict
                    cookies = {
                        k: v.value
                        for k, v in session.cookie_jar.filter_cookies(CRUMB_URL).items()
                    }
                    self.credentials = {"cookie": cookies, "crumb": crumb}

        except Exception as e:
            print(f"Yahoo Auth Error (Async): {e}")

        return self.credentials

    async def get_quotes_async(
        self, session: aiohttp.ClientSession, symbols: List[str]
    ) -> Dict[str, Dict]:
        """Asynchronously get quotes for the provided symbols.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used to perform the requests.
        symbols : list of str
            List of ticker symbols to query.

        Returns
        -------
        dict
            Mapping from upper-case symbol to the quote data dictionary
            returned by Yahoo. Returns an empty dict if no symbols are
            provided or if a failure occurs.
        """

        results = {}
        if not symbols:
            return results

        creds = await self._get_credentials_async(session)
        if not creds:
            print("Skipping Yahoo Async: No credentials.")
            return results

        try:
            params = {"symbols": ",".join(symbols), "crumb": creds["crumb"]}

            # aiohttp allows passing cookies as a dict
            async with session.get(
                QUOTE_URL, params=params, cookies=creds["cookie"], headers=HEADERS
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "quoteResponse" in data and "result" in data["quoteResponse"]:
                        for item in data["quoteResponse"]["result"]:
                            results[item["symbol"].upper()] = item
                elif resp.status == 401:
                    self.credentials = None
                    print("Yahoo 401 Unauthorized (Async). Credentials cleared.")

        except Exception as e:
            print(f"Yahoo Async Request Error: {e}")

        return results
