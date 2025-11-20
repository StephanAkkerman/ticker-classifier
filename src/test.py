import requests


def fetch_coingecko_crypto_bases() -> set[str]:
    """
    Get all crypto base symbols (BTC, ETH, etc.) from CoinGecko.
    """
    url = "https://api.coingecko.com/api/v3/coins/list"
    # url = "https://api.coingecko.com/api/v3//coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    bases = {coin["symbol"].upper() for coin in data if "symbol" in coin}
    return bases


print(len(fetch_coingecko_crypto_bases()))
