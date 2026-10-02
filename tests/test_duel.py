import asyncio

from ticker_classifier.apis.coingecko import CoinGeckoClient
from ticker_classifier.classifier import TickerClassifier


def _classifier(tmp_path) -> TickerClassifier:
    return TickerClassifier(db_name=str(tmp_path / "cache.db"))


def test_zero_mcap_etf_does_not_beat_real_crypto(tmp_path):
    # Grayscale's BTC trust is quoted on Yahoo as an ETF with marketCap 0.
    yahoo = {"BTC": {"quoteType": "ETF", "marketCap": 0, "shortName": "Grayscale"}}
    crypto = {"BTC": {"market_cap": 1.5e12, "name": "Bitcoin", "symbol": "BTC"}}

    result = _classifier(tmp_path)._process_duel(["BTC"], yahoo, crypto)

    assert result["BTC"]["category"] == "crypto"
    assert result["BTC"]["yahoo_lookup"] == "BTC-USD"


def test_failed_crypto_lookup_is_not_cached(tmp_path, monkeypatch):
    clf = _classifier(tmp_path)
    yahoo = {"BTC": {"quoteType": "ETF", "marketCap": 0, "shortName": "Grayscale"}}

    async def fake_yahoo(session, symbols):
        return yahoo

    async def fake_cg(session, symbols):
        clf.cg.lookup_failed = True  # what a swallowed CoinGecko error sets
        return {}

    monkeypatch.setattr(clf.yahoo, "get_quotes_async", fake_yahoo)
    monkeypatch.setattr(clf.cg, "get_prices_async", fake_cg)
    monkeypatch.setattr(clf.sectors, "get_profile", lambda s: {})

    result = asyncio.run(clf.classify_async(["BTC"]))

    assert result[0]["category"] == "ETF"  # best effort for this call...
    assert clf.cache.get_many(["BTC"]) == {}  # ...but never remembered


def test_coingecko_flags_failed_price_request():
    client = CoinGeckoClient()
    client._crypto_map = {"BTC": ["bitcoin"]}
    client._id_meta = {"bitcoin": {"symbol": "BTC", "name": "Bitcoin"}}
    client._name_map = {}
    client._tradingview_fallback_sync = lambda token: None

    import ticker_classifier.apis.coingecko as cg

    def boom(*a, **k):
        raise RuntimeError("429")

    orig = cg.requests.get
    cg.requests.get = boom
    try:
        assert client.get_prices_sync(["BTC"]) == {}
    finally:
        cg.requests.get = orig
    assert client.lookup_failed is True
