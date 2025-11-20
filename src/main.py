from dataclasses import dataclass
from enum import Enum

import financedatabase as fd


def build_crypto_bases_from_fd() -> set[str]:
    """
    Build a set of crypto *base* symbols (BTC, ETH, SOL, ...) from
    FinanceDatabase crypto pairs, excluding fiat currencies like USD, EUR, etc.
    """
    cryptos = fd.Cryptos().select()
    currencies = fd.Currencies().select()
    currency_codes = {c.upper() for c in currencies.index}

    bases: set[str] = set()

    for pair in cryptos.index:
        # patterns like BTC-USD, ETH-USDT, etc.
        if "-" in pair:
            base, _quote = pair.split("-", 1)
            base_u = base.upper()
        else:
            # just in case there are bare symbols as well
            base_u = pair.upper()

        # don't treat fiat currencies as crypto
        if base_u in currency_codes:
            continue

        bases.add(base_u)

    return bases


class Universe:
    def __init__(self) -> None:
        self.equities = fd.Equities().select()
        self.etfs = fd.ETFs().select()
        self.funds = fd.Funds().select()
        self.indices = fd.Indices().select()
        self.currencies = fd.Currencies().select()
        self.moneymarkets = fd.Moneymarkets().select()

        # Symbol sets for quick membership tests
        self.eq_syms = set(self.equities.index)
        self.etf_syms = set(self.etfs.index)
        self.fund_syms = set(self.funds.index)
        self.index_syms = set(self.indices.index)
        self.curr_syms = set(self.currencies.index)
        self.crypto_syms = build_crypto_bases_from_fd()
        self.mm_syms = set(self.moneymarkets.index)


class AssetClass(str, Enum):
    STOCK = "stock"
    CRYPTO = "crypto"
    FOREX = "forex"
    INDEX = "index"
    ETF = "etf"
    FUND = "fund"
    MONEY_MARKET = "money_market"
    UNKNOWN = "unknown"


@dataclass
class ClassificationResult:
    primary: AssetClass
    candidates: list[AssetClass]


COMMON_CRYPTO_QUOTES = {"USD", "USDT", "USDC", "BTC", "EUR"}


def looks_like_crypto_pair(symbol: str, crypto_bases: set[str]) -> bool:
    """
    Detect BTC-USD, BTCUSD, BTC/USDT, etc., using known crypto bases.
    """
    s = symbol.upper()
    cleaned = s.replace("-", "").replace("/", "")
    for quote in COMMON_CRYPTO_QUOTES:
        if cleaned.endswith(quote):
            base = cleaned[: -len(quote)]
            if base in crypto_bases:
                return True
    return False


def classify_single(symbol: str, uni: Universe) -> ClassificationResult:
    s = symbol.strip().upper()
    hits: list[AssetClass] = []

    # direct matches
    if s in uni.eq_syms:
        hits.append(AssetClass.STOCK)
    if s in uni.etf_syms:
        hits.append(AssetClass.ETF)
    if s in uni.fund_syms:
        hits.append(AssetClass.FUND)
    if s in uni.index_syms:
        hits.append(AssetClass.INDEX)
    if s in uni.mm_syms:
        hits.append(AssetClass.MONEY_MARKET)

    # single-symbol crypto (but never fiat)
    if s in uni.crypto_syms and s not in uni.curr_syms:
        hits.append(AssetClass.CRYPTO)

    # crypto pair patterns (BTCUSD, BTC-USD, BTC/USDT)
    if looks_like_crypto_pair(s, uni.crypto_syms):
        hits.append(AssetClass.CRYPTO)

    # FX pattern: if it’s exactly 6 chars of 2 currency codes, e.g. EURUSD, GBPJPY
    s_clean = s.replace("/", "").replace("-", "")
    if len(s_clean) == 6:
        base, quote = s_clean[:3], s_clean[3:]
        if base in uni.curr_syms and quote in uni.curr_syms:
            hits.append(AssetClass.FOREX)

    if not hits:
        return ClassificationResult(AssetClass.UNKNOWN, [AssetClass.UNKNOWN])

    # dedupe while preserving order
    seen = set()
    hits_unique: list[AssetClass] = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            hits_unique.append(h)

    # priority: likeliest first (can tweak this)
    priority_order = [
        AssetClass.STOCK,
        AssetClass.ETF,
        AssetClass.FUND,
        AssetClass.INDEX,
        AssetClass.CRYPTO,
        AssetClass.MONEY_MARKET,
        AssetClass.FOREX,
    ]
    priority_index = {cls: i for i, cls in enumerate(priority_order)}
    hits_sorted = sorted(hits_unique, key=lambda c: priority_index.get(c, 999))

    primary = hits_sorted[0]
    return ClassificationResult(primary=primary, candidates=hits_sorted)


if __name__ == "__main__":
    test_symbols = ["AAPL", "BTC", "EURUSD", "GBPJPY", "USD", "INVALID"]
    universe = Universe()
    for sym in test_symbols:
        result = classify_single(sym, universe)
        print(
            f"Ticker: {sym:8s} -> primary={result.primary}, candidates={result.candidates}"
        )
