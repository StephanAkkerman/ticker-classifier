from typing import Any, Dict, List, Optional


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_best_crypto_row(
    rows: List[Dict[str, Any]], canonical_symbol: str
) -> Optional[Dict[str, Any]]:
    best: tuple[float, Dict[str, Any]] | None = None

    for row in rows:
        price = _to_float(row.get("close"))
        if price is None or price <= 0:
            continue

        row_type = str(row.get("type") or "").lower()
        row_symbol = str(row.get("symbol") or "").upper()
        volume = _to_float(row.get("volume"))

        score = 0.0
        if "crypto" in row_type:
            score += 5.0
        if volume is not None and volume > 0:
            score += 1.0
        if row_symbol.endswith(("USD", "USDT", "USDC")):
            score += 1.5
        if canonical_symbol and canonical_symbol in row_symbol:
            score += 1.0

        if best is None or score > best[0]:
            best = (score, row)

    return best[1] if best else None


def get_tradingview_crypto_fallback(
    token: str, canonical_symbol: str, display_name: str
) -> Optional[Dict[str, Any]]:
    """Return a best-effort crypto fallback payload using TradingView data."""

    canonical = str(canonical_symbol or "").upper().strip()
    if not canonical:
        return None

    try:
        from tradingview_scraper.symbols.symbol_markets import SymbolMarkets
    except Exception:
        return None

    markets = SymbolMarkets()
    rows: List[Dict[str, Any]] = []

    for candidate in (f"{canonical}USD", f"{canonical}USDT", canonical):
        for scanner in ("crypto", "global"):
            try:
                result = markets.scrape(symbol=candidate, scanner=scanner, limit=25)
            except TypeError:
                result = markets.scrape(symbol=candidate, limit=25)
            except Exception:
                continue

            data = result.get("data") if isinstance(result, dict) else result
            if isinstance(data, list):
                rows.extend([row for row in data if isinstance(row, dict)])

    best = _pick_best_crypto_row(rows, canonical)
    if not best:
        return None

    price = _to_float(best.get("close"))
    if price is None or price <= 0:
        return None

    volume = _to_float(best.get("volume"))
    # Synthetic cap used only for category confidence when CoinGecko is down.
    pseudo_market_cap = 2_000_000.0
    if volume is not None and volume > 0:
        pseudo_market_cap = max(pseudo_market_cap, volume * price)

    return {
        "market_cap": pseudo_market_cap,
        "name": display_name or str(token or canonical).title(),
        "symbol": canonical,
        "id": f"tradingview:{canonical.lower()}",
        "source": "tradingview-fallback",
    }
