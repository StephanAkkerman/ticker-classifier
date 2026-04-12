"""Equity metadata lookup utilities.

This module provides a lazy-loaded lookup for equity metadata sourced from
``financedatabase`` when available.
"""

from typing import Any, Dict, Optional, Tuple


def _clean_text(value: object) -> Optional[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    return text


class EquitySectorLookup:
    """Best-effort sector lookup for equity symbols.

    The lookup is lazy and optional: if ``financedatabase`` cannot be loaded,
    calls to :meth:`get` simply return ``(None, None)``.
    """

    def __init__(self):
        self._data = None
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return

        self._loaded = True
        try:
            import financedatabase as fd

            self._data = fd.Equities().data
        except Exception:
            self._data = None

    def _select_row(self, symbol: str):
        if self._data is None or symbol not in self._data.index:
            return None

        row = self._data.loc[symbol]

        # Multiple listings can share the same symbol; prefer US row if present.
        if getattr(row, "ndim", 1) == 2:
            rows = row
            if "country" in rows.columns:
                us_rows = rows[rows["country"] == "United States"]
                if not us_rows.empty:
                    rows = us_rows
            row = rows.iloc[0]

        return row

    def get_profile(self, symbol: str) -> Dict[str, str]:
        """Return a compact profile for ``symbol`` when available.

        The returned dictionary is empty when data is unavailable.
        """
        sym = str(symbol or "").strip().upper()
        if not sym:
            return {}

        self._load()
        row = self._select_row(sym)
        if row is None:
            return {}

        def _get(key: str) -> Optional[str]:
            return _clean_text(getattr(row, "get", lambda *_: None)(key))

        profile: Dict[str, Any] = {
            "sector": _get("sector"),
            "industry_group": _get("industry_group"),
            "industry": _get("industry"),
            "country": _get("country"),
            "exchange": _get("market") or _get("exchange"),
            "currency": _get("currency"),
            "website": _get("website"),
            "market_cap_category": _get("market_cap"),
        }

        return {k: v for k, v in profile.items() if v}

    def get(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Return ``(sector, industry)`` for ``symbol`` when available."""
        profile = self.get_profile(symbol)
        sector = profile.get("sector")
        industry = profile.get("industry")
        return sector, industry


if __name__ == "__main__":
    lookup = EquitySectorLookup()
    for sym in ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "TSLA", "BRK.A", "V", "JPM"]:
        print(f"{sym}: {lookup.get_profile(sym)}")
