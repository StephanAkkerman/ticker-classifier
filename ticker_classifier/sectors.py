"""Equity sector and industry lookup utilities.

This module provides a lazy-loaded lookup for equity sector metadata,
using ``financedatabase`` when available.
"""

from typing import Optional, Tuple


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

    def get(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Return ``(sector, industry)`` for ``symbol`` when available."""
        sym = str(symbol or "").strip().upper()
        if not sym:
            return None, None

        self._load()
        if self._data is None or sym not in self._data.index:
            return None, None

        row = self._data.loc[sym]

        # Multiple listings can share the same symbol; prefer US row if present.
        if getattr(row, "ndim", 1) == 2:
            rows = row
            if "country" in rows.columns:
                us_rows = rows[rows["country"] == "United States"]
                if not us_rows.empty:
                    rows = us_rows
            row = rows.iloc[0]

        sector = _clean_text(getattr(row, "get", lambda *_: None)("sector"))
        industry = _clean_text(getattr(row, "get", lambda *_: None)("industry"))
        return sector, industry


if __name__ == "__main__":
    lookup = EquitySectorLookup()
    for sym in ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "TSLA", "BRK.A", "V", "JPM"]:
        sector, industry = lookup.get(sym)
        print(f"{sym}: sector={sector}, industry={industry}")
