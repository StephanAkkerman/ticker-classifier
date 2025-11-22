import yfinance as yf
from pycoingecko import CoinGeckoAPI


class MarketClassifier:
    def __init__(self):
        self.cg = CoinGeckoAPI()

        # HARDCODED COMMODITIES / SHORTCUTS
        self.shortcuts = {
            "GOLD": {"type": "Commodity", "tik": "GC=F"},
            "SILVER": {"type": "Commodity", "tik": "SI=F"},
            "OIL": {"type": "Commodity", "tik": "CL=F"},
            "SPX": {"type": "Index", "tik": "^GSPC"},
            "NDX": {"type": "Index", "tik": "^IXIC"},
        }

        self.major_forex = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF"}

        self.minor_forex = {
            "AED",
            "AFN",
            "ALL",
            "AMD",
            "ANG",
            "AOA",
            "ARS",
            "AWG",
            "AZN",
            "BAM",
            "BBD",
            "BDT",
            "BGN",
            "BHD",
            "BIF",
            "BMD",
            "BND",
            "BOB",
            "BRL",
            "BSD",
            "BTN",
            "BWP",
            "BYN",
            "BZD",
            "CLP",
            "COP",
            "CRC",
            "CUP",
            "CVE",
            "CZK",
            "DJF",
            "DKK",
            "DOP",
            "DZD",
            "EGP",
            "ERN",
            "ETB",
            "FJD",
            "FKP",
            "GEL",
            "GHS",
            "GIP",
            "GMD",
            "GNF",
            "GTQ",
            "GYD",
            "HNL",
            "HRK",
            "HTG",
            "HUF",
            "IDR",
            "ILS",
            "INR",
            "IQD",
            "IRR",
            "ISK",
            "JMD",
            "JOD",
            "KES",
            "KGS",
            "KHR",
            "KMF",
            "KPW",
            "KRW",
            "KWD",
            "KYD",
            "KZT",
            "LAK",
            "LBP",
            "LKR",
            "LRD",
            "LSL",
            "LYD",
            "MAD",
            "MDL",
            "MGA",
            "MKD",
            "MMK",
            "MNT",
            "MOP",
            "MRU",
            "MUR",
            "MVR",
            "MWK",
            "MXN",
            "MYR",
            "MZN",
            "NAD",
            "NGN",
            "NIO",
            "NOK",
            "NPR",
            "OMR",
            "PAB",
            "PEN",
            "PGK",
            "PHP",
            "PKR",
            "PLN",
            "PYG",
            "QAR",
            "RON",
            "RSD",
            "RUB",
            "RWF",
            "SAR",
            "SBD",
            "SCR",
            "SDG",
            "SEK",
            "SGD",
            "SHP",
            "SLL",
            "SOS",
            "SRD",
            "SSP",
            "STN",
            "SYP",
            "SZL",
            "THB",
            "TJS",
            "TMT",
            "TND",
            "TOP",
            "TRY",
            "TTD",
            "TWD",
            "TZS",
            "UAH",
            "UGX",
            "UYU",
            "UZS",
            "VES",
            "VND",
            "VUV",
            "WST",
            "XAF",
            "XCD",
            "XOF",
            "XPF",
            "YER",
            "ZAR",
            "ZMW",
        }

    def classify(self, symbol):
        symbol = symbol.upper().strip()

        # 1. Check Shortcuts first
        if symbol in self.shortcuts:
            return self.shortcuts[symbol]

        scores = {"stock": 0, "crypto": 0, "forex": 0}
        details = {}

        # 2. Forex Check
        if symbol in self.major_forex:
            scores["forex"] = 100_000_000_000_000  # Huge bias for major currencies
        elif symbol in self.minor_forex:
            scores["forex"] = (
                50_000_000  # 50 Million (Beats micro-caps, loses to real stocks)
            )

        # 3. Stock/ETF/Index Check
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            qtype = info.get("quoteType")

            if qtype in ["EQUITY", "ETF", "INDEX", "MUTUALFUND", "FUTURE"]:
                mcap = info.get("marketCap", 0)

                # Artificial boosts for non-market-cap assets
                if qtype == "INDEX":
                    mcap = 50_000_000_000
                if qtype == "FUTURE":
                    mcap = 10_000_000_000

                scores["stock"] = mcap
                details["stock"] = {"type": qtype, "name": info.get("shortName")}
        except:
            pass

        # 4. Crypto Check
        try:
            results = self.cg.search(query=symbol)
            for coin in results.get("coins", []):
                if coin["symbol"].upper() == symbol:
                    rank = coin.get("market_cap_rank")
                    # Simple Rank-to-Cap estimation
                    est_mcap = 0
                    if rank:
                        if rank <= 10:
                            est_mcap = 100_000_000_000
                        elif rank <= 500:
                            est_mcap = 100_000_000
                        else:
                            est_mcap = 500_000

                    if est_mcap > scores["crypto"]:
                        scores["crypto"] = est_mcap
                        details["crypto"] = {"type": "Crypto", "name": coin.get("name")}
        except:
            pass

        # 5. Resolve
        winner = max(scores, key=scores.get)

        if scores[winner] == 0:
            return {"type": "Unknown", "ticker": symbol}

        final_data = details.get(winner, {})

        # Add ambiguity flag
        alternatives = [k for k, v in scores.items() if v > 0 and k != winner]

        return {
            "category": (
                winner if winner != "stock" else final_data.get("type")
            ),  # Returns "ETF" or "EQUITY" specific
            "ticker": symbol,
            "name": final_data.get("name"),
            "yahoo_lookup": symbol if winner != "crypto" else f"{symbol}-USD",
            "alternatives": alternatives,
        }


# --- TEST ---
resolver = MarketClassifier()
tests = [
    "NVDA",  # Stock
    "EUR",  # Major Forex
    "ALL",  # Edge Case: Allstate (Stock) vs Albanian Lek (Forex) -> Stock should win
    "PEPE",  # Crypto
    "CUBE",  # Edge Case: CubeSmart (Stock) vs Cuban Peso (Forex)
    "SPY",  # ETF
]

print(f"{'INPUT':<8} | {'WINNER':<10} | {'YAHOO LOOKUP'}")
print("-" * 40)
for t in tests:
    res = resolver.classify(t)
    print(res)
