# ticker-classifier

<!-- Add a banner here like: https://github.com/StephanAkkerman/fintwit-bot/blob/main/img/logo/fintwit-banner.png -->

---
<!-- Adjust the link of the first and second badges to your own repo -->
<p align="center">
  <img alt="GitHub Actions Workflow Status" src="https://img.shields.io/github/actions/workflow/status/StephanAkkerman/ticker-classifier/pyversions.yml?label=python%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13&logo=python&style=flat-square">
  <img src="https://img.shields.io/github/license/StephanAkkerman/ticker-classifier.svg?color=brightgreen" alt="License">
  <a href="https://github.com/psf/black"><img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black"></a>
</p>

## Introduction

`ticker-classifier` is a small Python library for classifying ticker-like symbols (for example `AAPL`, `BTC`, `EUR`, `GOLD`) into a simple market/category representation.
It uses Yahoo Finance for equities, CoinGecko for cryptocurrencies and a few heuristics for currencies/commodities. The output indicates the most likely category, a display name, market cap when available, and a `yahoo_lookup` value to fetch further data if desired.

## Table of Contents 🗂

- [Key Features](#key-features)
- [Installation](#installation)
- [Usage](#usage)
- [API](#api)
- [Development](#development)
- [Citation](#citation)
- [Contributing](#contributing)
- [License](#license)

## Key Features 🔑

- Classify symbols as `Equity`, `Crypto`, `Forex`, `Commodity`, `Index` or `Unknown`.
- Uses multiple public APIs and simple heuristics to make robust decisions.
- Provides both synchronous and asynchronous APIs.
- Lightweight disk cache to avoid repeated lookups (`TickerCache`).

## Installation ⚙️

Install from pip using the provided `requirements.txt` or install the package directly from the repository for latest changes:

```bash
pip install -r requirements.txt
```

or

```bash
pip install git+https://github.com/StephanAkkerman/ticker-classifier.git
```

## Usage ⌨️

Basic synchronous usage:

```python
from ticker_classifier.classifier import TickerClassifier

classifier = TickerClassifier()
symbols = ["AAPL", "BTC", "EUR", "GOLD", "UNKNOWN123"]
results = classifier.classify(symbols)
for r in results:
    print(r)
```

Example asynchronous usage:

```python
import asyncio
from ticker_classifier.classifier import TickerClassifier

async def main():
    classifier = TickerClassifier()
    symbols = ["AAPL", "BTC", "ETH", "JPY"]
    results = await classifier.classify_async(symbols)
    for r in results:
        print(r)

asyncio.run(main())
```

Notes
- The classifier caches positive classifications (non-`Unknown`) in an
SQLite database (default `ticker_cache.db`) for `24` hours by default.
- You can customize the cache filename and expiry by passing `db_name` and
`hours_to_expire` to `TickerClassifier`.

## API

- `ticker_classifier.classifier.TickerClassifier`
- `classify(symbols: List[str]) -> List[dict]` – synchronous classification.
- `classify_async(symbols: List[str]) -> List[dict]` – async classification.
- `ticker_classifier.apis.yahoo.YahooClient` – low-level Yahoo quote fetcher (sync + async helpers).
- `ticker_classifier.apis.coingecko.CoinGeckoClient` – crypto lookup + market cap helpers (sync + async).
- `ticker_classifier.db.cache.TickerCache` – tiny SQLite-backed cache used by `TickerClassifier`.

## Development

Run formatting and linting tools you prefer (project uses `black` code style).

Run a quick smoke check by running the `classifier.py` module directly:

```powershell
& .venv\Scripts\python.exe ticker_classifier\classifier.py
```

If you add tests, run them with your chosen test runner (e.g. `pytest`).

## Citation ✍️
If you use this project in your research, please cite as follows (adjust
metadata accordingly):

```bibtex
@misc{ticker-classifier,
author  = {Stephan Akkerman},
title   = {ticker-classifier},
year    = {2025},
publisher = {GitHub},
howpublished = {\url{https://github.com/StephanAkkerman/ticker-classifier}}
}
```

## Contributing 🛠

Contributions are welcome. Suggested workflow:

1. Fork the repository and create a feature branch.
2. Run tests and format your changes with `black`.
3. Open a pull request with a clear description of the change.

Please open issues for feature requests or bugs and include a small
reproducible example when possible.

![https://github.com/StephanAkkerman/ticker-classifier/graphs/contributors](https://contributors-img.firebaseapp.com/image?repo=StephanAkkerman/ticker-classifier)

## License 📜

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
