# Insider Trading Tracker

Track interesting stocks based on insider trading metrics using free SEC data.

## What it does

- Downloads the latest available **SEC Form 3/4/5 insider transactions dataset** (quarterly)
- Filters for **open-market purchases** and material insider buys
- Computes **position increase**, **trade value**, and optional **market cap**
- Ranks candidates and outputs a console table + CSV
- Optional enrichment from `data.sec.gov` for SIC description and shares outstanding
- Optional market-cap estimation using free Stooq daily prices

This is a research tool for educational purposes. It is **not** investment advice.

## Quick start

```bash
uv init
uv python install

insider-trading run --days 30
```

Notes:
- `uv init` creates the uv project scaffolding and config.
- `uv python install` is optional if you already have a suitable Python.

This will download the most recent SEC Form 345 dataset into `data/` and produce:

- Console table output
- CSV in `outputs/`

To enable enrichment and tighter filtering:

```bash
insider-trading run --days 30 --enrich --require-market-cap
```

## Why SEC Form 345 data

The SEC publishes a **quarterly insider transactions dataset** extracted from Forms 3/4/5. It is flattened and easier to parse than raw filings, and is free to access. See `SEC Data Sources` below.

## Data sources

- **SEC Insider Transactions Data Sets (Form 345)** for insider trades
- **SEC data.sec.gov APIs** for company profile and shares outstanding (optional enrichment)
- **Stooq CSV endpoint** for last close price (optional market cap)

## Command reference

```bash
insider-trading run [--days N] [--since YYYY-MM-DD] [--quarter YYYYqQ]
                  [--require-market-cap] [--enrich] [--config config.toml]
```

### Flags

- `--days`: lookback in days based on transaction date (default 30)
- `--since`: explicit start date (overrides `--days`)
- `--quarter`: force a specific quarter, e.g. `2025q4`
- `--require-market-cap`: drop rows without computed market cap
- `--enrich`: fetch company facts (shares outstanding) and SIC description
- `--config`: path to a TOML config file

## Configuration

Create a `config.toml` (or use `config.example.toml`) to tune thresholds and weights.

### SEC user-agent setup

The SEC requires all automated access to include a descriptive `User-Agent` string with contact info.

Concrete steps:

1. Pick a label for your app, for example `InsiderTradingTracker`.
2. Choose a contact email you control.
3. Combine them into a single string, for example:
   `InsiderTradingTracker your.name@example.com`
4. Create `config.toml` by copying `config.example.toml`.
5. Update the `user_agent` field:

```toml
user_agent = "InsiderTradingTracker your.name@example.com"
```

If you do not set this, requests to `data.sec.gov` can fail or be rate-limited.

## Output columns

The CSV output is intentionally wide. You can remove columns you don't need.

## Notes on criteria mapping

- **Open market purchases**: `TRANS_CODE == 'P'` and `TRANS_ACQUIRED_DISP_CD == 'A'`
- **Materiality**: trade value >= threshold, position increase >= threshold
- **Information asymmetry**: industry keyword boost using SIC description (optional)
- **Cannibal trait**: share count reduction >= threshold (optional)

## SEC Data Sources

- Insider Transactions Data Sets (Form 345):
  - https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
- SEC data.sec.gov API documentation:
  - https://www.sec.gov/edgar/sec-api-documentation
- SEC access policies and user-agent requirement:
  - https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm

## Disclaimer

This software is for educational and research use only. It does not constitute financial advice.
