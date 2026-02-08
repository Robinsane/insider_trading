from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import Config, load_config
from .filters import basic_filters, is_material_trade, is_open_market_purchase, passes_position_increase, passes_security_title_filter
from .market_cap import fetch_market_cap_fmp, get_fmp_usage_count
from .parsing import read_tsv
from .report import render_table, write_csv
from .scoring import score_row
from .sec import (
    SecClient,
    company_facts,
    company_submissions,
    get_form345_dataset_for_quarter,
    get_latest_form345_dataset,
    load_or_fetch_json,
)
from .prices.stooq import fetch_last_close
from .prices.yahoo import fetch_last_price as fetch_last_price_yahoo
from .prices.yahoo import fetch_market_cap as fetch_market_cap_yahoo


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_sec_date(value: str) -> date | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_submission_index(dataset_dir: Path) -> dict[str, dict[str, str]]:
    submissions = {}
    for row in read_tsv(dataset_dir / "SUBMISSION.tsv"):
        submissions[row["ACCESSION_NUMBER"]] = row
    return submissions


def load_reporting_owners(dataset_dir: Path) -> dict[str, list[dict[str, str]]]:
    owners: dict[str, list[dict[str, str]]] = {}
    for row in read_tsv(dataset_dir / "REPORTINGOWNER.tsv"):
        owners.setdefault(row["ACCESSION_NUMBER"], []).append(row)
    return owners


def enrich_company_data(
    row: dict[str, Any],
    client: SecClient,
    cache_dir: Path,
    enable_enrich: bool,
    cfg: Config,
) -> None:
    if not enable_enrich:
        return

    cik = row.get("ISSUERCIK")
    if not cik:
        return

    sub = load_or_fetch_json(
        cache_dir,
        f"submissions_{cik}",
        lambda: company_submissions(client, cik),
    )
    sic = sub.get("sic")
    row["sic"] = sic
    row["sic_description"] = sub.get("sicDescription")
    tickers = sub.get("tickers")
    if isinstance(tickers, list):
        row["sec_tickers"] = tickers
    elif isinstance(tickers, str):
        row["sec_tickers"] = [tickers]

    facts = load_or_fetch_json(
        cache_dir,
        f"facts_{cik}",
        lambda: company_facts(client, cik),
    )

    try:
        shares = (
            facts["facts"]["dei"]["EntityCommonStockSharesOutstanding"]["units"][
                "shares"
            ]
        )
    except KeyError:
        return

    latest = None
    for item in shares:
        end = item.get("end")
        val = item.get("val")
        if end and val is not None:
            if latest is None or end > latest[0]:
                latest = (end, val)
    if latest:
        row["shares_outstanding"] = latest[1]

    # Cannibal trait: compare latest vs about a year ago
    if latest:
        latest_end = latest[0]
        latest_val = latest[1]
        one_year_ago = datetime.strptime(latest_end, "%Y-%m-%d").date() - timedelta(days=365)
        prior = None
        for item in shares:
            end = item.get("end")
            val = item.get("val")
            if end and val is not None:
                end_date = datetime.strptime(end, "%Y-%m-%d").date()
                if end_date <= one_year_ago:
                    if prior is None or end_date > prior[0]:
                        prior = (end_date, val)
        if prior and prior[1] > 0:
            reduction = (prior[1] - latest_val) / prior[1] * 100.0
            row["share_count_reduction_pct"] = round(reduction, 4)


def _symbol_variants(symbol: str) -> list[str]:
    base = (symbol or "").strip().upper()
    if not base:
        return []
    variants = [base]
    if "." in base:
        variants.append(base.replace(".", "-"))
    if "/" in base:
        variants.append(base.replace("/", "-"))
    # de-dupe while preserving order
    seen: set[str] = set()
    ordered: list[str] = []
    for item in variants:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _candidate_symbols(row: dict[str, Any]) -> list[str]:
    symbols: list[str] = []

    def add(value: str | None) -> None:
        if not value:
            return
        for variant in _symbol_variants(value):
            if variant not in symbols:
                symbols.append(variant)

    add(row.get("ISSUERTRADINGSYMBOL"))
    for ticker in row.get("sec_tickers") or []:
        add(ticker)
    return symbols


def _text_blob(row: dict[str, Any]) -> str:
    parts = [
        row.get("ISSUERNAME") or "",
        row.get("sic_description") or "",
    ]
    return " ".join(parts).upper()


def _guess_yahoo_suffixes(row: dict[str, Any]) -> list[str]:
    text = _text_blob(row)
    suffixes: list[str] = []

    def add(value: str) -> None:
        if value and value not in suffixes:
            suffixes.append(value)

    if any(token in text for token in ["PLC", "P.L.C", "LTD", "LIMITED"]):
        add(".L")
        add(".IR")
    if any(token in text for token in ["S.A.", "S A ", "SOCIEDAD ANONIMA", "SOCIETE ANONYME"]):
        add(".PA")
        add(".MC")
        add(".MI")
    if any(token in text for token in ["AG", "GMBH", "KGAA", "SE"]):
        add(".DE")
    if any(token in text for token in ["NV", "B.V."]):
        add(".AS")
    if "AB" in text:
        add(".ST")
    if "OYJ" in text:
        add(".HE")
    if any(token in text for token in ["A/S", "A S "]):
        add(".CO")
        add(".OL")
    if any(token in text for token in ["S.P.A", "S P A", "SPA"]):
        add(".MI")
    if "S.A.B." in text:
        add(".MX")

    defaults = [".L", ".TO", ".PA", ".AS", ".DE", ".SW"]
    for item in defaults:
        add(item)

    return suffixes


def _build_yahoo_symbols(symbols: list[str], row: dict[str, Any], max_attempts: int) -> list[str]:
    candidates: list[str] = []
    suffixes = _guess_yahoo_suffixes(row)

    def add(value: str) -> None:
        if value and value not in candidates:
            candidates.append(value)

    for symbol in symbols:
        if len(candidates) >= max_attempts:
            break
        if "." in symbol:
            add(symbol)
            continue
        add(symbol)
        for suffix in suffixes:
            if len(candidates) >= max_attempts:
                break
            add(f"{symbol}{suffix}")

    return candidates[:max_attempts]


def _fetch_price(symbols: list[str], row: dict[str, Any], cfg: Config) -> tuple[float | None, str | None, str | None]:
    for symbol in symbols:
        stooq_symbol = f"{symbol.lower()}{cfg.stooq_symbol_suffix}"
        price = fetch_last_close(stooq_symbol)
        if price is not None:
            return price, "stooq", symbol
    yahoo_symbols = _build_yahoo_symbols(symbols, row, max_attempts=6)
    for yahoo_symbol in yahoo_symbols:
        price = fetch_last_price_yahoo(yahoo_symbol, sleep_seconds=cfg.yahoo_sleep_seconds)
        if price is not None:
            return price, "yahoo", yahoo_symbol
    return None, None, None


def _fetch_market_cap(symbols: list[str], row: dict[str, Any], cfg: Config) -> tuple[float | None, str | None, str | None]:
    yahoo_symbols = _build_yahoo_symbols(symbols, row, max_attempts=6)
    for yahoo_symbol in yahoo_symbols:
        market_cap = fetch_market_cap_yahoo(yahoo_symbol, sleep_seconds=cfg.yahoo_sleep_seconds)
        if market_cap is not None:
            return market_cap, "yahoo", yahoo_symbol
    for symbol in symbols:
        usage_path = cfg.data_dir / "fmp_usage.json"
        market_cap = fetch_market_cap_fmp(symbol, cfg.fmp_api_key, usage_path=usage_path)
        if market_cap is not None:
            return market_cap, "fmp", symbol
    return None, None, None


def enrich_market_cap(row: dict[str, Any], cfg: Config) -> None:
    symbols = _candidate_symbols(row)
    if not symbols:
        return

    shares_outstanding = _float(row.get("shares_outstanding"))
    if shares_outstanding is not None:
        price, source, symbol = _fetch_price(symbols, row, cfg)
        if price is not None and source and symbol:
            row["last_close_usd"] = price
            row["market_cap_usd"] = round(price * float(shares_outstanding), 2)
            row["market_cap_source"] = source
            row["market_cap_symbol"] = symbol
            _warn_suspicious_market_cap(row)
            return

    market_cap, source, symbol = _fetch_market_cap(symbols, row, cfg)
    if market_cap is not None and source and symbol:
        row["market_cap_usd"] = round(market_cap, 2)
        row["market_cap_source"] = source
        row["market_cap_symbol"] = symbol
        _warn_suspicious_market_cap(row)


def _warn_suspicious_market_cap(row: dict[str, Any]) -> None:
    market_cap = _float(row.get("market_cap_usd"))
    trade_value = _float(row.get("trade_value"))
    if market_cap is None or trade_value is None:
        return
    if market_cap < trade_value:
        row["market_cap_warning"] = "market_cap_below_trade_value"


def build_trade_rows(
    dataset_dir: Path,
    cfg: Config,
    client: SecClient,
    enable_enrich: bool,
) -> list[dict[str, Any]]:
    submissions = load_submission_index(dataset_dir)
    owners = load_reporting_owners(dataset_dir)

    rows: list[dict[str, Any]] = []
    cache_dir = cfg.data_dir / "cache"

    for idx, row in enumerate(read_tsv(dataset_dir / "NONDERIV_TRANS.tsv"), start=1):
        acc = row.get("ACCESSION_NUMBER")
        sub = submissions.get(acc, {})
        for owner in owners.get(acc, [{}]):
            combined = dict(sub)
            combined.update(row)
            combined.update({
                "RPTOWNERCIK": owner.get("RPTOWNERCIK"),
                "RPTOWNERNAME": owner.get("RPTOWNERNAME"),
                "RPTOWNER_RELATIONSHIP": owner.get("RPTOWNER_RELATIONSHIP"),
                "RPTOWNER_TITLE": owner.get("RPTOWNER_TITLE"),
            })

            combined["filing_date"] = _parse_sec_date(sub.get("FILING_DATE"))
            combined["transaction_date"] = _parse_sec_date(row.get("TRANS_DATE"))

            shares = _float(row.get("TRANS_SHARES"))
            price = _float(row.get("TRANS_PRICEPERSHARE"))
            combined["trade_value"] = None
            if shares is not None and price is not None:
                combined["trade_value"] = round(shares * price, 2)

            shares_after = _float(row.get("SHRS_OWND_FOLWNG_TRANS"))
            if shares_after is not None and shares is not None:
                before = max(shares_after - shares, 0.0)
                if before > 0:
                    combined["position_increase_pct"] = round((shares / before) * 100.0, 4)

            # Early filter before expensive enrichment to keep runtime reasonable.
            if (
                is_open_market_purchase(combined, cfg)
                and passes_security_title_filter(combined, cfg)
                and is_material_trade(combined, cfg)
                and passes_position_increase(combined, cfg)
            ):
                enrich_company_data(combined, client, cache_dir, enable_enrich, cfg)
                enrich_market_cap(combined, cfg)

            rows.append(combined)

        if idx % 5000 == 0:
            print(f"Processed {idx} NONDERIV_TRANS rows...")

    return rows


def select_output_columns(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    preferred = [
        "score",
        "ISSUERTRADINGSYMBOL",
        "ISSUERNAME",
        "ISSUERCIK",
        "RPTOWNERNAME",
        "RPTOWNER_RELATIONSHIP",
        "RPTOWNER_TITLE",
        "TRANS_DATE",
        "TRANS_CODE",
        "TRANS_ACQUIRED_DISP_CD",
        "TRANS_SHARES",
        "TRANS_PRICEPERSHARE",
        "trade_value",
        "position_increase_pct",
        "shares_outstanding",
        "last_close_usd",
        "market_cap_usd",
        "market_cap_source",
        "market_cap_symbol",
        "market_cap_warning",
        "sic",
        "sic_description",
        "share_count_reduction_pct",
        "ACCESSION_NUMBER",
        "FILING_DATE",
    ]
    # add everything else to the end
    others = [c for c in rows[0].keys() if c not in preferred]
    return preferred + others


def main() -> None:
    parser = argparse.ArgumentParser(description="Insider trading tracker")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--quarter", type=str, default=None)
    parser.add_argument("--require-market-cap", action="store_true")
    parser.add_argument("--enrich", action="store_true")
    parser.add_argument("--config", type=str, default=None)

    args = parser.parse_args()
    cfg = load_config(Path(args.config) if args.config else None)

    if args.require_market_cap:
        cfg.require_market_cap = True

    client = SecClient(cfg.user_agent, sleep_seconds=cfg.sec_sleep_seconds)

    if args.quarter:
        year = int(args.quarter[:4])
        quarter = int(args.quarter[-1])
        dataset_dir = get_form345_dataset_for_quarter(client, cfg.data_dir, year, quarter)
    else:
        dataset_dir = get_latest_form345_dataset(client, cfg.data_dir)

    rows = build_trade_rows(dataset_dir, cfg, client, enable_enrich=args.enrich)

    if args.since:
        since = _parse_date(args.since)
    else:
        since = date.today() - timedelta(days=args.days)

    filtered = []
    for row in rows:
        tx_date = row.get("transaction_date")
        if tx_date and tx_date < since:
            continue
        if not basic_filters(row, cfg):
            continue
        row["score"] = score_row(row, cfg)
        filtered.append(row)

    def _sort_key(row: dict[str, Any]) -> tuple[int, float]:
        has_market_cap = 1 if row.get("market_cap_usd") is not None else 0
        return (has_market_cap, float(row.get("score") or 0))

    filtered.sort(key=_sort_key, reverse=True)

    columns = select_output_columns(filtered)
    if not columns:
        print("No results after filtering.")
        return

    table = render_table(filtered[:25], columns[:12])
    print(table)

    out_path = cfg.output_dir / f"insider_trading_{date.today().isoformat()}.csv"
    write_csv(out_path, filtered, columns)
    print(f"\nWrote {len(filtered)} rows to {out_path}")
    if cfg.fmp_api_key:
        usage_path = cfg.data_dir / "fmp_usage.json"
        used = get_fmp_usage_count(usage_path)
        print(f"FMP calls used today: {used}")


if __name__ == "__main__":
    main()
