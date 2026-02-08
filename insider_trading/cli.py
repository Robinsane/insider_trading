from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import Config, load_config
from .filters import basic_filters, is_material_trade, is_open_market_purchase, passes_position_increase, passes_security_title_filter
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


def enrich_market_cap(row: dict[str, Any], cfg: Config) -> None:
    symbol = row.get("ISSUERTRADINGSYMBOL")
    if not symbol:
        return
    shares_outstanding = row.get("shares_outstanding")
    if not shares_outstanding:
        return
    stooq_symbol = f"{symbol.lower()}{cfg.stooq_symbol_suffix}"
    price = fetch_last_close(stooq_symbol)
    if price is None:
        return
    row["last_close_usd"] = price
    row["market_cap_usd"] = round(price * float(shares_outstanding), 2)


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

    filtered.sort(key=lambda r: float(r.get("score") or 0), reverse=True)

    columns = select_output_columns(filtered)
    if not columns:
        print("No results after filtering.")
        return

    table = render_table(filtered[:25], columns[:12])
    print(table)

    out_path = cfg.output_dir / f"insider_trading_{date.today().isoformat()}.csv"
    write_csv(out_path, filtered, columns)
    print(f"\nWrote {len(filtered)} rows to {out_path}")


if __name__ == "__main__":
    main()
