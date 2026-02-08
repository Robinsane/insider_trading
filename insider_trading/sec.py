from __future__ import annotations

import json
import time
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

SEC_BASE = "https://www.sec.gov"
DATASET_BASE = (
    "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"
)
DATA_API_BASE = "https://data.sec.gov"


@dataclass
class SecClient:
    user_agent: str
    sleep_seconds: float = 0.12

    def get(self, url: str, stream: bool = False) -> requests.Response:
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        resp = requests.get(url, headers=headers, timeout=60, stream=stream)
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        resp.raise_for_status()
        return resp

    def get_json(self, url: str) -> dict[str, Any]:
        return self.get(url).json()


def build_form345_url(year: int, quarter: int) -> str:
    return f"{DATASET_BASE}/{year}q{quarter}_form345.zip"


def download_form345_zip(client: SecClient, year: int, quarter: int, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = build_form345_url(year, quarter)
    zip_path = dest_dir / f"{year}q{quarter}_form345.zip"
    if zip_path.exists():
        return zip_path
    resp = client.get(url, stream=True)
    with zip_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return zip_path


def extract_form345(zip_path: Path, dest_dir: Path) -> Path:
    out_dir = dest_dir / zip_path.stem
    if out_dir.exists():
        return out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    return out_dir


def iter_recent_quarters(today: date, max_back: int = 8) -> list[tuple[int, int]]:
    year = today.year
    quarter = (today.month - 1) // 3 + 1
    items = []
    for _ in range(max_back):
        items.append((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return items


def get_latest_form345_dataset(client: SecClient, dest_dir: Path, today: date | None = None) -> Path:
    today = today or date.today()
    last_error = None
    for year, quarter in iter_recent_quarters(today):
        try:
            zip_path = download_form345_zip(client, year, quarter, dest_dir)
            return extract_form345(zip_path, dest_dir)
        except Exception as exc:  # pragma: no cover - network variability
            last_error = exc
            continue
    raise RuntimeError(f"Unable to download recent Form 345 datasets: {last_error}")


def cik_pad(cik: str) -> str:
    return str(cik).zfill(10)


def company_submissions(client: SecClient, cik: str) -> dict[str, Any]:
    url = f"{DATA_API_BASE}/submissions/CIK{cik_pad(cik)}.json"
    try:
        return client.get_json(url)
    except requests.HTTPError as exc:  # pragma: no cover - network variability
        if exc.response is not None and exc.response.status_code == 404:
            return {}
        raise


def company_facts(client: SecClient, cik: str) -> dict[str, Any]:
    url = f"{DATA_API_BASE}/api/xbrl/companyfacts/CIK{cik_pad(cik)}.json"
    try:
        return client.get_json(url)
    except requests.HTTPError as exc:  # pragma: no cover - network variability
        if exc.response is not None and exc.response.status_code == 404:
            return {}
        raise


def load_or_fetch_json(cache_dir: Path, cache_key: str, fetch_fn) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{cache_key}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    data = fetch_fn()
    cache_path.write_text(json.dumps(data), encoding="utf-8")
    return data


def get_form345_dataset_for_quarter(client: SecClient, dest_dir: Path, year: int, quarter: int) -> Path:
    zip_path = download_form345_zip(client, year, quarter, dest_dir)
    return extract_form345(zip_path, dest_dir)
