#!/usr/bin/env python3
"""Fetch Dubai DLD transaction data and compute area-level price trend signals.

The script reads the open Data Dubai DLD transactions endpoint and outputs
aggregated trend comparisons (recent window vs prior window) for selected areas.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BASE_URL = "https://data.dubai/o/dda/data-services/dataset-metadata"
DEFAULT_DATASET_ID = 470061
DEFAULT_PAGE_SIZE = 1000
DEFAULT_WORKERS = 16
DEFAULT_WINDOWS = (14, 30)

TEXT_FIELDS = (
    "master_project_en",
    "project_name_en",
    "building_name_en",
    "area_name_en",
)

SAFE_FIELDS = (
    "transaction_id",
    "instance_date",
    "trans_group_en",
    "reg_type_en",
    "master_project_en",
    "project_name_en",
    "building_name_en",
    "area_name_en",
    "property_type_en",
    "property_sub_type_en",
    "procedure_name_en",
    "actual_worth",
    "meter_sale_price",
    "procedure_area",
    "load_timestamp",
)

PRESETS: Dict[str, Sequence[str]] = {
    "damac-hills-2": (
        r"\bdamac\s*hills\s*2\b",
        r"\bdamac\s+islands\s*2\b",
    ),
    "emaar-south": (
        r"\bemaar\s*south\b",
        r"\bdubai\s*south\s*residential\s*district\b",
    ),
    "jlt": (
        r"\bjumeirah\s*lakes\s*towers\b",
        r"\bjlt\b",
    ),
    "reportage": (
        r"\breportage\b",
    ),
    "gemz-danube": (
        r"(?=.*\bgemz\b)(?=.*\bdanube\b)",
    ),
}


@dataclass
class TargetPattern:
    name: str
    regexes: List[re.Pattern[str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze Dubai DLD transaction trend signals for selected areas.",
    )
    parser.add_argument(
        "--dataset-id",
        type=int,
        default=DEFAULT_DATASET_ID,
        help=f"Dataset id on data.dubai (default: {DEFAULT_DATASET_ID})",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Rows requested per API page (default: {DEFAULT_PAGE_SIZE})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel workers for page fetch (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional hard cap for number of pages to scan.",
    )
    parser.add_argument(
        "--preset",
        action="append",
        default=[],
        help=(
            "Preset target set. Repeatable. "
            f"Available: {', '.join(sorted(PRESETS.keys()))}, or 'all'."
        ),
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help=(
            "Custom target in format name=regex1|regex2. "
            "Repeatable."
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Only include matches inside this many days from latest available date.",
    )
    parser.add_argument(
        "--windows",
        default=",".join(str(w) for w in DEFAULT_WINDOWS),
        help="Comparison windows in days, comma separated (default: 14,30).",
    )
    parser.add_argument(
        "--latest-date",
        default=None,
        help="Optional YYYY-MM-DD reference date. Defaults to max matched date.",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Write JSON summary to this path.",
    )
    parser.add_argument(
        "--output-markdown",
        default=None,
        help="Write markdown report to this path.",
    )
    parser.add_argument(
        "--output-matches",
        default=None,
        help="Write deduplicated matched transactions (JSONL) to this path.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress updates while fetching pages.",
    )
    return parser.parse_args()


def parse_windows(raw: str) -> List[int]:
    values: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value <= 0:
            raise ValueError("Window values must be positive integers.")
        values.append(value)
    if not values:
        raise ValueError("At least one window must be provided.")
    return sorted(set(values))


def parse_targets(presets: Sequence[str], custom_targets: Sequence[str]) -> List[TargetPattern]:
    target_map: Dict[str, List[str]] = {}

    use_all = not presets and not custom_targets
    selected_presets = list(presets)
    if use_all or any(p.lower() == "all" for p in selected_presets):
        selected_presets = sorted(PRESETS.keys())

    for preset in selected_presets:
        key = preset.lower().strip()
        if key == "all":
            continue
        if key not in PRESETS:
            raise ValueError(f"Unknown preset '{preset}'.")
        target_map.setdefault(key, []).extend(PRESETS[key])

    for raw in custom_targets:
        if "=" not in raw:
            raise ValueError(
                f"Invalid custom target '{raw}'. Use name=regex1|regex2 format."
            )
        name, expr = raw.split("=", 1)
        name = name.strip().lower().replace(" ", "-")
        patterns = [p.strip() for p in expr.split("|") if p.strip()]
        if not name or not patterns:
            raise ValueError(
                f"Invalid custom target '{raw}'. Use name=regex1|regex2 format."
            )
        target_map.setdefault(name, []).extend(patterns)

    if not target_map:
        raise ValueError("No targets were configured.")

    compiled: List[TargetPattern] = []
    for name, patterns in target_map.items():
        compiled.append(
            TargetPattern(
                name=name,
                regexes=[re.compile(pattern, re.IGNORECASE) for pattern in patterns],
            )
        )
    return sorted(compiled, key=lambda t: t.name)


def to_iso_date(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d")


def to_optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def fetch_json(url: str, retries: int = 3, timeout: int = 45) -> dict:
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                payload = resp.read().decode("utf-8")
            return json.loads(payload)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == retries - 1:
                break
    raise RuntimeError(f"Failed request after {retries} attempts: {url}\n{last_error}")


def build_page_url(dataset_id: int, page: int, page_size: int) -> str:
    params = {
        "datasetId": str(dataset_id),
        "page": str(page),
        "pageSize": str(page_size),
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(params)}"


def fetch_page(dataset_id: int, page: int, page_size: int) -> List[dict]:
    url = build_page_url(dataset_id, page, page_size)
    response = fetch_json(url)
    data = response.get("data")
    if isinstance(data, list):
        return data
    return []


def find_last_page(dataset_id: int, page_size: int, hard_cap: Optional[int]) -> int:
    if hard_cap is not None:
        return hard_cap

    def has_rows(page: int) -> bool:
        return bool(fetch_page(dataset_id, page, page_size))

    if not has_rows(1):
        return 0

    lo, hi = 1, 2
    while has_rows(hi):
        lo = hi
        hi *= 2
        if hi > 10000:
            break

    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if has_rows(mid):
            lo = mid
        else:
            hi = mid

    return lo


def matched_targets(row: dict, targets: Sequence[TargetPattern]) -> List[str]:
    haystack = " | ".join(str(row.get(field) or "") for field in TEXT_FIELDS)
    hits: List[str] = []
    for target in targets:
        for pattern in target.regexes:
            if pattern.search(haystack):
                hits.append(target.name)
                break
    return hits


def sanitize_row(row: dict, targets: List[str]) -> dict:
    clean = {field: row.get(field) for field in SAFE_FIELDS}
    clean["targets"] = sorted(set(targets))
    clean["actual_worth"] = to_optional_float(clean.get("actual_worth"))
    clean["meter_sale_price"] = to_optional_float(clean.get("meter_sale_price"))
    clean["procedure_area"] = to_optional_float(clean.get("procedure_area"))
    return clean


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def dedupe_transactions(rows: Iterable[dict]) -> List[dict]:
    best: Dict[str, dict] = {}
    synthetic_index = 0

    for row in rows:
        key = str(row.get("transaction_id") or "").strip()
        if not key:
            key = f"_synthetic_{synthetic_index}"
            synthetic_index += 1

        previous = best.get(key)
        if previous is None:
            best[key] = row
            continue

        prev_ts = parse_timestamp(previous.get("load_timestamp"))
        curr_ts = parse_timestamp(row.get("load_timestamp"))

        replace = False
        if prev_ts and curr_ts:
            replace = curr_ts > prev_ts
        elif curr_ts and not prev_ts:
            replace = True
        elif not prev_ts and not curr_ts:
            replace = str(row.get("instance_date") or "") > str(previous.get("instance_date") or "")

        if replace:
            merged_targets = sorted(set(previous.get("targets", [])) | set(row.get("targets", [])))
            row["targets"] = merged_targets
            best[key] = row
        else:
            previous["targets"] = sorted(
                set(previous.get("targets", [])) | set(row.get("targets", []))
            )

    return list(best.values())


def median_or_none(values: Iterable[Optional[float]]) -> Optional[float]:
    numeric = [v for v in values if isinstance(v, (float, int))]
    if not numeric:
        return None
    return float(statistics.median(numeric))


def pct_change(new_value: Optional[float], old_value: Optional[float]) -> Optional[float]:
    if new_value is None or old_value in (None, 0):
        return None
    return ((new_value - old_value) / old_value) * 100.0


def period_stats(rows: Sequence[dict], start: datetime, end: datetime, land_only: bool) -> dict:
    filtered = []
    for row in rows:
        try:
            row_date = to_iso_date(str(row.get("instance_date")))
        except ValueError:
            continue

        if not (start <= row_date <= end):
            continue

        if land_only and str(row.get("property_type_en") or "").strip().lower() != "land":
            continue

        filtered.append(row)

    return {
        "n": len(filtered),
        "median_meter_sale_price": median_or_none(r.get("meter_sale_price") for r in filtered),
        "median_actual_worth": median_or_none(r.get("actual_worth") for r in filtered),
    }


def fmt_number(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "NA"
    return f"{value:,.{decimals}f}"


def fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    return f"{value:+.1f}%"


def build_markdown(summary: dict) -> str:
    lines: List[str] = []
    lines.append("# Dubai DLD Price Trend Snapshot")
    lines.append("")
    lines.append(f"- Dataset ID: `{summary['dataset_id']}`")
    lines.append(f"- Latest matched transaction date: `{summary['latest_date']}`")
    lines.append(f"- Analysis lookback: `{summary['lookback_days']}` days")
    lines.append(f"- Pages scanned: `{summary['pages_scanned']}`")
    lines.append(f"- Sales rows matched before dedupe: `{summary['raw_matches']}`")
    lines.append(f"- Sales rows after dedupe: `{summary['deduped_matches']}`")
    lines.append("")

    for target_name, target_summary in summary["targets"].items():
        lines.append(f"## {target_name}")
        lines.append("")
        lines.append(f"- Matches in lookback window: `{target_summary['matches_in_lookback']}`")
        lines.append("")

        lines.append("### All Sales")
        lines.append("")
        lines.append("| Window | Recent n | Prior n | Median AED/m2 (recent) | Median AED/m2 (prior) | Delta AED/m2 | Median Value AED (recent) | Median Value AED (prior) | Delta Value |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

        for window in target_summary["all_sales"]["windows"]:
            recent = window["recent"]
            prior = window["prior"]
            lines.append(
                "| "
                f"{window['days']}d | {recent['n']} | {prior['n']} | "
                f"{fmt_number(recent['median_meter_sale_price'])} | {fmt_number(prior['median_meter_sale_price'])} | {fmt_pct(window['delta_meter_sale_price_pct'])} | "
                f"{fmt_number(recent['median_actual_worth'])} | {fmt_number(prior['median_actual_worth'])} | {fmt_pct(window['delta_actual_worth_pct'])} |"
            )

        land_windows = target_summary["land_sales"]["windows"]
        if any((w["recent"]["n"] + w["prior"]["n"]) > 0 for w in land_windows):
            lines.append("")
            lines.append("### Land-only Sales")
            lines.append("")
            lines.append("| Window | Recent n | Prior n | Median AED/m2 (recent) | Median AED/m2 (prior) | Delta AED/m2 | Median Value AED (recent) | Median Value AED (prior) | Delta Value |")
            lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
            for window in land_windows:
                recent = window["recent"]
                prior = window["prior"]
                lines.append(
                    "| "
                    f"{window['days']}d | {recent['n']} | {prior['n']} | "
                    f"{fmt_number(recent['median_meter_sale_price'])} | {fmt_number(prior['median_meter_sale_price'])} | {fmt_pct(window['delta_meter_sale_price_pct'])} | "
                    f"{fmt_number(recent['median_actual_worth'])} | {fmt_number(prior['median_actual_worth'])} | {fmt_pct(window['delta_actual_worth_pct'])} |"
                )

        lines.append("")

    lines.append("## Source")
    lines.append("")
    lines.append("- https://data.dubai/en/web/guest/l/470061")
    lines.append("- https://data.dubai/o/dda/data-services/dataset-metadata")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    try:
        windows = parse_windows(args.windows)
        targets = parse_targets(args.preset, args.target)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    last_page = find_last_page(args.dataset_id, args.page_size, args.max_pages)
    if last_page <= 0:
        print("[error] No pages found in dataset.", file=sys.stderr)
        return 3

    if args.verbose:
        print(f"[info] scanning pages 1..{last_page} with {args.workers} workers")

    raw_matches: List[dict] = []
    pages_scanned = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(fetch_page, args.dataset_id, page, args.page_size): page
            for page in range(1, last_page + 1)
        }
        for future in as_completed(futures):
            page = futures[future]
            rows = future.result()
            pages_scanned += 1

            for row in rows:
                if str(row.get("trans_group_en") or "").strip().lower() != "sales":
                    continue

                hits = matched_targets(row, targets)
                if not hits:
                    continue

                raw_matches.append(sanitize_row(row, hits))

            if args.verbose and (pages_scanned % 100 == 0 or pages_scanned == last_page):
                print(
                    f"[info] pages={pages_scanned}/{last_page} raw_matches={len(raw_matches)}"
                )

    if not raw_matches:
        print("[error] No matching sales transactions found for the selected targets.", file=sys.stderr)
        return 4

    deduped_rows = dedupe_transactions(raw_matches)

    if args.latest_date:
        latest_date = to_iso_date(args.latest_date)
    else:
        latest_date = max(to_iso_date(str(r["instance_date"])) for r in deduped_rows)

    cutoff_date = latest_date - timedelta(days=max(1, args.days) - 1)
    lookback_rows = [
        row for row in deduped_rows if to_iso_date(str(row["instance_date"])) >= cutoff_date
    ]

    if not lookback_rows:
        print("[error] No rows remain inside the lookback period.", file=sys.stderr)
        return 5

    target_summary: Dict[str, dict] = {}

    for target in targets:
        target_rows = [r for r in lookback_rows if target.name in r.get("targets", [])]

        all_windows = []
        land_windows = []

        for window_days in windows:
            recent_start = latest_date - timedelta(days=window_days - 1)
            recent_end = latest_date
            prior_start = recent_start - timedelta(days=window_days)
            prior_end = recent_start - timedelta(days=1)

            all_recent = period_stats(target_rows, recent_start, recent_end, land_only=False)
            all_prior = period_stats(target_rows, prior_start, prior_end, land_only=False)
            land_recent = period_stats(target_rows, recent_start, recent_end, land_only=True)
            land_prior = period_stats(target_rows, prior_start, prior_end, land_only=True)

            all_windows.append(
                {
                    "days": window_days,
                    "recent": all_recent,
                    "prior": all_prior,
                    "delta_meter_sale_price_pct": pct_change(
                        all_recent["median_meter_sale_price"],
                        all_prior["median_meter_sale_price"],
                    ),
                    "delta_actual_worth_pct": pct_change(
                        all_recent["median_actual_worth"],
                        all_prior["median_actual_worth"],
                    ),
                }
            )

            land_windows.append(
                {
                    "days": window_days,
                    "recent": land_recent,
                    "prior": land_prior,
                    "delta_meter_sale_price_pct": pct_change(
                        land_recent["median_meter_sale_price"],
                        land_prior["median_meter_sale_price"],
                    ),
                    "delta_actual_worth_pct": pct_change(
                        land_recent["median_actual_worth"],
                        land_prior["median_actual_worth"],
                    ),
                }
            )

        target_summary[target.name] = {
            "matches_in_lookback": len(target_rows),
            "all_sales": {"windows": all_windows},
            "land_sales": {"windows": land_windows},
        }

    summary = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset_id": args.dataset_id,
        "page_size": args.page_size,
        "pages_scanned": last_page,
        "raw_matches": len(raw_matches),
        "deduped_matches": len(deduped_rows),
        "lookback_days": args.days,
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "cutoff_date": cutoff_date.strftime("%Y-%m-%d"),
        "targets": target_summary,
    }

    report = build_markdown(summary)

    print(report)

    if args.output_json:
        output_json = Path(args.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.output_markdown:
        output_md = Path(args.output_markdown)
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(report + "\n", encoding="utf-8")

    if args.output_matches:
        output_matches = Path(args.output_matches)
        output_matches.parent.mkdir(parents=True, exist_ok=True)
        with output_matches.open("w", encoding="utf-8") as handle:
            for row in lookback_rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
