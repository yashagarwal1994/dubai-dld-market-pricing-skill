#!/usr/bin/env python3
"""Fetch Dubai DLD transaction data and compute area-level price trend signals.

The script reads the open Data Dubai DLD transactions endpoint and outputs
aggregated trend comparisons (recent window vs prior window) for selected areas.
"""

from __future__ import annotations

import argparse
import heapq
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
    "area_id",
    "trans_group_en",
    "reg_type_en",
    "master_project_en",
    "project_name_en",
    "project_number",
    "building_name_en",
    "area_name_en",
    "property_type_en",
    "property_sub_type_en",
    "rooms_en",
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

SUPPORTED_SOURCE_HOSTS = ("propertyfinder", "bayut")
URL_HINT_QUERY_KEYS = {
    "area",
    "areas",
    "community",
    "location",
    "project",
    "building",
    "tower",
    "keyword",
    "q",
}
URL_TOKEN_STOPWORDS = {
    "a",
    "ae",
    "al",
    "and",
    "apartment",
    "apartments",
    "bayut",
    "bed",
    "beds",
    "bedroom",
    "bedrooms",
    "buy",
    "city",
    "com",
    "development",
    "dubai",
    "finder",
    "for",
    "home",
    "homes",
    "house",
    "houses",
    "in",
    "listing",
    "listings",
    "plan",
    "plp",
    "price",
    "prices",
    "project",
    "properties",
    "property",
    "propertyfinder",
    "ready",
    "rent",
    "sale",
    "studio",
    "the",
    "to",
    "tower",
    "townhouse",
    "townhouses",
    "unit",
    "units",
    "villa",
    "villas",
    "with",
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
        "--mode",
        choices=("trends", "latest", "property"),
        default="trends",
        help=(
            "Operation mode: trends (area window deltas), latest (all-Dubai latest sales), "
            "property (inspect one transaction vs prior comparable sales)."
        ),
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
        "--area",
        action="append",
        default=[],
        help=(
            "Plain-text area/society/building name. "
            "Repeatable and converted to safe match patterns."
        ),
    )
    parser.add_argument(
        "--source-url",
        action="append",
        default=[],
        help=(
            "Property listing URL (Property Finder or Bayut). "
            "The script infers target patterns from URL slugs/query."
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
        "--latest-limit",
        type=int,
        default=25,
        help="Rows to show in --mode latest (default: 25).",
    )
    parser.add_argument(
        "--transaction-id",
        default=None,
        help="Transaction id to inspect in --mode property.",
    )
    parser.add_argument(
        "--property-area-tolerance-pct",
        type=float,
        default=5.0,
        help=(
            "Allowed +/- tolerance for procedure_area matching in --mode property "
            "(default: 5.0)."
        ),
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=20,
        help="Comparable history rows shown in --mode property (default: 20).",
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


def dedupe_strings(values: Iterable[str]) -> List[str]:
    seen = set()
    output: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def normalize_text_phrase(raw: str) -> str:
    value = urllib.parse.unquote_plus(raw or "")
    value = re.sub(r"\.(html?|php)$", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"[_/\-]+", " ", value)
    value = re.sub(r"[^a-zA-Z0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def tokenize_phrase(raw: str) -> List[str]:
    words = re.findall(r"[a-z0-9]+", raw.lower())
    tokens: List[str] = []
    for word in words:
        if word in URL_TOKEN_STOPWORDS:
            continue
        if word.isdigit() and len(word) >= 4:
            continue
        if len(word) == 1 and not word.isdigit():
            continue
        tokens.append(word)
    return tokens


def slugify_name(raw: str, max_words: int = 6) -> str:
    tokens = tokenize_phrase(raw)
    if not tokens:
        tokens = re.findall(r"[a-z0-9]+", raw.lower())
    return "-".join(tokens[:max_words]).strip("-")


def tokens_to_regex(tokens: Sequence[str]) -> Optional[str]:
    if not tokens:
        return None
    if all(token.isdigit() for token in tokens):
        return None

    def token_part(token: str) -> str:
        if token.isdigit():
            return re.escape(token)
        if len(token) <= 3:
            return re.escape(token)
        if token.endswith("s"):
            return re.escape(token[:-1]) + r"s?"
        return re.escape(token) + r"s?"

    return r"\b" + r"[\s\-]*".join(token_part(token) for token in tokens) + r"\b"


def patterns_from_phrase(phrase: str) -> List[str]:
    tokens = tokenize_phrase(phrase)
    if not tokens:
        return []

    patterns: List[str] = []

    full = tokens_to_regex(tokens[:5])
    if full:
        patterns.append(full)

    for size in (2, 3):
        if len(tokens) < size:
            continue
        for index in range(0, len(tokens) - size + 1):
            candidate = tokens_to_regex(tokens[index : index + size])
            if candidate:
                patterns.append(candidate)

    if len(tokens) == 1:
        single = tokens_to_regex(tokens[:1])
        if single:
            patterns.append(single)

    return dedupe_strings(patterns)[:12]


def infer_target_from_source_url(source_url: str, index: int) -> Tuple[str, List[str]]:
    parsed = urllib.parse.urlparse(source_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid --source-url '{source_url}'. Use a full http(s) URL.")

    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if not any(marker in host for marker in SUPPORTED_SOURCE_HOSTS):
        supported = ", ".join(SUPPORTED_SOURCE_HOSTS)
        raise ValueError(
            f"Unsupported --source-url host '{host}'. Supported sources: {supported}."
        )

    source_tag = "propertyfinder" if "propertyfinder" in host else "bayut"

    phrase_candidates: List[str] = []
    for segment in parsed.path.split("/"):
        segment = segment.strip()
        if not segment or re.fullmatch(r"\d+", segment):
            continue
        cleaned = normalize_text_phrase(segment)
        if cleaned:
            phrase_candidates.append(cleaned)

    query_params = urllib.parse.parse_qs(parsed.query, keep_blank_values=False)
    for key, values in query_params.items():
        if key.lower() not in URL_HINT_QUERY_KEYS:
            continue
        for value in values:
            cleaned = normalize_text_phrase(value)
            if cleaned:
                phrase_candidates.append(cleaned)

    phrase_candidates = dedupe_strings(phrase_candidates)

    regex_patterns: List[str] = []
    for phrase in phrase_candidates:
        regex_patterns.extend(patterns_from_phrase(phrase))

    regex_patterns = dedupe_strings(regex_patterns)
    if not regex_patterns:
        raise ValueError(
            f"Could not infer area/building target from URL '{source_url}'. "
            "Use --area or --target as fallback."
        )

    primary_phrase = ""
    primary_score = -1
    for phrase in phrase_candidates:
        score = len(tokenize_phrase(phrase))
        if score > primary_score:
            primary_phrase = phrase
            primary_score = score

    slug = slugify_name(primary_phrase) or f"listing-{index}"
    target_name = f"{source_tag}-{slug}"[:80]
    return target_name.strip("-"), regex_patterns


def parse_targets(
    presets: Sequence[str],
    custom_targets: Sequence[str],
    areas: Sequence[str],
    source_urls: Sequence[str],
) -> List[TargetPattern]:
    target_map: Dict[str, List[str]] = {}

    use_all = not presets and not custom_targets and not areas and not source_urls
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

    for raw_area in areas:
        cleaned_area = normalize_text_phrase(raw_area)
        patterns = patterns_from_phrase(cleaned_area)
        name = slugify_name(cleaned_area)
        if not name or not patterns:
            raise ValueError(
                f"Could not build a valid target from --area '{raw_area}'."
            )
        target_map.setdefault(name, []).extend(patterns)

    for index, source_url in enumerate(source_urls, start=1):
        name, patterns = infer_target_from_source_url(source_url, index)
        target_map.setdefault(name, []).extend(patterns)

    if not target_map:
        raise ValueError("No targets were configured.")

    compiled: List[TargetPattern] = []
    for name, patterns in target_map.items():
        compiled.append(
            TargetPattern(
                name=name,
                regexes=[
                    re.compile(pattern, re.IGNORECASE)
                    for pattern in dedupe_strings(patterns)
                ],
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
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            json.JSONDecodeError,
            ConnectionError,
            OSError,
        ) as exc:
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


def iter_dataset_rows(
    dataset_id: int,
    page_size: int,
    last_page: int,
    workers: int,
    verbose: bool,
) -> Iterable[Tuple[int, List[dict], int]]:
    pages_scanned = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(fetch_page, dataset_id, page, page_size): page
            for page in range(1, last_page + 1)
        }
        for future in as_completed(futures):
            page = futures[future]
            try:
                rows = future.result()
            except Exception as exc:
                rows = []
                if verbose:
                    print(f"[warn] page={page} fetch failed: {exc}")
            pages_scanned += 1
            if verbose and (pages_scanned % 100 == 0 or pages_scanned == last_page):
                print(f"[info] pages={pages_scanned}/{last_page}")
            yield page, rows, pages_scanned


def row_sort_score(row: dict) -> Tuple[int, int]:
    try:
        date_score = to_iso_date(str(row.get("instance_date") or "")).toordinal()
    except ValueError:
        date_score = 0
    load_ts = parse_timestamp(str(row.get("load_timestamp") or "")) if row.get("load_timestamp") else None
    load_score = int(load_ts.timestamp()) if load_ts else 0
    return date_score, load_score


def normalized_key(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def property_fingerprint(row: dict) -> dict:
    area_value = to_optional_float(row.get("procedure_area"))
    return {
        "project_number": normalized_key(row.get("project_number")),
        "building_name_en": normalized_key(row.get("building_name_en")),
        "project_name_en": normalized_key(row.get("project_name_en")),
        "master_project_en": normalized_key(row.get("master_project_en")),
        "area_id": normalized_key(row.get("area_id")),
        "area_name_en": normalized_key(row.get("area_name_en")),
        "property_type_en": normalized_key(row.get("property_type_en")),
        "property_sub_type_en": normalized_key(row.get("property_sub_type_en")),
        "rooms_en": normalized_key(row.get("rooms_en")),
        "procedure_area": area_value,
    }


def row_matches_fingerprint_base(row: dict, fingerprint: dict) -> bool:
    row_project_number = normalized_key(row.get("project_number"))
    row_building_name = normalized_key(row.get("building_name_en"))
    row_project_name = normalized_key(row.get("project_name_en"))
    row_area_id = normalized_key(row.get("area_id"))
    row_area_name = normalized_key(row.get("area_name_en"))

    if fingerprint["project_number"] and row_project_number:
        if row_project_number != fingerprint["project_number"]:
            return False
    else:
        if fingerprint["building_name_en"] and row_building_name:
            if row_building_name != fingerprint["building_name_en"]:
                return False
        elif fingerprint["project_name_en"] and row_project_name:
            if row_project_name != fingerprint["project_name_en"]:
                return False

        if fingerprint["area_id"] and row_area_id:
            if row_area_id != fingerprint["area_id"]:
                return False
        elif fingerprint["area_name_en"] and row_area_name:
            if row_area_name != fingerprint["area_name_en"]:
                return False

    for field in ("property_type_en", "property_sub_type_en"):
        expected = fingerprint[field]
        actual = normalized_key(row.get(field))
        if expected and actual and expected != actual:
            return False

    expected_rooms = fingerprint["rooms_en"]
    actual_rooms = normalized_key(row.get("rooms_en"))
    if expected_rooms and actual_rooms and expected_rooms != actual_rooms:
        return False

    return True


def within_area_tolerance(row: dict, fingerprint: dict, area_tolerance_pct: float) -> bool:
    expected_area = fingerprint["procedure_area"]
    actual_area = to_optional_float(row.get("procedure_area"))
    if expected_area and actual_area:
        pct = abs(actual_area - expected_area) / expected_area * 100.0
        if pct > max(0.0, area_tolerance_pct):
            return False
    return True


def row_matches_fingerprint(row: dict, fingerprint: dict, area_tolerance_pct: float) -> bool:
    if not row_matches_fingerprint_base(row, fingerprint):
        return False
    return within_area_tolerance(row, fingerprint, area_tolerance_pct)


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
    lines.append(
        f"- Targets analyzed: `{', '.join(sorted(summary['targets'].keys()))}`"
    )
    if summary.get("input_areas"):
        lines.append(f"- Area inputs: `{', '.join(summary['input_areas'])}`")
    if summary.get("input_source_urls"):
        lines.append("- Source URLs:")
        for source_url in summary["input_source_urls"]:
            lines.append(f"  - `{source_url}`")
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


def build_latest_markdown(summary: dict) -> str:
    lines: List[str] = []
    lines.append("# Dubai DLD Latest Transactions")
    lines.append("")
    lines.append(f"- Dataset ID: `{summary['dataset_id']}`")
    lines.append(f"- Pages scanned: `{summary['pages_scanned']}`")
    lines.append(f"- Sales rows scanned: `{summary['sales_rows_scanned']}`")
    lines.append(f"- Rows returned: `{len(summary['latest_rows'])}`")
    lines.append("")
    lines.append("| Date | Transaction ID | Area | Project/Building | Type | Rooms | Area (m2) | AED/m2 | Value (AED) |")
    lines.append("|---|---|---|---|---|---|---:|---:|---:|")
    for row in summary["latest_rows"]:
        project_building = (
            str(row.get("building_name_en") or "").strip()
            or str(row.get("project_name_en") or "").strip()
            or str(row.get("master_project_en") or "").strip()
            or "NA"
        )
        lines.append(
            "| "
            f"{row.get('instance_date') or 'NA'} | "
            f"{row.get('transaction_id') or 'NA'} | "
            f"{row.get('area_name_en') or 'NA'} | "
            f"{project_building} | "
            f"{row.get('property_sub_type_en') or row.get('property_type_en') or 'NA'} | "
            f"{row.get('rooms_en') or 'NA'} | "
            f"{fmt_number(row.get('procedure_area'))} | "
            f"{fmt_number(row.get('meter_sale_price'))} | "
            f"{fmt_number(row.get('actual_worth'))} |"
        )
    lines.append("")
    lines.append("## Source")
    lines.append("")
    lines.append("- https://data.dubai/en/web/guest/l/470061")
    lines.append("- https://data.dubai/o/dda/data-services/dataset-metadata")
    lines.append("")
    return "\n".join(lines)


def delta_direction_label(delta: Optional[float]) -> str:
    if delta is None:
        return "NA"
    if delta <= -1.0:
        return "down"
    if delta >= 1.0:
        return "up"
    return "flat"


def build_property_markdown(summary: dict) -> str:
    selected = summary["selected_transaction"]
    previous = summary.get("previous_transaction")
    lines: List[str] = []
    lines.append("# Dubai DLD Property Follow-up")
    lines.append("")
    lines.append(f"- Dataset ID: `{summary['dataset_id']}`")
    lines.append(
        f"- Selected transaction: `{selected.get('transaction_id')}` on `{selected.get('instance_date')}`"
    )
    lines.append(f"- Comparable sales found: `{summary['comparable_count']}`")
    lines.append(
        f"- Comparison area tolerance: `+/- {summary['area_tolerance_pct']:.1f}%`"
    )
    lines.append(f"- Matching scope used: `{summary['comparison_scope']}`")
    lines.append("")

    lines.append("## Verdict")
    lines.append("")
    if previous:
        lines.append(
            f"- Latest comparable before selected: `{previous.get('transaction_id')}` on `{previous.get('instance_date')}`"
        )
        lines.append(
            "- AED/m2 change: "
            f"`{fmt_pct(summary['delta_meter_sale_price_pct'])}` "
            f"({delta_direction_label(summary['delta_meter_sale_price_pct'])})"
        )
        lines.append(
            "- Value change: "
            f"`{fmt_pct(summary['delta_actual_worth_pct'])}` "
            f"({delta_direction_label(summary['delta_actual_worth_pct'])})"
        )
    else:
        lines.append("- Not enough prior comparable sales to determine direction.")
    lines.append("")

    lines.append("## Selected Transaction")
    lines.append("")
    lines.append(
        f"- Area: `{selected.get('area_name_en') or 'NA'}` | "
        f"Building: `{selected.get('building_name_en') or 'NA'}` | "
        f"Project: `{selected.get('project_name_en') or 'NA'}`"
    )
    lines.append(
        f"- Property: `{selected.get('property_sub_type_en') or selected.get('property_type_en') or 'NA'}` | "
        f"Rooms: `{selected.get('rooms_en') or 'NA'}` | "
        f"Area: `{fmt_number(selected.get('procedure_area'))}` m2"
    )
    lines.append(
        f"- Price: `{fmt_number(selected.get('meter_sale_price'))}` AED/m2 | "
        f"Value: `{fmt_number(selected.get('actual_worth'))}` AED"
    )
    lines.append("")

    lines.append("## Comparable History")
    lines.append("")
    lines.append("| Date | Transaction ID | AED/m2 | Value (AED) | Area (m2) |")
    lines.append("|---|---|---:|---:|---:|")
    for row in summary["history_rows"]:
        lines.append(
            "| "
            f"{row.get('instance_date') or 'NA'} | "
            f"{row.get('transaction_id') or 'NA'} | "
            f"{fmt_number(row.get('meter_sale_price'))} | "
            f"{fmt_number(row.get('actual_worth'))} | "
            f"{fmt_number(row.get('procedure_area'))} |"
        )

    lines.append("")
    lines.append("## Source")
    lines.append("")
    lines.append("- https://data.dubai/en/web/guest/l/470061")
    lines.append("- https://data.dubai/o/dda/data-services/dataset-metadata")
    lines.append("")
    return "\n".join(lines)


def run_trends_mode(args: argparse.Namespace, last_page: int) -> int:
    try:
        windows = parse_windows(args.windows)
        targets = parse_targets(args.preset, args.target, args.area, args.source_url)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    if args.verbose:
        print(f"[info] scanning pages 1..{last_page} with {args.workers} workers")

    raw_matches: List[dict] = []
    pages_scanned = 0

    for _page, rows, pages_scanned in iter_dataset_rows(
        args.dataset_id, args.page_size, last_page, args.workers, False
    ):
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
        print(
            "[error] No matching sales transactions found for the selected targets.",
            file=sys.stderr,
        )
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
        "pages_scanned": pages_scanned,
        "raw_matches": len(raw_matches),
        "deduped_matches": len(deduped_rows),
        "lookback_days": args.days,
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "cutoff_date": cutoff_date.strftime("%Y-%m-%d"),
        "input_areas": list(args.area),
        "input_source_urls": list(args.source_url),
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


def run_latest_mode(args: argparse.Namespace, last_page: int) -> int:
    if args.latest_limit <= 0:
        print("[error] --latest-limit must be a positive integer.", file=sys.stderr)
        return 2

    if args.verbose:
        print(f"[info] scanning pages 1..{last_page} with {args.workers} workers")

    buffer_limit = max(args.latest_limit * 20, args.latest_limit)
    heap: List[Tuple[int, int, int, dict]] = []
    sequence = 0
    pages_scanned = 0
    sales_rows_scanned = 0

    for _page, rows, pages_scanned in iter_dataset_rows(
        args.dataset_id, args.page_size, last_page, args.workers, args.verbose
    ):
        for row in rows:
            if str(row.get("trans_group_en") or "").strip().lower() != "sales":
                continue
            sales_rows_scanned += 1
            clean = sanitize_row(row, [])
            sequence += 1
            date_score, load_score = row_sort_score(clean)
            payload = (date_score, load_score, sequence, clean)
            if len(heap) < buffer_limit:
                heapq.heappush(heap, payload)
            else:
                if (date_score, load_score, sequence) > heap[0][:3]:
                    heapq.heappushpop(heap, payload)

    if not heap:
        print("[error] No sales rows were found in the dataset.", file=sys.stderr)
        return 4

    ranked = sorted(heap, key=lambda item: item[:3], reverse=True)
    latest_rows: List[dict] = []
    seen_tx = set()
    for _date, _load, _seq, row in ranked:
        tx_id = str(row.get("transaction_id") or "").strip()
        key = tx_id or f"__row_{_seq}"
        if key in seen_tx:
            continue
        seen_tx.add(key)
        latest_rows.append(row)
        if len(latest_rows) >= args.latest_limit:
            break

    summary = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset_id": args.dataset_id,
        "page_size": args.page_size,
        "pages_scanned": pages_scanned,
        "sales_rows_scanned": sales_rows_scanned,
        "latest_rows": latest_rows,
    }
    report = build_latest_markdown(summary)
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
            for row in latest_rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    return 0


def run_property_mode(args: argparse.Namespace, last_page: int) -> int:
    tx_id = str(args.transaction_id or "").strip()
    if not tx_id:
        print("[error] --transaction-id is required in --mode property.", file=sys.stderr)
        return 2

    if args.verbose:
        print(f"[info] locating transaction {tx_id} across pages 1..{last_page}")

    selected: Optional[dict] = None
    pages_scanned_find = 0
    for _page, rows, pages_scanned_find in iter_dataset_rows(
        args.dataset_id, args.page_size, last_page, args.workers, args.verbose
    ):
        for row in rows:
            if str(row.get("trans_group_en") or "").strip().lower() != "sales":
                continue
            if str(row.get("transaction_id") or "").strip() != tx_id:
                continue
            candidate = sanitize_row(row, [])
            if selected is None or row_sort_score(candidate) > row_sort_score(selected):
                selected = candidate

    if selected is None:
        print(
            f"[error] Transaction id '{tx_id}' was not found in scanned sales rows.",
            file=sys.stderr,
        )
        return 4

    fingerprint = property_fingerprint(selected)
    if args.verbose:
        print("[info] collecting comparable sales for selected transaction")

    comparable_rows_broad: List[dict] = []
    pages_scanned_compare = 0
    for _page, rows, pages_scanned_compare in iter_dataset_rows(
        args.dataset_id, args.page_size, last_page, args.workers, args.verbose
    ):
        for row in rows:
            if str(row.get("trans_group_en") or "").strip().lower() != "sales":
                continue
            if not row_matches_fingerprint_base(row, fingerprint):
                continue
            comparable_rows_broad.append(sanitize_row(row, []))

    deduped_broad = dedupe_transactions(comparable_rows_broad)
    deduped_strict = [
        row
        for row in deduped_broad
        if within_area_tolerance(row, fingerprint, args.property_area_tolerance_pct)
    ]
    if len(deduped_strict) >= 2:
        deduped = deduped_strict
        comparison_scope = "strict (same property profile and similar area)"
    else:
        deduped = deduped_broad
        comparison_scope = "broader (same property profile, area relaxed)"

    deduped.sort(
        key=lambda row: (
            row_sort_score(row)[0],
            row_sort_score(row)[1],
            str(row.get("transaction_id") or ""),
        ),
        reverse=True,
    )

    selected_row = None
    for row in deduped:
        if str(row.get("transaction_id") or "").strip() == tx_id:
            selected_row = row
            break
    if selected_row is None:
        selected_row = selected

    selected_score = row_sort_score(selected_row)
    previous_row = None
    for row in deduped:
        if str(row.get("transaction_id") or "").strip() == tx_id:
            continue
        if row_sort_score(row) < selected_score:
            previous_row = row
            break

    delta_meter = None
    delta_value = None
    if previous_row is not None:
        delta_meter = pct_change(
            selected_row.get("meter_sale_price"), previous_row.get("meter_sale_price")
        )
        delta_value = pct_change(
            selected_row.get("actual_worth"), previous_row.get("actual_worth")
        )

    history_limit = max(1, args.history_limit)
    history_rows = deduped[:history_limit]

    summary = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset_id": args.dataset_id,
        "page_size": args.page_size,
        "pages_scanned_find": pages_scanned_find,
        "pages_scanned_compare": pages_scanned_compare,
        "area_tolerance_pct": args.property_area_tolerance_pct,
        "comparison_scope": comparison_scope,
        "selected_transaction": selected_row,
        "previous_transaction": previous_row,
        "comparable_count": len(deduped),
        "delta_meter_sale_price_pct": delta_meter,
        "delta_actual_worth_pct": delta_value,
        "history_rows": history_rows,
    }

    report = build_property_markdown(summary)
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
            for row in history_rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    return 0


def main() -> int:
    args = parse_args()
    last_page = find_last_page(args.dataset_id, args.page_size, args.max_pages)
    if last_page <= 0:
        print("[error] No pages found in dataset.", file=sys.stderr)
        return 3

    if args.mode == "latest":
        return run_latest_mode(args, last_page)
    if args.mode == "property":
        return run_property_mode(args, last_page)
    return run_trends_mode(args, last_page)


if __name__ == "__main__":
    sys.exit(main())
