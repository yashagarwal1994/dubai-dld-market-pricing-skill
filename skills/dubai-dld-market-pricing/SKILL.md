---
name: dubai-dld-market-pricing
description: Analyze Dubai real-estate prices using the official Dubai DLD open transactions feed on data.dubai. Use when asked whether prices are cooling or rising in specific communities, to compare recent windows versus prior windows (for example 14d or 30d), to list the latest city-wide transactions, to inspect one transaction and check if it is down/up versus prior comparable sales, to look up prices for a society/area, or when given a Property Finder/Bayut listing URL to infer the target area from the link without exposing personal information.
---

# Dubai DLD Market Pricing

## Overview
Use this skill to run repeatable, evidence-based pricing checks for Dubai communities using the public DLD transaction dataset. It is built for quick "is it cooling?" or "is it going up?" decisions based on recent closed transactions.

## Workflow
1. Pick the mode: `trends`, `latest`, or `property`.
2. Run the analysis script against the official DLD endpoint.
3. For `trends`, compare recent windows against prior windows using medians.
4. For `latest`, return newest sales across all of Dubai.
5. For `property`, compare selected transaction versus prior comparable sales.
6. Report aggregate metrics with sample sizes, dates, and caveats when sparse.

## Run Analysis
Use preset targets (Damac Hills 2, Emaar South, JLT, Reportage, Gemz Danube):

```bash
scripts/dubai_dld_price_trends.py --preset all --days 90 --windows 14,30
```

Use specific presets:

```bash
scripts/dubai_dld_price_trends.py --preset damac-hills-2 --preset jlt --days 120
```

Fetch latest city-wide sales:

```bash
scripts/dubai_dld_price_trends.py --mode latest --latest-limit 25
```

Inspect one transaction and check if price moved down/up:

```bash
scripts/dubai_dld_price_trends.py \
  --mode property \
  --transaction-id "1-102-2026-21388" \
  --property-area-tolerance-pct 5 \
  --history-limit 20
```

Use plain text area/society lookups:

```bash
scripts/dubai_dld_price_trends.py --area "Jumeirah Village Circle" --area "Emaar South" --days 120
```

Use listing URLs from Property Finder or Bayut:

```bash
scripts/dubai_dld_price_trends.py \
  --source-url "https://www.propertyfinder.ae/en/plp/buy/apartment-for-sale-dubai-jumeirah-lake-towers-jlt-12345678.html" \
  --source-url "https://www.bayut.com/for-sale/apartments/dubai/jumeirah-lake-towers-jlt/" \
  --days 120 --windows 14,30
```

Use custom community matchers:

```bash
scripts/dubai_dld_price_trends.py \
  --target "business-bay=\\bbusiness\\s*bay\\b" \
  --target "downtown=\\bdowntown\\s*dubai\\b" \
  --days 90 --windows 14,30
```

Persist outputs:

```bash
scripts/dubai_dld_price_trends.py \
  --preset all \
  --output-json output/summary.json \
  --output-markdown output/report.md \
  --output-matches output/matches.jsonl
```

## Interpret Results
- Prefer `meter_sale_price` median as primary price signal.
- Use `actual_worth` median as secondary confirmation.
- Treat outcomes with low counts (`n < 10`) as directional only.
- If 14-day and 30-day deltas disagree, classify as mixed/noisy.
- URL-derived targets rely on listing slug/query text; if matching is weak, add `--area` or explicit `--target`.
- In `property` mode, if strict area matching is sparse, fallback uses broader same-profile comparables.

## Privacy Rules
- Do not include private user data in prompts, outputs, or files.
- Keep outputs aggregate-only (counts, medians, deltas, windows).
- Avoid sharing raw personal records or non-essential row fields.
- Keep references to source endpoints and analysis dates for auditability.

## Data Source
Read endpoint and field notes in [references/dld-data-source.md](references/dld-data-source.md).
