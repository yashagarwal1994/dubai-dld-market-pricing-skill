---
name: dubai-dld-market-pricing
description: Analyze Dubai real-estate price movement using the official Dubai DLD open transactions feed on data.dubai. Use when asked whether prices are cooling or rising in specific communities, to compare recent windows versus prior windows (for example 14d or 30d), or to build area-level transaction summaries from DLD data without exposing personal information.
---

# Dubai DLD Market Pricing

## Overview
Use this skill to run repeatable, evidence-based pricing checks for Dubai communities using the public DLD transaction dataset. It is built for quick "is it cooling?" or "is it going up?" decisions based on recent closed transactions.

## Workflow
1. Define target communities before fetching data.
2. Run the analysis script against the official DLD endpoint.
3. Compare recent windows against prior windows using medians.
4. Report only aggregate metrics with sample sizes and dates.
5. Add caveats when sample size is small.

## Run Analysis
Use preset targets (Damac Hills 2, Emaar South, JLT, Reportage, Gemz Danube):

```bash
scripts/dubai_dld_price_trends.py --preset all --days 90 --windows 14,30
```

Use specific presets:

```bash
scripts/dubai_dld_price_trends.py --preset damac-hills-2 --preset jlt --days 120
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

## Privacy Rules
- Do not include private user data in prompts, outputs, or files.
- Keep outputs aggregate-only (counts, medians, deltas, windows).
- Avoid sharing raw personal records or non-essential row fields.
- Keep references to source endpoints and analysis dates for auditability.

## Data Source
Read endpoint and field notes in [references/dld-data-source.md](references/dld-data-source.md).
