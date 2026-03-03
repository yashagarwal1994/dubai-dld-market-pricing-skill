# Dubai DLD Market Pricing Skill

A public Agent Skill repository for analyzing Dubai real-estate price direction using the official open DLD transaction feed.

This repo is designed for installation with [`skills.sh`](https://skills.sh/) and follows the Agent Skills format.

## Available Skills

### dubai-dld-market-pricing

Analyze Dubai community-level price movement from DLD transactions and compare recent windows vs prior windows (for example 14d and 30d).

Use when you need to answer questions like:
- Are prices cooling in a specific society/community?
- Is `AED/m2` trending up or down recently?
- What is the transaction-value direction in recent weeks?
- What are transaction-based trend signals for a specific Property Finder/Bayut listing area?
- What are the latest sales transactions across all of Dubai right now?
- For one transaction, is this property trending down or up vs prior comparable sales?

## Installation

```bash
npx skills add yashagarwal1994/dubai-dld-market-pricing-skill
```

List skills before install:

```bash
npx skills add yashagarwal1994/dubai-dld-market-pricing-skill --list
```

## Usage Example

```bash
scripts/dubai_dld_price_trends.py --preset all --days 90 --windows 14,30
```

Fetch latest city-wide sales:

```bash
scripts/dubai_dld_price_trends.py --mode latest --latest-limit 25
```

Inspect one transaction against prior comparable sales:

```bash
scripts/dubai_dld_price_trends.py \
  --mode property \
  --transaction-id "1-102-2026-21388" \
  --history-limit 20
```

Lookup by society/area name:

```bash
scripts/dubai_dld_price_trends.py --area "Jumeirah Lakes Towers" --days 120 --windows 14,30
```

Lookup directly from Property Finder / Bayut URLs:

```bash
scripts/dubai_dld_price_trends.py \
  --source-url "https://www.propertyfinder.ae/en/plp/buy/apartment-for-sale-dubai-jumeirah-lake-towers-jlt-12345678.html" \
  --source-url "https://www.bayut.com/for-sale/apartments/dubai/jumeirah-lake-towers-jlt/" \
  --days 120 --windows 14,30
```

## Data Source

- Dataset page: https://data.dubai/en/web/guest/l/470061
- API endpoint: https://data.dubai/o/dda/data-services/dataset-metadata

## Privacy

This skill is designed for aggregate analysis only. It intentionally focuses on non-personal transaction fields and summarizes results using counts, medians, and percentage deltas.

## License

MIT
