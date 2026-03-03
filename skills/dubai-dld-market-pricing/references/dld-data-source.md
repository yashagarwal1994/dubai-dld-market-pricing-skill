# Dubai DLD Open Dataset Notes

## Primary Dataset
- Portal page: `https://data.dubai/en/web/guest/l/470061`
- API endpoint: `https://data.dubai/o/dda/data-services/dataset-metadata`
- Dataset id: `470061` (Real Estate Transactions)

## API Pattern
Use query parameters:
- `datasetId` (integer)
- `page` (1-based)
- `pageSize` (default practical value: 1000)

Example:

```text
https://data.dubai/o/dda/data-services/dataset-metadata?datasetId=470061&page=1&pageSize=1000
```

## Fields Used By This Skill
- `transaction_id`
- `instance_date`
- `area_id`
- `trans_group_en`
- `reg_type_en`
- `master_project_en`
- `project_name_en`
- `project_number`
- `building_name_en`
- `area_name_en`
- `property_type_en`
- `property_sub_type_en`
- `rooms_en`
- `procedure_name_en`
- `actual_worth`
- `meter_sale_price`
- `procedure_area`
- `load_timestamp`

## Notes
- Focus on `trans_group_en = Sales` for market price direction.
- Use medians over means to reduce outlier distortion.
- De-duplicate rows by `transaction_id` before final aggregation.
- Many comparisons should be interpreted with sample-size context.
