# dbt project: local_lakehouse

This dbt project builds a small analytics warehouse inside DuckDB.

Key highlights:
- `models/bronze/*` materialize raw Parquet files into DuckDB tables
- `models/staging/*` cleans/types/standardizes
- `models/intermediate/*` does joins and calculations
- `models/marts/*` creates dimensions, facts, and a daily KPI mart
- `snapshots/snap_customers.sql` demonstrates SCD-like tracking (check strategy)
- `tests/` includes an example singular test

Run from repo root:
```bash
python run.py build
```
