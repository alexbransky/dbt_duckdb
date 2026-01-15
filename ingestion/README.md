# Ingestion

This project uses **Python** to generate deterministic synthetic data into `data/raw/` (your "bronze" layer).
dbt then builds staging/intermediate/marts models inside a local DuckDB database file.

Run:
```bash
python run.py ingest --days 30 --customers 500 --products 100 --orders 3000
```
