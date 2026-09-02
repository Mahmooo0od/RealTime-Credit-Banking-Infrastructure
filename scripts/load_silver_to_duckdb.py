import duckdb
import os

DB_PATH = "data/warehouse/banking.duckdb"
SILVER_PATH = "data/silver/enriched_applications/*.parquet"

os.makedirs("data/warehouse", exist_ok=True)

con = duckdb.connect(DB_PATH)

con.execute(f"""
    CREATE OR REPLACE TABLE raw_enriched_applications AS
    SELECT * FROM read_parquet('{SILVER_PATH}')
""")

count = con.execute("SELECT COUNT(*) FROM raw_enriched_applications").fetchone()[0]
print(f"Loaded {count} rows into DuckDB raw_enriched_applications table")

con.close()
