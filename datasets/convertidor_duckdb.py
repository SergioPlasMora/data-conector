import duckdb

parquet_files = [
    "dataset_100mb.parquet",
    "dataset_100mb.parquet",
    "dataset_100mb.parquet",
    "dataset_100mb.parquet",
    "dataset_100mb.parquet",
]

duckdb_file = "dataset_500mb.duckdb"

con = duckdb.connect(duckdb_file)

con.execute("""
    CREATE TABLE data AS
    SELECT * FROM read_parquet(?)
""", [parquet_files])

con.close()

print("✅ 5 Parquet importados en un solo DuckDB")
