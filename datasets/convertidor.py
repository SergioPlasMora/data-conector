import pandas as pd
import sys
from pathlib import Path

csv_file = sys.argv[1]
parquet_file = Path(csv_file).with_suffix(".parquet")

df = pd.read_csv(csv_file)

df.to_parquet(
    parquet_file,
    engine="pyarrow",
    compression="snappy"  # rápido y estándar
)

print(f"✅ Convertido: {csv_file} → {parquet_file}")
