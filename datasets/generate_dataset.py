import csv
import random
import sys
from datetime import datetime, timedelta

TARGET_MB = int(sys.argv[1]) if len(sys.argv) > 1 else 10
TARGET_BYTES = TARGET_MB * 1024 * 1024

filename = f"dataset_{TARGET_MB}mb.csv"

header = ["id", "name", "value", "category", "timestamp"]
categories = [f"C{n}" for n in range(500)]

def random_timestamp():
    start = datetime(2000, 1, 1)
    delta = timedelta(seconds=random.randint(0, 20 * 365 * 24 * 3600))
    return (start + delta).isoformat() + "Z"

row_id = 1
current_size = 0

with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)

with open(filename, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    while current_size < TARGET_BYTES:
        writer.writerow([
            row_id,
            f"Item-{row_id}-{random.randint(1, 1_000_000)}",
            random.random() * random.randint(1, 10_000_000),
            random.choice(categories),
            random_timestamp()
        ])
        row_id += 1

        if row_id % 1000 == 0:
            current_size = f.tell()

print(f"✅ Archivo creado: {filename}")
print(f"📦 Tamaño aproximado: {current_size / (1024 * 1024):.2f} MB")
