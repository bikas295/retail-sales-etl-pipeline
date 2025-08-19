import os
import random
import pandas as pd
from datetime import date
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
incoming = BASE_DIR / "data" / "incoming"
incoming.mkdir(parents=True, exist_ok=True)

# Today's date for filename
today = date.today().isoformat()

# Generate random sales rows
rows = []
for _ in range(500):  # pretend it's a “large” file
    pid = f"P{random.randint(100, 199)}"
    qty = max(0, int(random.gauss(3, 2)))  # some skewed distribution
    price = round(random.uniform(5, 200), 2)
    rows.append({"date": today, "product_id": pid, "quantity": qty, "price": price})

df = pd.DataFrame(rows)

# 🔹 Schema drift simulation
# Sometimes add an unexpected column
if random.random() < 0.5:
    df["discount"] = [random.choice([0, 0, 5, 10]) for _ in range(len(df))]

# Sometimes drop a column (rare)
if random.random() < 0.2:
    df.drop(columns=["price"], inplace=True)

# Save to CSV
path = incoming / f"sales_{today}.csv"
df.to_csv(path, index=False)
print(f"✅ Wrote {len(df)} rows to {path}")
