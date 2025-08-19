import os
import pandas as pd
from pathlib import Path

# Where processed outputs will be stored
BASE_DIR = Path(__file__).resolve().parents[1]
processed_dir = BASE_DIR / "data" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

# Define expected schema
CANONICAL_COLS = {
    "date": "datetime64[ns]",
    "product_id": "string",
    "quantity": "Int64",
    "price": "float64",
}

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce schema, handle missing cols, drop unexpected, fix types."""
    # Drop unexpected cols
    for col in list(df.columns):
        if col not in CANONICAL_COLS:
            print(f"[Schema Drift] Dropping unexpected column: {col}")
            df.drop(columns=[col], inplace=True)

    # Add missing cols with defaults
    for col, dtype in CANONICAL_COLS.items():
        if col not in df.columns:
            print(f"[Schema Drift] Adding missing column: {col}")
            if col == "date":
                df[col] = pd.NaT
            elif col == "product_id":
                df[col] = pd.NA
            elif col == "quantity":
                df[col] = 0
            elif col == "price":
                df[col] = 0.0

    # Type conversions
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["product_id"] = df["product_id"].astype("string")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("float64")

    # Drop rows with invalid keys
    before = len(df)
    df = df.dropna(subset=["date", "product_id"])
    print(f"[Cleaning] Dropped {before - len(df)} bad rows")

    return df[list(CANONICAL_COLS.keys())]

def transform_csv(input_path: str):
    """Main transformation: clean raw CSV -> parquet outputs."""
    print(f"[Transform] Reading {input_path}")
    df = pd.read_csv(input_path)

    df = _clean_dataframe(df)

    # Add revenue column
    df["revenue"] = df["quantity"] * df["price"]

    # Save cleaned rows
    cleaned_path = processed_dir / "cleaned.parquet"
    df.to_parquet(cleaned_path, index=False)
    print(f"[Transform] Saved cleaned → {cleaned_path}")

    # Daily aggregate
    agg = (
    df.assign(sale_date=df["date"].dt.date)
      .groupby(["sale_date", "product_id"], as_index=False)
      .agg(total_qty=("quantity", "sum"), total_revenue=("revenue", "sum"))
    )


    agg_path = processed_dir / "agg.parquet"
    agg.to_parquet(agg_path, index=False)
    print(f"[Transform] Saved agg → {agg_path}")

    print(f"[Summary] Rows in: {len(df)}, Groups: {len(agg)}")

    return str(cleaned_path), str(agg_path)

if __name__ == "__main__":
    # Quick manual run on latest file in data/incoming/
    incoming = BASE_DIR / "data" / "incoming"
    latest_csv = max(incoming.glob("*.csv"), key=os.path.getmtime)
    transform_csv(str(latest_csv))
