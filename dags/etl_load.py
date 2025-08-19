import os
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    user = os.getenv("POSTGRES_USER", os.getenv("USER"))
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retaildb")

    return create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}")

def load_to_postgres(cleaned_path: str, agg_path: str):
    engine = get_engine()

    # -------------------
    # Load cleaned rows
    # -------------------
    print(f"[Load] Loading cleaned data from {cleaned_path}")
    df = pd.read_parquet(cleaned_path)

    # Rename 'date' -> 'sale_date' to match DB schema
    if "date" in df.columns:
        df.rename(columns={"date": "sale_date"}, inplace=True)

    # Drop 'revenue' because it's a generated column in DB
    if "revenue" in df.columns:
        df = df.drop(columns=["revenue"])

    # Ensure column order matches DB
    df = df[["sale_date", "product_id", "quantity", "price"]]

    df.to_sql("sales_clean", con=engine, if_exists="append", index=False, method="multi")
    print(f"[Load] Inserted {len(df)} rows into sales_clean")

    # -------------------
    # Load aggregated data (upsert)
    # -------------------
    print(f"[Load] Loading agg data from {agg_path}")
    agg = pd.read_parquet(agg_path)

    # Ensure expected column order
    agg = agg[["sale_date", "product_id", "total_qty", "total_revenue"]]

    # Write to temporary table
    agg.to_sql("_tmp_sales_daily_agg", con=engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sales_daily_agg (sale_date, product_id, total_qty, total_revenue)
            SELECT sale_date, product_id, total_qty, total_revenue
            FROM _tmp_sales_daily_agg
            ON CONFLICT (sale_date, product_id)
            DO UPDATE SET
                total_qty = EXCLUDED.total_qty,
                total_revenue = EXCLUDED.total_revenue;
        """))
        conn.execute(text("DROP TABLE IF EXISTS _tmp_sales_daily_agg"))

    print(f"[Load] Upserted {len(agg)} rows into sales_daily_agg")

if __name__ == "__main__":
    cleaned = "data/processed/cleaned.parquet"
    agg = "data/processed/agg.parquet"
    load_to_postgres(cleaned, agg)
