CREATE TABLE IF NOT EXISTS sales_clean (
    id BIGSERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_id TEXT NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    revenue NUMERIC(14,2) GENERATED ALWAYS AS (quantity * price) STORED
);

CREATE TABLE IF NOT EXISTS sales_daily_agg (
    sale_date DATE NOT NULL,
    product_id TEXT NOT NULL,
    total_qty BIGINT NOT NULL,
    total_revenue NUMERIC(14,2) NOT NULL,
    PRIMARY KEY (sale_date, product_id)
);
