# 🛍️ Retail Sales ETL Pipeline

A robust, daily ETL pipeline that simulates ingestion of retail sales data, performs cleaning, transformation, and stores the results into a PostgreSQL database. The entire process is orchestrated using Apache Airflow 3.x.

---

## 📦 Stack

- **Python 3.12**
- **Pandas 2.3**
- **PostgreSQL 16**
- **Apache Airflow 3.0.4**
- **SQLAlchemy 2.x**
- **psycopg 3**
- **Parquet (PyArrow)**

---

## 📁 Project Structure

```bash
data_engg_proj/
├── dags/
│   ├── etl_extract.py           # Picks latest CSV & archives it
│   ├── etl_transform.py         # Cleans & aggregates data
│   ├── etl_load.py              # Loads data to Postgres
│   └── retail_sales_etl_dag.py  # Airflow DAG definition
├── data/
│   ├── incoming/                # Simulated raw CSVs
│   ├── processed/               # Cleaned & aggregated parquet
│   └── archive/                 # Archived raw files post-extract
├── db/
│   └── init.sql                 # SQL schema for Postgres
├── scripts/
│   └── generate_fake_csv.py     # Simulates schema-drifting CSVs
├── .env                         # PostgreSQL connection vars
├── requirements.txt
└── README.md

