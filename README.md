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

```
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
```

---

## ⚙️ Setup Instructions

### 🐧 macOS (Local PostgreSQL)

#### 1. Python & DB setup

```
brew install pyenv postgresql@16
pyenv install 3.12.6
pyenv global 3.12.6
python -m venv venv
source venv/bin/activate
```

#### 2. Install Python dependencies

```
pip install "psycopg[binary]" "SQLAlchemy>=2.0,<3" "pandas>=2.3,<2.4" "pyarrow"
```

#### 3. Initialize PostgreSQL

```
brew services start postgresql@16
createdb retaildb
psql retaildb -f db/init.sql
```

---

### 🪟 Windows (WSL recommended or native PowerShell)

✅ Recommended: Use WSL (Windows Subsystem for Linux) + Ubuntu  
❗ Not recommended: Docker-based Postgres for this project

#### Option 1: WSL + Ubuntu

```
sudo apt update && sudo apt install -y python3.12 python3.12-venv postgresql libpq-dev
python3.12 -m venv venv
source venv/bin/activate

pip install "psycopg[binary]" "SQLAlchemy>=2.0,<3" "pandas>=2.3,<2.4" "pyarrow"

sudo service postgresql start
createdb retaildb
psql retaildb -f db/init.sql
```

#### Option 2: Native Windows (PowerShell)

1. Install PostgreSQL 16 for Windows  
2. Install Python 3.12 from [https://www.python.org](https://www.python.org)  
3. Then run:

```
python -m venv venv
.\venv\Scripts\activate
pip install "psycopg[binary]" "SQLAlchemy>=2.0,<3" "pandas>=2.3,<2.4" "pyarrow"
```

4. Use pgAdmin or CLI to create a new DB called `retaildb`

```
psql -U postgres -d retaildb -f db/init.sql
```

> Replace `postgres` with your DB username if different.

---

## 🧪 Manual Pipeline Run (for testing)

```
# Generate sample CSV with schema drift
python scripts/generate_fake_csv.py

# Clean & aggregate to parquet
python dags/etl_transform.py

# Load to Postgres
python dags/etl_load.py
```

---

## ⏱️ Airflow Integration

### Install Airflow 3.0.4 (with constraints)

```
export AIRFLOW_VERSION=3.0.4
PYVER=$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYVER}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINT_URL"
```

### Point Airflow to the right DAG folder

```
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
```

### First time Airflow setup

```
airflow standalone
```

> Save the generated admin password or create your own:

```
airflow users create -u admin -p admin -r Admin -f Admin -l User -e admin@example.com
```

---

## 🧠 How It Works

### 🔹 Extract
- Picks the latest CSV from `data/incoming/`  
- Archives a copy to `data/archive/`

### 🔹 Transform
- Handles schema drift (extra/missing cols)
- Converts to typed DataFrame
- Outputs:
  - `cleaned.parquet` (row-level)
  - `agg.parquet` (aggregated)

### 🔹 Load
- Inserts rows into `sales_clean`
- Upserts aggregates into `sales_daily_agg`

---

## 🧾 Data Flow Diagram

```
┌────────────┐      ┌────────────┐      ┌────────────┐
│  incoming/ │ ───▶ │ transform  │ ───▶ │ Postgres DB│
│ CSV (raw)  │      │ .parquet   │      │ clean + agg│
└────────────┘      └────────────┘      └────────────┘
       │
       └──────▶ archive/
```

---

## 🐘 PostgreSQL Tables

### `sales_clean`

| Column     | Type      | Notes                               |
|------------|-----------|-------------------------------------|
| id         | BIGSERIAL | Auto-increment primary key          |
| sale_date  | DATE      | Enforced schema column              |
| product_id | TEXT      |                                     |
| quantity   | INT       |                                     |
| price      | NUMERIC   |                                     |
| revenue    | NUMERIC   | `GENERATED ALWAYS AS (quantity*price)` |

---

### `sales_daily_agg`

| Column        | Type    |
|---------------|---------|
| sale_date     | DATE    |
| product_id    | TEXT    |
| total_qty     | BIGINT  |
| total_revenue | NUMERIC |

---

## 💬 Interview Highlights

- Built for **resilience**: handles schema drift, missing columns, type mismatches
- Modular: clean **Extract → Transform → Load**
- Upserts for aggregates using Postgres conflict handling
- Logs schema drift + row drop stats
- Easily expandable for S3, Spark, or validation tools like Great Expectations

---

## 📌 Improvements & Ideas

- Add Great Expectations for data validation
- Replace CSV with S3/Cloud Storage
- Use Spark/Dask for large-scale data
- Slack/Email alerts on drift/failure
- Push logs to Prometheus/Grafana

---

## 🧾 License

MIT License

---

