PROJECT EXAMPLE : ETL - CSV TO STAGING DATABASE IN POSTGRESQL WITH PYTHON# ETL Project: Automating Data Ingestion from CSV to PostgreSQL Staging Layer

---

## 📌 Project Overview

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline built with Python. The pipeline reads raw customer data from a CSV file, performs data cleaning and transformation, and loads the result into a **PostgreSQL staging database** using an **upsert strategy** to handle duplicate records efficiently.

This project is designed as a foundational example of a real-world data engineering workflow.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core programming language |
| Pandas | Data manipulation and transformation |
| Psycopg2 | PostgreSQL database connector |
| PostgreSQL 15+ | Target staging database |
| Jupyter Notebook | Development environment |

---

## 📂 Project Structure

```
ETL_Python/
├── data/
│   └── dataset_customer_practice.csv   # Raw source data
├── task/
│   └── etl_pipeline.ipynb              # Main ETL notebook
├── README.md
└── requirements.txt
```

---

## 🔄 ETL Flow

### 1. Extract (E)
- Read raw CSV file using `pandas.read_csv()`
- Preview data shape, data types, and sample records
- Identify data quality issues before transformation

### 2. Transform (T)
The transformation step includes several cleaning operations:

    | Step | Description |
    |---|---|
    | T1 - Column Name Cleaning -> Lowercase, replace spaces with underscores, remove special characters |
    | T2 - Data Type Conversion -> Convert `first_day` from string to `datetime` with format `%m/%d/%Y` |
    | T3 - Handle Missing Values -> Drop or fill null values based on column context |
    | T4 - Remove Duplicates -> Drop duplicate rows to ensure data integrity |
    | T5 - Column Rename -> Rename columns to match target database schema |

### 3. Load (L)
- Select only relevant columns for the target table
- Convert DataFrame to list of tuples for `execute_batch()`
- Use **UPSERT** strategy (`INSERT ... ON CONFLICT DO UPDATE`) to avoid duplicate records
- Commit transaction on success, rollback on failure
- Close database connection in `finally` block

---

## 🗄️ Target Table Schema

```sql
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id   SERIAL PRIMARY KEY,
    gender        VARCHAR(255),
    age           INTEGER,
    first_date    DATE,
    annual_income NUMERIC(10, 2),
    spending_score INTEGER
);
```

---

## ⚙️ Configuration

```python
CSV_FILE    = 'data/dataset_customer_practice.csv'
DB_SCHEMA   = 'staging'
DB_NAME     = 'SalesDB'
DB_USER     = 'postgres'
DB_PASSWORD = 'your_password'
DB_HOST     = 'localhost'
DB_PORT     = '5433'
TARGET_TABLE = 'customers'
```

> ⚠️ Do not hardcode credentials in production. Use environment variables or a `.env` file.

---

## 🚀 How to Run

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/etl-csv-postgresql.git
cd etl-csv-postgresql
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Setup PostgreSQL**
- Create database `SalesDB`
- Create schema `staging`
```sql
CREATE DATABASE "SalesDB";
CREATE SCHEMA staging;
```

**4. Run the notebook**
```bash
jupyter notebook task/csv_to_postgresql.ipynb
```

---

## 📦 Requirements

```
pandas>=2.0.0
psycopg2-binary>=2.9.0
jupyter>=1.0.0
```

---

## 📊 Sample Data
200 rows, 6 collumn

| CustomerID | Gender | Age | First Date | Annual Income (k$) | Spending Score (1-100) |
|---|---|---|---|---|---|
| 1 | Male | 19 | 1/1/2023 | 15 | 39 |
| 2 | Female | 21 | 3/15/2023 | 15 | 81 |
| 3 | Female | 20 | 6/22/2023 | 16 | 6 |

---

## 📝 Key Learnings

- Handling special characters in column names during transformation
- Implementing upsert strategy with `psycopg2` and `execute_batch()` for efficient bulk insert
- Setting PostgreSQL schema via `SET search_path` after connection
- Proper error handling with try/except/finally in database operations

---

## 👤 Author

**Rian Prasetiyo**  
📧 rianprasetiyo2@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/yourprofile) 
