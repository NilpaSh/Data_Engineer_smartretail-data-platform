# DataPulse — End-to-End Retail Analytics Platform

> **A beginner-friendly, production-grade data engineering & analytics portfolio project.**
> Covers the full data lifecycle: raw data → cleaning → transformation → scheduling → interactive dashboards.

---

## 📖 Table of Contents

1. [What Is This Project? (Plain English)](#1-what-is-this-project-plain-english)
2. [Who Is This For?](#2-who-is-this-for)
3. [Technologies Used — Explained Simply](#3-technologies-used--explained-simply)
   - [Python](#31-python)
   - [PostgreSQL](#32-postgresql)
   - [dbt (data build tool)](#33-dbt-data-build-tool)
   - [Apache Airflow](#34-apache-airflow)
   - [Streamlit & Plotly](#35-streamlit--plotly)
   - [Docker & Docker Compose](#36-docker--docker-compose)
   - [GitHub Actions (CI/CD)](#37-github-actions-cicd)
4. [Core Data Concepts Explained](#4-core-data-concepts-explained)
   - [What is a Data Warehouse?](#41-what-is-a-data-warehouse)
   - [Medallion Architecture (Bronze / Silver / Gold)](#42-medallion-architecture-bronze--silver--gold)
   - [Star Schema (Fact & Dimension Tables)](#43-star-schema-fact--dimension-tables)
   - [ETL vs ELT](#44-etl-vs-elt)
   - [RFM Analysis](#45-rfm-analysis)
   - [Cohort Analysis](#46-cohort-analysis)
5. [Project Architecture (How Everything Connects)](#5-project-architecture-how-everything-connects)
6. [Project Folder Structure](#6-project-folder-structure)
7. [Quick Start — Step by Step](#7-quick-start--step-by-step)
8. [Running Each Layer Individually](#8-running-each-layer-individually)
9. [The Dashboard — What You'll See](#9-the-dashboard--what-youll-see)
10. [Data Model (Star Schema Diagram)](#10-data-model-star-schema-diagram)
11. [dbt Models — Layer by Layer](#11-dbt-models--layer-by-layer)
12. [Airflow DAG — How the Pipeline is Scheduled](#12-airflow-dag--how-the-pipeline-is-scheduled)
13. [Running Tests](#13-running-tests)
14. [Learning Path — Where to Go Next](#14-learning-path--where-to-go-next)
15. [Common Errors & How to Fix Them](#15-common-errors--how-to-fix-them)
16. [Glossary](#16-glossary)

---

## 1. What Is This Project? (Plain English)

Imagine you run an **online retail store** (like Amazon, but smaller). Every day:
- Customers sign up and place orders
- Products are bought, sometimes returned
- You want to answer questions like:
  - "Which products make the most money?"
  - "Which customers are about to stop buying from us?"
  - "Are sales higher on weekends or weekdays?"

**DataPulse** is a system that:
1. **Generates** fake but realistic retail data (1,000 customers, 300 products, 8,000+ orders)
2. **Loads** that raw data into a database
3. **Cleans and transforms** it into well-structured tables
4. **Schedules** the whole process to run automatically every day
5. **Visualizes** insights in an interactive web dashboard

This is exactly what a **Data Engineer** + **Data Analyst** builds at companies like Target, Walmart, Uber, Spotify.

---

## 2. Who Is This For?

| Background | Value You'll Get |
|-----------|-----------------|
| **Aspiring Data Analyst** | Learn SQL analytics, dashboards, RFM, cohort analysis |
| **Aspiring Data Engineer** | Learn ETL pipelines, dbt, Airflow, data modeling |
| **CS Student** | See how Python, SQL, Docker work together in a real project |
| **Career Switcher** | Portfolio project that impresses interviewers at FAANG/top companies |

**Prerequisites (you only need the basics):**
- Basic Python (variables, functions, loops)
- Basic SQL (SELECT, WHERE, GROUP BY)
- No prior knowledge of dbt, Airflow, Docker needed — this guide explains everything

---

## 3. Technologies Used — Explained Simply

### 3.1 Python

**What is it?**
Python is a programming language. It's the most popular language for data work.

**How it's used in this project:**
- `data_generation/generate_ecommerce_data.py` — creates fake customers, products, orders
- `ingestion/bronze_loader.py` — reads CSV files and loads them into the database
- `transformation/silver_transformer.py` — cleans and fixes the data

**Key Python libraries used:**

| Library | What it does | Example use in this project |
|---------|-------------|---------------------------|
| `pandas` | Works with tables of data (like Excel in code) | Reading CSVs, cleaning columns |
| `sqlalchemy` | Connects Python to databases | Inserting data into PostgreSQL |
| `faker` | Generates fake realistic data | Creating fake names, emails, addresses |
| `numpy` | Math and statistics | Creating realistic seasonal sales patterns |
| `python-dotenv` | Reads secret settings from a `.env` file | Database password, host |

**Learn Python:**
- [Python.org beginner guide](https://docs.python.org/3/tutorial/)
- [pandas tutorial](https://pandas.pydata.org/docs/getting_started/intro_tutorials/)

---

### 3.2 PostgreSQL

**What is it?**
PostgreSQL (often called "Postgres") is a **relational database** — it stores data in tables with rows and columns, like a very powerful Excel spreadsheet. You query it using SQL.

**Why not just use Excel or CSV files?**
- Can handle millions of rows
- Multiple people/applications can read/write at the same time
- Relationships between tables (orders link to customers)
- Much faster for complex queries

**How it's used in this project:**
PostgreSQL acts as our **Data Warehouse** — the central place where all data lives.

It has 3 schemas (like folders):
```
retail_dw/
├── bronze/    ← raw data loaded directly from CSV
├── silver/    ← cleaned and typed data
└── gold/      ← analytics-ready tables built by dbt
```

**Key SQL concepts used:**
```sql
-- Joins: combining two tables
SELECT o.order_id, c.full_name
FROM silver.orders o
JOIN silver.customers c ON o.customer_id = c.customer_id;

-- Window functions: running totals, rankings
SELECT order_date,
       SUM(revenue) OVER (PARTITION BY year ORDER BY order_date) AS ytd_revenue
FROM gold.daily_sales;

-- CTEs: breaking complex queries into readable steps
WITH customer_stats AS (
    SELECT customer_id, COUNT(*) AS total_orders
    FROM silver.orders GROUP BY 1
)
SELECT * FROM customer_stats WHERE total_orders > 5;
```

**Learn PostgreSQL:**
- [PostgreSQL Tutorial](https://www.postgresqltutorial.com/)
- [Mode SQL Tutorial](https://mode.com/sql-tutorial/)

---

### 3.3 dbt (data build tool)

**What is it?**
dbt is a tool that lets you write **SQL SELECT statements** and it handles building the tables for you. Think of it like "SQL with superpowers."

**The problem it solves:**
Before dbt, data teams would write messy SQL scripts scattered across files with no organization or testing. dbt brings software engineering best practices (version control, testing, documentation) to SQL.

**How it works (simplified):**
1. You write a `.sql` file with a `SELECT` statement
2. dbt runs it and creates a table/view in the database
3. You can reference other dbt models using `{{ ref('model_name') }}`
4. dbt figures out the correct order to build everything

**Example — without dbt (old way):**
```sql
-- You manually run these scripts in order:
-- Step 1: run_01_create_staging.sql
-- Step 2: run_02_create_customers.sql  (depends on step 1)
-- Step 3: run_03_create_fact_table.sql (depends on steps 1 & 2)
-- Easy to mess up the order, no testing, no documentation
```

**Example — with dbt (new way):**
```sql
-- dbt_project/models/marts/core/dim_customers.sql
-- dbt automatically knows this depends on stg_customers

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}  -- dbt resolves this reference
)
SELECT customer_id, full_name, email FROM customers
```

**dbt model layers in this project:**
```
staging/        → Light renaming of Silver tables (views)
intermediate/   → Business logic calculations (views)
marts/core/     → Final dimension & fact tables (tables)
marts/marketing/→ Analytics-specific tables (tables)
```

**dbt testing:**
```yaml
# schema.yml — tests run automatically with `dbt test`
- name: customer_id
  tests:
    - unique          # No duplicate customer IDs
    - not_null        # Every row must have a customer_id
```

**Learn dbt:**
- [dbt Learn (free)](https://learn.getdbt.com/)
- [dbt Documentation](https://docs.getdbt.com/)

---

### 3.4 Apache Airflow

**What is it?**
Airflow is a **workflow scheduler** — it runs your pipelines automatically on a schedule (daily, hourly, etc.) and shows you a visual web interface to monitor them.

**The problem it solves:**
Imagine you need to run this every day:
1. Load new CSV data → 2. Clean data → 3. Run dbt → 4. Check data quality

You could set a cron job, but: what if step 2 fails? How do you know? How do you re-run just step 2? Airflow handles all of this.

**Key concepts:**

| Term | Meaning |
|------|---------|
| **DAG** | Directed Acyclic Graph — a pipeline definition (a set of tasks in order) |
| **Task** | A single step in the pipeline (e.g., "run bronze loader") |
| **Operator** | The type of task (`PythonOperator`, `BashOperator`, etc.) |
| **Schedule** | When the DAG runs (`"0 6 * * *"` = every day at 6 AM) |
| **TaskGroup** | A way to visually group related tasks |

**Our DAG visualization:**
```
pipeline_start
     │
     ▼
check_source_files ──────────────────┐
     │                               │
     ▼ (files exist)      (no files) ▼
bronze_load          generate_data → bronze_load
     │
     ▼
silver_transform
     │
     ▼
data_quality_checks
     │
     ▼
dbt_deps → dbt_run → dbt_test → dbt_docs
     │
     ▼
pipeline_end
```

**Learn Airflow:**
- [Airflow Official Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)
- [Astronomer Learn](https://www.astronomer.io/learn/)

---

### 3.5 Streamlit & Plotly

**What is Streamlit?**
Streamlit turns Python scripts into **web applications** with no HTML/CSS/JavaScript needed. You write Python, get a beautiful dashboard.

```python
import streamlit as st
import pandas as pd

st.title("My Dashboard")                    # Shows a heading
df = pd.read_csv("data.csv")
st.dataframe(df)                            # Shows an interactive table
st.line_chart(df["revenue"])                # Shows a line chart
```

**What is Plotly?**
Plotly creates **interactive charts** — hover over a bar chart to see exact values, zoom into a chart, click legend items to hide/show series.

**Charts used in this project:**
| Chart Type | Where | What it shows |
|-----------|-------|--------------|
| Area chart | Sales Overview | Revenue over time |
| Pie/Donut | Executive Dashboard | Revenue by category |
| Choropleth Map | Executive Dashboard | Sales by US state |
| Heatmap | Customer Analytics | Cohort retention rates |
| Treemap | Customer Analytics | RFM segment sizes |
| Sunburst | Product Performance | Category → Subcategory drill-down |
| Scatter bubble | Product Performance | Price vs margin analysis |

**Learn Streamlit:**
- [Streamlit Docs](https://docs.streamlit.io/)
- [30 Days of Streamlit](https://30days.streamlit.app/)

---

### 3.6 Docker & Docker Compose

**What is Docker?**
Docker packages your application and **all its dependencies** into a container — like a lightweight virtual machine. The container runs the same way on your laptop, a colleague's machine, or a cloud server.

**The problem it solves:**
> "It works on my machine!" — every developer ever

With Docker, everyone runs the exact same environment.

**What is Docker Compose?**
Docker Compose lets you define and run **multiple containers together** with one command. Our project uses:

```yaml
# docker-compose.yml (simplified)
services:
  postgres:     # The database container
    image: postgres:15
    
  dashboard:    # The Streamlit app container
    build: .
    depends_on: [postgres]
    
  airflow:      # The scheduler container (optional)
    image: apache/airflow:2.7.3
```

**Key Docker commands:**
```bash
docker-compose up -d      # Start all containers in background
docker-compose down       # Stop all containers
docker-compose logs -f    # View live logs
docker ps                 # List running containers
```

**Learn Docker:**
- [Docker Getting Started](https://docs.docker.com/get-started/)
- [Play with Docker (free browser)](https://labs.play-with-docker.com/)

---

### 3.7 GitHub Actions (CI/CD)

**What is CI/CD?**
- **CI (Continuous Integration):** Every time you push code, automated tests run to catch bugs
- **CD (Continuous Deployment):** Automatically deploy code when tests pass

**What is GitHub Actions?**
It's GitHub's built-in automation system. You define "workflows" in YAML files and GitHub runs them automatically when you push code.

**Our CI pipeline does this on every push:**
```
Push code to GitHub
       │
       ▼
1. Lint check (is the code formatted correctly?)
       │
       ▼
2. Unit tests (pytest — do the functions work correctly?)
       │
       ▼
3. dbt validation (do all SQL models compile without errors?)
       │
       ▼
4. Security scan (bandit — any obvious security vulnerabilities?)
```

**Learn GitHub Actions:**
- [GitHub Actions Quickstart](https://docs.github.com/en/actions/quickstart)

---

## 4. Core Data Concepts Explained

### 4.1 What is a Data Warehouse?

A **Data Warehouse** (DW) is a database specifically designed for **analytics and reporting**, not for day-to-day operations.

| Regular Database (OLTP) | Data Warehouse (OLAP) |
|------------------------|----------------------|
| Used by your app (insert/update/delete) | Used by analysts (read-heavy queries) |
| Many small, fast transactions | Few complex, slow queries |
| Normalized (many tables, few columns) | Denormalized (fewer tables, many columns) |
| Example: your store's live database | Example: analytics database |

In this project, **PostgreSQL** serves as our data warehouse.

---

### 4.2 Medallion Architecture (Bronze / Silver / Gold)

This is the most important architectural pattern in modern data engineering. Think of it like **refining raw ore into gold**:

```
RAW CSV FILES
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (raw, as-is)                              │
│                                                           │
│  Table: bronze.raw_customers                             │
│  customer_id | first_name | email     | registration_date│
│  CUST-00001  | "darshit " | bad@email | "2024/01/15"    │  ← messy!
│                                                           │
│  Rule: NEVER modify data here. Store exactly as received │
└─────────────────────────────────────────────────────────┘
     │
     ▼  (Silver Transformer cleans this)
┌─────────────────────────────────────────────────────────┐
│  SILVER LAYER  (cleaned, typed, deduplicated)            │
│                                                           │
│  Table: silver.customers                                 │
│  customer_id | first_name | email          | reg_date    │
│  CUST-00001  | Darshit    | bad@email→NULL | 2024-01-15  │  ← clean!
│                                                           │
│  Rules: Fix types, remove duplicates, validate emails    │
└─────────────────────────────────────────────────────────┘
     │
     ▼  (dbt builds this)
┌─────────────────────────────────────────────────────────┐
│  GOLD LAYER  (analytics-ready, business logic applied)   │
│                                                           │
│  Table: gold.dim_customers                               │
│  customer_id | full_name | lifetime_value | rfm_segment  │
│  CUST-00001  | Darshit P | $1,234.56      | Champions    │  ← insights!
│                                                           │
│  Rules: Joins, aggregations, business metrics, KPIs      │
└─────────────────────────────────────────────────────────┘
```

**Why three layers?**
- If something breaks, you can always re-process from Bronze (raw data is preserved)
- Each layer has one clear responsibility (separation of concerns)
- Debug issues at the right layer

---

### 4.3 Star Schema (Fact & Dimension Tables)

A **star schema** is a way to organize database tables for fast analytics.

**Two types of tables:**

**Dimension tables** ("who/what/when"):
- `dim_customers` — who bought?
- `dim_products` — what was bought?
- `dim_date` — when was it bought?
- Contain descriptive attributes (names, categories, segments)

**Fact tables** ("what happened"):
- `fact_orders` — one row per order line item
- Contain numbers (revenue, quantity, profit)
- Link to dimension tables via foreign keys

```
          dim_date
         (when?)
            │
dim_customers ──── fact_orders ──── dim_products
(who?)              (events)         (what?)
```

Why star schema? Because this query runs very fast:
```sql
-- "Monthly revenue by product category for B2C customers in California"
SELECT d.month_name, p.category, SUM(f.gross_revenue)
FROM fact_orders f
JOIN dim_date      d ON f.date_id = d.date_id
JOIN dim_products  p ON f.product_id = p.product_id
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE c.state = 'CA'
  AND c.customer_segment = 'B2C'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

### 4.4 ETL vs ELT

**ETL (Extract → Transform → Load)** — old approach:
```
Source → [Transform in memory] → Load into DW
Problem: transformation happens outside the DW, hard to debug
```

**ELT (Extract → Load → Transform)** — modern approach (what we use):
```
Source → Load raw into Bronze → Transform INSIDE the DW using SQL (dbt)
Benefit: all data is in the warehouse, transformations are SQL (easy to debug, version control)
```

This project uses **ELT**:
1. **Extract**: Python reads CSV files
2. **Load**: Python loads raw data into Bronze (PostgreSQL)
3. **Transform**: dbt transforms Bronze→Silver→Gold inside PostgreSQL

---

### 4.5 RFM Analysis

**RFM** stands for **Recency, Frequency, Monetary** — a classic customer segmentation technique.

| Metric | Question | How calculated |
|--------|----------|----------------|
| **Recency** | When did this customer last buy? | Days since last order |
| **Frequency** | How often do they buy? | Total completed orders |
| **Monetary** | How much do they spend? | Total revenue from customer |

Each customer gets a score from 1-5 for each metric (5 = best), then they're grouped:

| Segment | Meaning | R | F | M |
|---------|---------|---|---|---|
| **Champions** | Best customers — buy often, recently, high value | 5 | 5 | 5 |
| **Loyal Customers** | Regular buyers | 3-4 | 3-4 | 3-4 |
| **At Risk** | Used to be great, haven't bought recently | 1-2 | 3-5 | 3-5 |
| **Cannot Lose Them** | High-value but inactive | 1 | 4-5 | any |
| **Hibernating** | Low on all metrics | 1-2 | 1-2 | any |
| **New Customers** | Bought recently but only once | 4-5 | 1-2 | any |

**In dbt (from `dim_customers.sql`):**
```sql
-- Assign 1-5 scores using NTILE window function
NTILE(5) OVER (ORDER BY last_order_date DESC) AS recency_score
```

---

### 4.6 Cohort Analysis

A **cohort** is a group of users who did something in the same time period.

**Example:** All customers who signed up in January 2024 form the "Jan 2024 cohort."

**Cohort retention** answers: "Of the customers who signed up in January, what % are still buying 3 months later?"

```
Cohort   | Month 0 | Month 1 | Month 2 | Month 3 | Month 6
---------|---------|---------|---------|---------|--------
Jan 2024 | 100%    | 45%     | 38%     | 32%     | 21%
Feb 2024 | 100%    | 42%     | 35%     | 30%     | 19%
Mar 2024 | 100%    | 48%     | 40%     | 35%     | 23%
```

This is shown as a **heatmap** in the Customer Analytics dashboard. Green = high retention, red = customers leaving.

---

---

## 5. Project Architecture (How Everything Connects)

```
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Data Generation                                                │
│  generate_ecommerce_data.py                                              │
│  Creates fake: customers.csv, products.csv, orders.csv, order_items.csv  │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  CSV files written to data/raw/
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Bronze Ingestion  (ingestion/bronze_loader.py)                 │
│                                                                           │
│  Reads CSV → Validates schema → Deduplicates → Loads into PostgreSQL     │
│  Destination: bronze.raw_customers, bronze.raw_orders, etc.              │
│  Key feature: Logs every run to bronze.pipeline_runs (audit trail)       │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  Raw data now in database
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Silver Transformation  (transformation/silver_transformer.py)  │
│                                                                           │
│  Reads bronze.* → Cleans types → Validates → Writes to silver.*         │
│  • Emails validated with regex                                            │
│  • Dates parsed from multiple formats (YYYY-MM-DD, MM/DD/YYYY, etc.)    │
│  • Numeric strings converted to DECIMAL                                  │
│  • Referential integrity checked (orders must have valid customers)      │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  Clean data in silver.*
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Gold Layer  (dbt_project/)                                     │
│                                                                           │
│  staging/     → Light renaming (views, fast, no storage)                 │
│  intermediate/→ Business logic (RFM scores, product metrics)             │
│  marts/core/  → Star schema (dim_customers, dim_products, fact_orders)   │
│  marts/marketing/ → Analytics tables (daily_sales, customer_ltv)        │
│                                                                           │
│  dbt auto-detects dependencies and builds in correct order               │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  Analytics tables in gold.*
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 5 — Dashboard  (dashboard/)                                        │
│                                                                           │
│  Streamlit reads from gold.* and displays:                               │
│  • Executive KPIs (revenue, orders, margins)                             │
│  • Sales trends with 7-day moving averages                               │
│  • RFM customer segmentation treemap                                     │
│  • Cohort retention heatmap                                              │
│  • Product performance sunburst chart                                    │
│  • US state choropleth sales map                                         │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION (runs all 5 steps above automatically)                    │
│  airflow/dags/daily_etl_pipeline.py                                      │
│  → Runs every day at 06:00 UTC                                           │
│  → Retries failed tasks with exponential backoff                         │
│  → Sends alerts if pipeline fails                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Project Folder Structure

```
datapulse-analytics/
│
├── README.md                          ← You are here
├── docker-compose.yml                 ← Defines all services (Postgres, Dashboard, Airflow)
├── Dockerfile.dashboard               ← Instructions to build the dashboard container
├── Makefile                           ← Shortcut commands (make pipeline, make test, etc.)
├── requirements.txt                   ← Python packages to install
├── .env.example                       ← Copy this to .env and fill in your settings
├── .gitignore                         ← Files git should not track (passwords, data files)
│
├── sql/
│   ├── init.sql                       ← Creates all database schemas and tables
│   └── create_airflow_db.sh           ← Creates Airflow's own metadata database
│
├── data/
│   └── raw/                           ← Generated CSV files go here (git-ignored)
│
├── data_generation/
│   └── generate_ecommerce_data.py     ← Creates fake realistic retail data
│
├── ingestion/
│   └── bronze_loader.py               ← Loads raw CSVs into bronze.* tables
│
├── transformation/
│   └── silver_transformer.py          ← Cleans bronze.* into silver.* tables
│
├── dbt_project/                       ← The entire Gold layer lives here
│   ├── dbt_project.yml                ← dbt configuration (model paths, schemas)
│   ├── profiles.yml                   ← Database connection settings for dbt
│   └── models/
│       ├── staging/                   ← Thin views over Silver tables
│       │   ├── stg_customers.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_products.sql
│       │   ├── stg_order_items.sql
│       │   └── schema.yml             ← Column descriptions + dbt tests
│       ├── intermediate/              ← Business logic aggregations
│       │   ├── int_customer_orders.sql
│       │   └── int_product_metrics.sql
│       └── marts/
│           ├── core/                  ← Core data model (star schema)
│           │   ├── dim_date.sql
│           │   ├── dim_customers.sql
│           │   ├── dim_products.sql
│           │   └── fact_orders.sql
│           └── marketing/            ← Derived analytics tables
│               ├── customer_ltv.sql
│               └── daily_sales.sql
│
├── airflow/
│   └── dags/
│       └── daily_etl_pipeline.py     ← The Airflow DAG scheduling the pipeline
│
├── dashboard/
│   ├── app.py                        ← Main page: Executive KPI Dashboard
│   ├── utils/
│   │   └── db_connection.py          ← Reusable database connection helper
│   └── pages/
│       ├── 1_Sales_Overview.py       ← Revenue trends, order patterns
│       ├── 2_Customer_Analytics.py   ← RFM, cohorts, LTV, churn
│       └── 3_Product_Performance.py  ← Top products, margins, inventory
│
└── tests/
    └── test_pipeline.py              ← pytest unit tests for all components
```

---

## 7. Quick Start — Step by Step

### Prerequisites

Install these before starting:

| Tool | Download | Why |
|------|---------|-----|
| **Python 3.10+** | [python.org](https://python.org) | Run pipeline scripts |
| **Docker Desktop** | [docker.com](https://www.docker.com/products/docker-desktop/) | Run PostgreSQL & dashboard |
| **Git** | [git-scm.com](https://git-scm.com) | Clone the project |

> **Check versions:**
> ```bash
> python --version    # Should be 3.10+
> docker --version    # Should be 20+
> git --version
> ```

---

### Step 1 — Get the project

```bash
git clone https://github.com/your-username/datapulse-analytics.git
cd datapulse-analytics
```

---

### Step 2 — Create your settings file

```bash
cp .env.example .env
```

The `.env` file stores your database credentials. The defaults work fine for local development:
```bash
POSTGRES_USER=datapulse
POSTGRES_PASSWORD=datapulse123
POSTGRES_DB=retail_dw
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

> Never commit your `.env` file to git — it's already in `.gitignore`

---

### Step 3 — Start the database

```bash
docker-compose up -d postgres
```

What happens:
1. Docker downloads the PostgreSQL 15 image (~150MB, first time only)
2. Starts a PostgreSQL database on port 5432
3. Automatically runs `sql/init.sql` which creates all schemas and tables

Verify it's running:
```bash
docker ps
# Should show: datapulse_postgres ... Up
```

---

### Step 4 — Install Python dependencies

```bash
# Create a virtual environment (recommended — keeps your system Python clean)
python -m venv venv
source venv/bin/activate          # Mac/Linux
# venv\Scripts\activate           # Windows

# Install all packages
pip install -r requirements.txt
```

This installs pandas, sqlalchemy, faker, streamlit, plotly, dbt-postgres, pytest, and more.

---

### Step 5 — Generate synthetic data

```bash
python data_generation/generate_ecommerce_data.py
```

Expected output:
```
============================================================
DataPulse — Synthetic Data Generator
Date range: 2023-01-01 → 2024-12-31
============================================================

[1/4] Generating customers...
  ✓ Generated 1000 customers
  ✓ Saved 1,000 rows → ./data/raw/customers.csv (98.5 KB)

[2/4] Generating product catalog...
  ✓ Generated 300 products
  ✓ Saved 300 rows → ./data/raw/products.csv (24.3 KB)

[3/4] Generating orders & line items...
  ✓ Generated 8000 orders with 18,432 line items

[4/4] Generating clickstream events...
  ✓ Generated 30,000 clickstream events

DATA GENERATION SUMMARY
  Customers  :    1,000
  Orders     :    8,000
  Total Rev  : $2,341,567.89
```

---

### Step 6 — Load data into Bronze layer

```bash
python ingestion/bronze_loader.py
```

This reads the CSVs and loads them into `bronze.raw_*` tables in PostgreSQL.

---

### Step 7 — Clean data into Silver layer

```bash
python transformation/silver_transformer.py
```

This reads from `bronze.*`, cleans and types everything, writes to `silver.*`.

---

### Step 8 — Build Gold layer with dbt

```bash
cd dbt_project

# Install dbt dependencies (first time only)
dbt deps

# Run all models
dbt run

# Test data quality
dbt test
```

Expected output from `dbt run`:
```
Running with dbt=1.7.3
Found 10 models, 20 tests, 0 snapshots

Concurrency: 4 threads

1 of 10 START sql view model silver.stg_customers ......................... [RUN]
1 of 10 OK created sql view model silver.stg_customers ..................... [CREATE VIEW in 0.12s]
...
10 of 10 OK created sql table model gold.daily_sales ....................... [SELECT 731 in 3.21s]

Finished running 4 views, 6 tables in 12.34s.
PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

---

### Step 9 — Start the dashboard

```bash
cd ..   # Back to project root

# Option A: Docker (recommended)
docker-compose up -d dashboard
# Visit: http://localhost:8501

# Option B: Local
streamlit run dashboard/app.py
# Visit: http://localhost:8501
```

---

### All in one command (after setup)

```bash
make pipeline
```

This runs: generate → ingest → transform → dbt run → dbt test

---

## 8. Running Each Layer Individually

```bash
# Generate data only
make generate

# Bronze load only
make ingest

# Silver transform only
make transform

# dbt run only
make dbt-run

# dbt test only
make dbt-test

# Open dbt documentation in browser
make dbt-docs

# Start Airflow scheduler
make up-airflow
# Visit: http://localhost:8080 (admin / admin)

# Run unit tests
make test

# Lint code
make lint
```

---

## 9. The Dashboard — What You'll See

### Page 1: Executive Dashboard (`app.py`)

**Top KPI cards:**
- Total Revenue, Total Orders, Unique Customers, Avg Order Value, Gross Profit, Margin %

**Charts:**
- Monthly revenue trend (area chart, color-coded by year)
- Revenue by category (donut chart)
- Category performance bar chart (Revenue vs Gross Profit)
- US state sales heatmap (choropleth map)

---

### Page 2: Sales Overview (`1_Sales_Overview.py`)

Three tabs:
1. **Revenue & Profit** — Area chart showing revenue vs gross profit over time
2. **Orders & AOV** — Bar + line combo chart (order count + avg order value)
3. **Deep Analysis** — Order status distribution, shipping method scatter, day-of-week revenue

---

### Page 3: Customer Analytics (`2_Customer_Analytics.py`)

Four tabs:
1. **RFM Segments** — Treemap of customer segments + revenue contribution bars + bubble chart
2. **Cohort Retention** — Heatmap showing monthly retention rates per signup cohort
3. **LTV Analysis** — Histogram of lifetime values, scatter of LTV vs orders, top 20 customers table
4. **Churn Signals** — Pie of active/at-risk/churned customers, revenue at risk by status

---

### Page 4: Product Performance (`3_Product_Performance.py`)

Four tabs:
1. **Top Products** — Horizontal bar (ranked by revenue/units/profit), price vs margin scatter
2. **Category Analysis** — Revenue vs margin scatter, margin tier distribution, sunburst chart
3. **Brand Performance** — Revenue/profit by brand, detail table
4. **Inventory Health** — Stacked bar by stock status, out-of-stock alerts

---

## 10. Data Model (Star Schema Diagram)

```
                         ┌──────────────────────────┐
                         │       dim_date            │
                         │──────────────────────────│
                         │ date_id (PK)              │
                         │ date, year, quarter       │
                         │ month, month_name         │
                         │ week_of_year              │
                         │ day_of_week, day_name     │
                         │ is_weekend                │
                         │ first_day_of_month        │
                         └─────────────┬────────────┘
                                       │ FK: date_id
                                       │
┌─────────────────────┐  FK: customer_id ┌────────────────────────────┐  FK: product_id ┌─────────────────────┐
│   dim_customers     │◄──────────────── │       fact_orders           │ ───────────────►│    dim_products     │
│─────────────────────│                  │────────────────────────────│                  │─────────────────────│
│ customer_id (PK)    │                  │ item_id (PK)               │                  │ product_id (PK)     │
│ first_name          │                  │ order_id                   │                  │ product_name        │
│ last_name, full_name│                  │ customer_id (FK)           │                  │ category            │
│ email               │                  │ product_id (FK)            │                  │ subcategory         │
│ city, state         │                  │ date_id (FK)               │                  │ brand               │
│ customer_segment    │                  │ order_date                 │                  │ cost_price          │
│ registration_date   │                  │ order_status               │                  │ selling_price       │
│ lifetime_value      │                  │ shipping_method            │                  │ margin_pct          │
│ rfm_segment         │                  │ customer_segment           │                  │ margin_tier         │
│ customer_tier       │                  │ category, brand            │                  │ stock_quantity      │
│ churn_status        │                  │ quantity                   │                  │ performance_tier    │
└─────────────────────┘                  │ unit_price                 │                  └─────────────────────┘
                                         │ discount_pct               │
                                         │ gross_revenue  ← measures  │
                                         │ cogs           ← measures  │
                                         │ gross_profit   ← measures  │
                                         └────────────────────────────┘
```

**How to read this:**
- `fact_orders` is in the centre — it has the numbers (revenue, profit, quantity)
- The surrounding tables (`dim_*`) describe context (who, what, when)
- `(PK)` = Primary Key (unique identifier for each row)
- `(FK)` = Foreign Key (links to another table's primary key)

---

## 11. dbt Models — Layer by Layer

### Staging Layer (`models/staging/`)
**Purpose:** Thin, lightweight views over Silver tables. Just renaming columns and adding derived flags. No complex business logic.

```sql
-- stg_orders.sql (simplified)
SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    CASE WHEN order_status = 'completed' THEN TRUE ELSE FALSE END AS is_completed,
    EXTRACT(YEAR FROM order_date) AS order_year
FROM silver.orders
```

### Intermediate Layer (`models/intermediate/`)
**Purpose:** Complex aggregations and business logic that will be reused by multiple mart models.

```sql
-- int_customer_orders.sql (simplified)
-- "How many orders has each customer made, and what's their total spend?"
SELECT
    customer_id,
    COUNT(order_id) FILTER (WHERE is_completed)   AS completed_orders,
    SUM(total_amount) FILTER (WHERE is_completed) AS total_revenue,
    MAX(order_date) FILTER (WHERE is_completed)   AS last_order_date
FROM stg_orders
GROUP BY customer_id
```

### Marts Layer (`models/marts/`)
**Purpose:** Final tables that analysts and dashboards query directly.

| Model | Layer | Type | What it contains |
|-------|-------|------|-----------------|
| `dim_date` | core | table | Calendar spine with 1 row per day (2023–2025) |
| `dim_customers` | core | table | Customer profiles + RFM scores + LTV |
| `dim_products` | core | table | Product catalog + performance metrics |
| `fact_orders` | core | table | One row per order line item, all measures |
| `daily_sales` | marketing | table | Pre-aggregated daily KPIs with window functions |
| `customer_ltv` | marketing | table | LTV + cohort + churn prediction signals |

### dbt Ref Graph (how models depend on each other)
```
silver.customers ──► stg_customers ──────────────────────────┐
silver.orders    ──► stg_orders    ──► int_customer_orders ──► dim_customers
silver.products  ──► stg_products  ──► int_product_metrics ──► dim_products
silver.order_items ► stg_order_items ─────────────────────────► fact_orders
                                                           ──► daily_sales
                                                   dim_date ──► fact_orders
```

---

## 12. Airflow DAG — How the Pipeline is Scheduled

The DAG file is at `airflow/dags/daily_etl_pipeline.py`.

**Schedule:** `"0 6 * * *"` = Run at 06:00 UTC every day

**Cron syntax cheat sheet:**
```
"0 6 * * *"
 │ │ │ │ └── day of week (0=Sun, 6=Sat, *=every)
 │ │ │ └──── month (*=every)
 │ │ └────── day of month (*=every)
 │ └──────── hour (6 = 6 AM)
 └────────── minute (0 = top of hour)
```

**Task flow:**
```
pipeline_start
      │
      ▼
check_source_files (BranchPythonOperator)
      │                    │
      │ files exist        │ no files
      ▼                    ▼
  bronze_load         generate_data
      ▲                    │
      └────────────────────┘
      │
      ▼
silver_transform
      │
      ▼
data_quality_checks   ← Fails the pipeline if data quality issues found
      │
      ├── dbt_deps
      │      │
      │   dbt_run
      │      │
      │   dbt_test   ← Fails if any dbt test fails
      │      │
      │   dbt_docs_generate
      │
      ▼
pipeline_end
```

**Retry policy:**
- 2 automatic retries per task
- Exponential backoff: fails at 5m → waits 5m → retry → waits 10m → retry → alerts

---

## 13. Running Tests

### Unit tests (pytest)
```bash
pytest tests/ -v
```

These tests verify that:
- `clean_email("not-an-email")` returns `None`
- `clean_date("2024-03-15")` returns `date(2024, 3, 15)`
- Generated customers always have `CUST-` prefix
- Product cost never exceeds selling price
- Order items always reference valid orders

### dbt tests
```bash
cd dbt_project && dbt test
```

These are **data quality tests** that run SQL checks against real data:
- `unique` — no duplicate primary keys
- `not_null` — required columns are never empty
- `accepted_values` — status must be one of ['completed', 'shipped', 'processing', ...]
- `relationships` — every `customer_id` in `fact_orders` must exist in `dim_customers`

### Coverage report
```bash
pytest tests/ --cov=ingestion --cov=transformation --cov-report=html
open htmlcov/index.html   # Visual coverage report in browser
```

---

## 14. Learning Path — Where to Go Next

If this project introduced you to new concepts, here's a structured path to deepen your skills:

### Beginner (0–3 months)
- **SQL fundamentals** → [SQLZoo](https://sqlzoo.net/), [Mode SQL Tutorial](https://mode.com/sql-tutorial/)
- **Python for data** → [Python for Everybody (Coursera)](https://www.coursera.org/specializations/python)
- **pandas** → [Kaggle pandas course (free)](https://www.kaggle.com/learn/pandas)

### Intermediate (3–6 months)
- **dbt** → [dbt Fundamentals (free)](https://learn.getdbt.com/courses/dbt-fundamentals)
- **Data Modeling** → *The Data Warehouse Toolkit* by Kimball (book)
- **PostgreSQL advanced SQL** → [pgexercises.com](https://pgexercises.com/)
- **Docker** → [Docker Getting Started](https://docs.docker.com/get-started/)

### Advanced (6–12 months)
- **Apache Airflow** → [Astronomer Learn](https://www.astronomer.io/learn/)
- **Apache Spark** → [Databricks Learning](https://www.databricks.com/learn)
- **Cloud Platforms** → AWS (Redshift, Glue), GCP (BigQuery, Dataflow), Azure (Synapse)
- **Streaming** → Apache Kafka fundamentals

### Certifications worth pursuing:
| Certification | Provider | For |
|--------------|---------|-----|
| dbt Certified Developer | dbt Labs | Data Engineers |
| Google Professional Data Engineer | Google | Cloud Data Engineers |
| AWS Certified Data Analytics | AWS | Cloud Data Engineers |
| Databricks Certified Associate | Databricks | Spark/Lakehouse Engineers |
| Tableau / Power BI Desktop | Tableau / Microsoft | Data Analysts |

### How to extend this project:
1. **Add Apache Kafka** — Real-time event streaming instead of CSV files
2. **Add ML forecasting** — Use scikit-learn to forecast next month's revenue
3. **Move to cloud** — Replace local Postgres with BigQuery or Redshift
4. **Add Great Expectations** — More sophisticated data quality framework
5. **Add a Data Catalog** — Document all datasets with Apache Atlas or DataHub
6. **Add PySpark** — Process the data at scale with Spark instead of pandas

---

## 15. Common Errors & How to Fix Them

### "Connection refused" when running bronze_loader.py
```
Error: could not connect to server: Connection refused (port 5432)
```
**Fix:** PostgreSQL isn't running.
```bash
docker-compose up -d postgres
docker ps   # Verify it shows "Up"
```

### "Module not found" error
```
ModuleNotFoundError: No module named 'pandas'
```
**Fix:** Virtual environment not activated, or packages not installed.
```bash
source venv/bin/activate      # Activate venv
pip install -r requirements.txt
```

### dbt can't connect to database
```
RuntimeError: Database Error: could not connect to server
```
**Fix:** Check your `dbt_project/profiles.yml` matches your `.env` file. Make sure Postgres is running.

### dbt test failures
```
Failure in test unique_dim_customers_customer_id
```
**Fix:** The Silver transformer likely loaded duplicate rows. Reset and re-run:
```bash
make reset-db    # WARNING: deletes all data
make pipeline    # Re-run everything
```

### Streamlit shows "Error loading dashboard data"
**Fix:** The dbt Gold layer hasn't been built yet. Run:
```bash
cd dbt_project && dbt run
```

### Docker port already in use
```
Error: Bind for 0.0.0.0:5432 failed: port is already allocated
```
**Fix:** Another PostgreSQL is running. Stop it or change the port in `docker-compose.yml`.
```bash
# Stop all docker containers
docker-compose down
# Or change port in docker-compose.yml: "5433:5432"
```

---

## 16. Glossary

| Term | Definition |
|------|-----------|
| **Bronze layer** | Raw data exactly as received from source, never modified |
| **Silver layer** | Cleaned, typed, and validated data |
| **Gold layer** | Analytics-ready tables with business logic applied |
| **DAG** | Directed Acyclic Graph — a workflow where tasks flow in one direction |
| **dbt** | Data Build Tool — runs SQL SELECT statements and builds tables/views |
| **Dimension table** | A table describing "who/what/when" (e.g., dim_customers) |
| **ELT** | Extract, Load, Transform — load raw first, then transform inside DW |
| **ETL** | Extract, Transform, Load — transform before loading (older approach) |
| **Fact table** | A table storing measurements/events (e.g., fact_orders with revenue) |
| **Foreign Key (FK)** | A column that references another table's primary key |
| **Idempotent** | Safe to run multiple times — same result every time, no duplicates |
| **Medallion Architecture** | Bronze → Silver → Gold layered data organization pattern |
| **OLAP** | Online Analytical Processing — databases optimized for analytics |
| **OLTP** | Online Transactional Processing — databases for live app operations |
| **Primary Key (PK)** | A column (or set of columns) that uniquely identifies each row |
| **RFM** | Recency, Frequency, Monetary — customer segmentation technique |
| **Schema** | A named container for database tables (like a folder) |
| **SLA** | Service Level Agreement — a time limit for pipeline completion |
| **Star Schema** | Database design with a central fact table and surrounding dimension tables |
| **Window function** | SQL function that calculates over a range of rows (e.g., running total) |
| **XCom** | Airflow's way for tasks to pass data to each other |

---

## Tech Stack Summary

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Ingestion** | Python + pandas | 3.11 / 2.x | Raw CSV → Bronze |
| **Storage** | PostgreSQL | 15 | Data Warehouse (3 schemas) |
| **Transformation** | dbt-core | 1.7.3 | Silver → Gold (SQL models) |
| **Orchestration** | Apache Airflow | 2.7.3 | Daily pipeline scheduling |
| **Visualization** | Streamlit + Plotly | 1.29 / 5.17 | Interactive dashboard |
| **Containerization** | Docker Compose | v3.9 | Reproducible environments |
| **Testing** | pytest + dbt test | 7.4 / built-in | Unit + data quality tests |
| **CI/CD** | GitHub Actions | — | Auto lint/test on push |
| **Data Generation** | Faker + NumPy | 21.0 / 1.26 | Realistic synthetic data |

---

## Author

Built as a portfolio project to demonstrate end-to-end data engineering and analytics skills.

**Connect**: [LinkedIn](https://linkedin.com) | [GitHub](https://github.com)

---

*"The goal of a data engineer is to make data analysts' jobs easier. The goal of a data analyst is to make business decisions easier. This project demonstrates both."*
