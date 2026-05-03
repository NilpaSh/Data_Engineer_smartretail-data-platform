# SmartRetail Data Platform 🏪

> **Author:** Nilpa &nbsp;|&nbsp; **Created:** 2026-05-03

---

## 📖 Table of Contents

1. [What Is This Project?](#-what-is-this-project)
2. [Who Is This For?](#-who-is-this-for)
3. [What Problem Does It Solve?](#-what-problem-does-it-solve)
4. [Technology Overview – Explained Simply](#-technology-overview--explained-simply)
5. [Architecture – The Big Picture](#-architecture--the-big-picture)
6. [Medallion Architecture Explained](#-medallion-architecture-explained)
7. [Project Folder Structure](#-project-folder-structure)
8. [File-by-File Explanation](#-file-by-file-explanation)
9. [How Data Flows Through the System](#-how-data-flows-through-the-system)
10. [SQL – What Was Built and Why](#-sql--what-was-built-and-why)
11. [Power BI Dashboards](#-power-bi-dashboards)
12. [Python & NumPy – What Was Built](#-python--numpy--what-was-built)
13. [Microsoft Fabric – Cloud Data Platform](#-microsoft-fabric--cloud-data-platform)
14. [Quick Start – Run It Yourself](#-quick-start--run-it-yourself)
15. [Resume Highlights](#-resume-highlights)
16. [Data Dictionary](#-data-dictionary-quick-reference)

---

## 🛍️ What Is This Project?

**SmartRetail Data Platform** is a complete, production-style **data engineering and analytics system** built for a fictional retail e-commerce company.

Imagine a company like Amazon or Best Buy that sells thousands of products to millions of customers every day. They need to:
- Store all that raw data safely
- Clean and organise it
- Analyse it to understand what's selling and who their best customers are
- Show it visually in dashboards so managers can make decisions

**This project builds exactly that system — from raw data all the way to visual dashboards.**

It covers the **entire data journey** in one place, which is why it's a strong portfolio project.

---

## 👥 Who Is This For?

This project is built to demonstrate skills for two job roles:

| Role | What They Do |
|---|---|
| **Data Engineer** | Builds the pipelines that move and transform data |
| **Data Analyst** | Analyses the data and creates reports/dashboards |

This single project covers both — making it ideal to showcase on a resume for either position.

---

## ❓ What Problem Does It Solve?

A retail company has data scattered everywhere:
- Customer records in a CRM (Customer Relationship Management) system
- Orders in a transactions database
- Product info in a separate catalog
- Website clicks in web logs

**The problem:** All of this data is in different formats, has missing values, duplicate records, and can't be used for analysis directly.

**The solution this project provides:**
1. Pull all that raw data into one place (Bronze)
2. Clean and validate it (Silver)
3. Organise it into a format perfect for analysis (Gold)
4. Build dashboards and forecasts on top of it (Power BI + Python)

---

## 🔧 Technology Overview – Explained Simply

### 🐍 Python
**What it is:** A programming language widely used for data work.  
**What it does here:** Orchestrates the entire pipeline — generates test data, loads files, runs transformations, and performs statistical analysis.  
**Think of it as:** The engine that drives everything.

---

### 🔢 NumPy
**What it is:** A Python library for fast mathematical operations on large arrays of numbers.  
**What it does here:**
- Generates realistic distributions for customer ages (bimodal curve), product prices (log-normal), and stock levels (Poisson distribution)
- Detects statistical outliers using Z-scores and Median Absolute Deviation (MAD)
- Calculates revenue statistics, rolling averages, and RFM scores
- Runs Fast Fourier Transform (FFT) to detect seasonal patterns in sales data

**Think of it as:** A super-powered calculator that works on millions of numbers at once — much faster than regular Python loops.

---

### 🐼 Pandas
**What it is:** A Python library for working with tables of data (like Excel, but in code).  
**What it does here:** Reads and writes Parquet files, joins tables, filters rows, handles missing values, and prepares data for loading.  
**Think of it as:** Excel inside Python.

---

### 🗄️ SQL Server (T-SQL)
**What it is:** Microsoft's relational database system. T-SQL is the flavour of SQL it uses.  
**What it does here:**
- Stores the final clean data in structured tables
- Runs complex analytical queries (window functions, CTEs, PIVOT)
- Contains stored procedures that refresh the Gold layer automatically
- Enforces data integrity with constraints (primary keys, foreign keys, check constraints)

**Think of it as:** A very organised filing cabinet that can also do complex calculations.

---

### 🏭 Microsoft Fabric
**What it is:** Microsoft's all-in-one cloud data platform (launched 2023). It combines data storage, data engineering, data science, and Power BI in one place.  
**What it does here:**
- **Lakehouse:** Stores all raw and processed data files (like a giant folder in the cloud)
- **Notebooks:** Runs PySpark code to process large datasets that are too big for a single laptop
- **Data Pipelines:** Automates the entire ETL process on a schedule (every day at 6am, for example)
- **OneLake:** Microsoft's unified data lake storage — one central place for all your data

**Think of it as:** The cloud office building where all the data work happens automatically.

---

### ⚡ PySpark (Apache Spark)
**What it is:** A framework for processing very large datasets across multiple computers simultaneously.  
**What it does here:** Runs inside Fabric Notebooks to process millions of rows faster than a single machine could.  
**Think of it as:** Instead of one worker processing data, you have 100 workers doing it in parallel.

---

### 🏷️ Delta Lake
**What it is:** A storage format for data lakes that adds database-like reliability to file storage.  
**What it does here:**
- Stores Bronze, Silver, and Gold data as Delta tables
- Provides ACID transactions (if something goes wrong midway, no partial writes happen)
- Supports time travel (you can query what the data looked like 7 days ago)
- Handles schema evolution (adding new columns without breaking things)

**Think of it as:** A save system for your data — like video game saves that can't get corrupted.

---

### 📊 Power BI
**What it is:** Microsoft's business intelligence and data visualisation tool.  
**What it does here:** Connects to the Gold layer data and displays interactive dashboards with charts, KPI cards, maps, and tables.  
**Think of it as:** The window that lets managers see the data as easy-to-understand charts.

---

### 📐 DAX (Data Analysis Expressions)
**What it is:** The formula language used inside Power BI to create custom calculations.  
**What it does here:** 50+ custom measures including:
- Year-over-year growth percentages
- Month-to-date and year-to-date totals
- 7-day and 30-day rolling averages
- Customer lifetime value predictions
- RFM customer segmentation scores

**Think of it as:** Excel formulas but specifically designed for Power BI — and much more powerful.

---

### 🧱 dbt (data build tool)
**What it is:** A tool that lets data engineers write SQL transformations and run them in the right order, with built-in testing and documentation.  
**What it does here:** Transforms Silver data into Gold models using SQL `SELECT` statements. Automatically runs tests like "this column should never be NULL" or "order IDs must be unique."  
**Think of it as:** A project manager for SQL — it runs your SQL files in the right order and checks the results.

---

### ✈️ Apache Airflow
**What it is:** A workflow scheduling system that automates running tasks in sequence.  
**What it does here:** Schedules the daily pipeline — ingest at midnight, transform at 1am, aggregate at 2am, refresh dashboards at 3am.  
**Think of it as:** A smart alarm clock that triggers each step of the pipeline automatically.

---

### 🧪 pytest
**What it is:** Python's testing framework.  
**What it does here:** Runs 20+ automated tests to verify that transformations are correct — e.g., verifying that invalid emails are removed, outliers are capped, and revenue totals are mathematically accurate.  
**Think of it as:** A quality inspector that checks everything automatically before data goes live.

---

## 🏗️ Architecture – The Big Picture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
│                                                                      │
│   🏪 Transactions DB  │  👥 CRM System  │  📦 Products  │  🌐 Web  │
└──────────┬───────────────────┬───────────────────┬────────────┬─────┘
           │                   │                   │            │
           ▼                   ▼                   ▼            ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  🥉                                  │
│           "Raw data – stored exactly as received"                    │
│                                                                      │
│    Microsoft Fabric Lakehouse  +  Delta Lake                         │
│    raw_customers │ raw_products │ raw_orders │ raw_web_events        │
│                                                                      │
│    ✅ Every row preserved    ✅ Audit columns added                   │
│    ✅ Batch ID tracked       ✅ Row hash for dedup detection          │
└──────────────────────────────────┬───────────────────────────────────┘
                                   │
                                   │  🐍 Python + NumPy
                                   │  (clean, validate, standardise)
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  🥈                                  │
│           "Clean, validated, conformed data"                         │
│                                                                      │
│    stg_customers │ stg_products │ stg_orders │ stg_order_items       │
│                                                                      │
│    ✅ Duplicates removed     ✅ Emails validated                      │
│    ✅ Age/price bounds checked  ✅ Outliers detected with Z-score     │
│    ✅ Row-level DQ score     ✅ Null handling                         │
└──────────────────────────────────┬───────────────────────────────────┘
                                   │
                                   │  🗄️ SQL + dbt
                                   │  (model, aggregate, enrich)
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  🥇                                    │
│           "Business-ready Star Schema"                               │
│                                                                      │
│    dim_customers │ dim_products │ dim_date │ fact_orders             │
│    agg_daily_sales │ agg_customer_ltv                                │
│                                                                      │
│    ✅ Star schema design     ✅ SCD Type 2 history                    │
│    ✅ Pre-aggregated tables  ✅ RFM scoring + LTV predictions         │
└──────────────────────────────────┬───────────────────────────────────┘
                                   │
                         ┌─────────┴──────────┐
                         │                    │
                         ▼                    ▼
               ┌──────────────────┐  ┌────────────────────┐
               │  📊 Power BI     │  │  🐍 Python Analytics│
               │  Dashboards      │  │  & Forecasting      │
               │                  │  │                     │
               │  Sales Overview  │  │  SARIMA / Prophet   │
               │  Customer 360    │  │  Holt-Winters       │
               │  Product Perf.   │  │  Hypothesis Tests   │
               │  Inventory       │  │  FFT Seasonality    │
               └──────────────────┘  └────────────────────┘
```

---

## 🥉🥈🥇 Medallion Architecture Explained

The **Medallion Architecture** is the most widely used pattern in modern data engineering. It organises data into three quality levels, like medals:

### 🥉 Bronze – "Raw Zone"
> Store everything exactly as it came from the source. Never delete or change anything.

- Raw data dumped in as-is (even if it has errors, nulls, or duplicates)
- Every row gets audit columns: *when was it loaded? from which file? what batch?*
- Acts as a permanent, immutable history of everything received
- **Why?** If something goes wrong later, you can always come back to the original

### 🥈 Silver – "Clean Zone"
> Apply business rules to clean and validate the data.

- Remove duplicate records
- Validate emails with regex (e.g., reject `notanemail`)
- Fix data types (make sure prices are numbers, not text)
- Detect and cap statistical outliers (e.g., a customer age of 500 is clearly wrong)
- Score each row with a data quality score (0–100)
- **Why?** Analysts shouldn't have to deal with messy data every time they write a query

### 🥇 Gold – "Business Zone"
> Organise data into a format perfect for dashboards and analysis.

- Build a **Star Schema** (see below)
- Pre-calculate aggregations (daily totals, customer lifetime value, RFM scores)
- Add business context (loyalty tier, ABC product classification, age groups)
- **Why?** Power BI runs faster and DAX measures are simpler when the data is already modelled correctly

---

## 📁 Project Folder Structure

```
smartretail-data-platform/
│
├── 📄 README.md                  ← You are here
├── 📄 requirements.txt           ← List of all Python packages needed
├── 📄 pyproject.toml             ← Test configuration
├── 📄 .gitignore                 ← Files to exclude from Git
│
├── 📂 data_generation/           ← Creates fake but realistic test data
│   └── generate_data.py          ← Generates customers, orders, products, events
│
├── 📂 ingestion/                 ← Loads raw data into the Bronze layer
│   ├── bronze_ingestion.py       ← Local & incremental Bronze loader
│   └── fabric_ingestion.py       ← Microsoft Fabric / OneLake connector
│
├── 📂 transformation/            ← Cleans (Silver) and models (Gold) data
│   ├── silver_transformation.py  ← Data quality, dedup, type casting
│   └── gold_aggregation.py       ← Star schema, RFM, LTV, daily aggregates
│
├── 📂 sql/                       ← All SQL files
│   ├── ddl/
│   │   ├── create_bronze_tables.sql   ← CREATE TABLE for Bronze zone
│   │   └── create_gold_tables.sql     ← CREATE TABLE for Gold star schema
│   ├── analytics/
│   │   └── sales_analysis.sql         ← Complex analytical queries
│   └── stored_procedures/
│       └── usp_refresh_gold.sql       ← Automated Gold refresh SP
│
├── 📂 fabric/                    ← Microsoft Fabric assets
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.py     ← PySpark Bronze notebook
│   │   └── 02_silver_transformation.py← PySpark Silver notebook
│   └── pipelines/
│       └── daily_etl_pipeline.json   ← Full pipeline definition
│
├── 📂 powerbi/                   ← Power BI assets
│   ├── dax_measures/
│   │   ├── sales_measures.dax         ← Revenue, growth, time intelligence
│   │   ├── customer_measures.dax      ← LTV, RFM, retention, churn
│   │   └── product_measures.dax       ← ABC class, margins, returns
│   └── report_structure/
│       └── report_design.md          ← Page-by-page dashboard spec
│
├── 📂 analytics/                 ← Statistical models & forecasting
│   ├── statistical_analysis.py   ← Descriptive stats, outliers, hypothesis tests
│   └── forecasting.py            ← Holt-Winters, SARIMA, Prophet, Ensemble
│
├── 📂 tests/                     ← Automated tests
│   └── test_pipeline.py          ← 20+ unit & integration tests
│
└── 📂 docs/
    └── data_dictionary.md        ← Every column in every table explained
```

---

## 📄 File-by-File Explanation

### `data_generation/generate_data.py`
Creates 500,000+ rows of synthetic (fake but realistic) data.

| What it generates | How it's made realistic |
|---|---|
| Customer ages | Bimodal distribution — peaks at 27 (young shoppers) and 44 (mature shoppers) |
| Annual income | Log-normal distribution — most people earn middle income, few earn very high |
| Order dates | Sine-wave seasonality — more orders in Nov–Dec (holiday season) |
| Stock levels | Poisson distribution — random but centred around a realistic average |
| Product prices | Log-normal — most products are cheap, a few are expensive |

This demonstrates knowledge of **statistical distributions** — a real skill tested in data interviews.

---

### `ingestion/bronze_ingestion.py`
Loads Parquet files from the source folder into the Bronze zone.

**Key features:**
- Adds 5 audit columns to every row (`_ingested_at`, `_source_file`, `_batch_id`, `_file_row_hash`, `_is_deleted`)
- Supports **append mode** (keep all history) or **overwrite mode** (replace existing)
- `IncrementalBronzeLoader` class tracks the last loaded timestamp (watermark) so it only loads new data on the next run — not everything again

---

### `ingestion/fabric_ingestion.py`
Connects to a real Microsoft Fabric Lakehouse using Azure credentials.

**Key features:**
- Uses `DefaultAzureCredential` — the secure, production-grade way to authenticate with Azure (no passwords in code!)
- Builds the correct OneLake path: `workspaceId/lakehouseId.Lakehouse/Tables/bronze/orders`
- `FabricPipelineTrigger` class calls the Fabric REST API to kick off a pipeline run programmatically

---

### `transformation/silver_transformation.py`
Cleans the Bronze data using Python and NumPy.

**What each method does:**

| Method | What it cleans |
|---|---|
| `transform_customers()` | Validates emails, clamps age 18–100, caps income outliers with IQR, standardises gender |
| `transform_products()` | Checks price > cost, recalculates margins, flags price outliers with Modified Z-score |
| `transform_orders()` | Removes future-dated orders, standardises status values, clamps discount 0–100% |
| `transform_order_items()` | Recalculates `line_total = qty × price`, removes zero/negative quantities |

**Outlier detection methods used (NumPy):**

- **Z-score:** Flags values more than 3 standard deviations from the mean
- **Modified Z-score (MAD-based):** More robust — uses the median instead of mean, better for skewed data
- **IQR fence:** Caps values outside `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]`

---

### `transformation/gold_aggregation.py`
Builds the final business-ready tables.

| Table built | What it contains |
|---|---|
| `dim_date` | Every date from 2019–2030 with year, quarter, month, fiscal year, is_weekend etc. |
| `dim_customers` | Customer profiles with loyalty tier, age group, income band, SCD2 history |
| `dim_products` | Product catalog with ABC classification (A=top 20% revenue, B=next 30%, C=rest) |
| `fact_orders` | One row per order line item, with all revenue measures calculated |
| `agg_daily_sales` | Pre-aggregated daily totals with 7-day moving average and MoM growth |
| `agg_customer_ltv` | RFM scores + predicted 24-month lifetime value + customer segment per customer |

---

### `sql/ddl/create_gold_tables.sql`
Defines the database structure (schema) for the Gold layer.

**Highlights:**
- `fact_orders` has a **Clustered Columnstore Index** — this is how you optimise a fact table for analytical queries in SQL Server (reads 10× faster than regular indexes for aggregations)
- `dim_customers` uses **SCD Type 2** pattern — old records get an end date instead of being overwritten, preserving history
- Foreign key constraints enforce referential integrity between fact and dimension tables

---

### `sql/analytics/sales_analysis.sql`
Five production-quality analytical queries showcasing advanced T-SQL:

| Query | SQL features used |
|---|---|
| YTD/MTD/QTD Revenue | `SUM() OVER`, `LAG()`, `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Cohort Retention Matrix | Multi-step CTE chain, date arithmetic, percentage calculation |
| ABC-XYZ Product Analysis | `STDEV()`, `PERCENT_RANK()`, `NTILE()`, cumulative revenue |
| Customer Segmentation | `PERCENT_RANK() OVER`, `NTILE(10)`, multi-join |
| Dynamic Channel PIVOT | Dynamic SQL, `STRING_AGG`, `sp_executesql`, `PIVOT` |

---

### `sql/stored_procedures/usp_refresh_gold.sql`
An enterprise-grade stored procedure that:
1. Validates Silver data quality before doing anything
2. Performs a MERGE (upsert) for `dim_customers` SCD2 logic
3. Does an incremental insert into `fact_orders` (only new rows, no duplicates)
4. Refreshes `agg_daily_sales` with a full TRUNCATE + INSERT
5. Logs every step to an audit table with row counts and timestamps
6. Uses `TRY/CATCH` for error handling — if anything fails, it logs the error and re-throws

---

### `fabric/notebooks/01_bronze_ingestion.py`
A PySpark notebook designed to run inside Microsoft Fabric.

- Reads raw Parquet files from the `Files/raw` zone of the Lakehouse
- Adds audit columns using Spark's built-in functions (`current_timestamp()`, `sha2()`)
- Writes Delta tables in **append mode** with `partitionBy("_batch_id")`
- Saves a JSON manifest file recording what was loaded and how many rows

---

### `fabric/notebooks/02_silver_transformation.py`
PySpark Silver transformation with Delta Lake UPSERT:

- Uses `DeltaTable.merge()` — the PySpark equivalent of a SQL MERGE statement
- Runs `OPTIMIZE` with `ZORDER` on all Silver tables after writing (compacts small files for faster reads)
- Applies a custom UDF (user-defined function) for row-level DQ scoring

---

### `fabric/pipelines/daily_etl_pipeline.json`
A Fabric Data Pipeline configuration that chains all steps:

```
Copy Raw Files → Bronze Notebook → Silver Notebook → Gold Notebook
    → Validate Row Counts → IF rows > 0: Refresh Power BI
                            ELSE: Send Teams alert
```

Runs automatically every day. If no rows were loaded to Gold, it sends a Teams webhook alert instead of silently failing.

---

### `powerbi/dax_measures/sales_measures.dax`
30+ DAX measures for the Sales dashboard:

| Category | Examples |
|---|---|
| Base measures | `[Total Revenue]`, `[Gross Margin %]`, `[AOV]` |
| Time intelligence | `[Revenue MTD]`, `[Revenue YTD]`, `[Revenue LY]`, `[YoY Growth %]` |
| Moving averages | `[Revenue 7-Day MA]`, `[Revenue 30-Day MA]` |
| Dynamic comparison | `[Revenue % of Total]`, `[Revenue % of Channel Total]` |
| KPI formatting | `[Revenue KPI Color]` — returns hex colour code for conditional formatting |

---

### `analytics/statistical_analysis.py`
Pure statistical analysis using NumPy and SciPy:

| Analysis | What it tells us |
|---|---|
| Descriptive stats | Mean, median, std dev, skewness, kurtosis, CV of revenue |
| Outlier detection | Consensus outlier flag using Z-score + Modified Z-score + IQR fence |
| Correlation matrix | Which customer features are correlated with high spend |
| Welch t-test | "Do email subscribers spend more than non-subscribers?" (statistically proven) |
| One-way ANOVA | "Does revenue differ across acquisition channels?" |
| FFT seasonality | Identifies the dominant seasonal cycle (e.g., 7-day weekly pattern, 365-day annual) |

---

### `analytics/forecasting.py`
Four revenue forecasting models with ensemble weighting:

| Model | Approach |
|---|---|
| **Holt-Winters** | Manual NumPy implementation — level + trend + seasonal smoothing |
| **Linear Trend** | NumPy `polyfit` — projects the trend line forward |
| **SARIMA** | statsmodels Seasonal ARIMA — classic time-series model |
| **Prophet** | Meta's forecasting library — handles holidays and multiple seasonalities |
| **Ensemble** | Weights the best two models by inverse MAPE — lowest-error model gets more weight |

Outputs a 90-day forecast with confidence intervals.

---

## 🔄 How Data Flows Through the System

**Step-by-step example: A customer places an order**

```
1. Customer "Alice" buys 2 laptops at $999 each on Black Friday

2. BRONZE ingestion (midnight batch)
   → Row lands in bronze.raw_orders with:
     order_id = ORD-00000001
     customer_id = CUST-0000042
     order_date = 2025-11-28
     _batch_id = BATCH_20251128_000015
     _ingested_at = 2025-11-28 00:00:15 UTC

3. SILVER transformation (1am)
   → order_date validated (not in future ✓)
   → discount_pct = 0.15 (clamped to [0,1] ✓)
   → _dq_score = 95.0

4. GOLD aggregation (2am)
   → fact_orders row:
     gross_revenue = 2 × 999 = 1,998.00
     discount_amount = 1,998 × 0.15 = 299.70
     net_revenue = 1,998 - 299.70 = 1,698.30
     date_key = 20251128

5. agg_daily_sales refreshed:
   → 2025-11-28: total_revenue += 1,698.30
   → 7-day moving average recalculated

6. Power BI refreshes at 3am:
   → Sales Overview dashboard shows Black Friday spike
   → YoY Growth % updates automatically
```

---

## 🗄️ SQL – What Was Built and Why

### What is a Star Schema?
A **Star Schema** is the standard way to organise data for analytics. It has two types of tables:

**Dimension tables** (the "who/what/when"):
- `dim_customers` — who bought it
- `dim_products` — what was bought
- `dim_date` — when it was bought

**Fact tables** (the "what happened"):
- `fact_orders` — the actual transactions (one row per item sold)

```
         dim_date
            │
dim_customers ── fact_orders ── dim_products
```

When you connect these tables, you can answer any business question:
- *"How much did Gold-tier customers in California spend on Electronics in Q3 2024?"*

### What is SCD Type 2?
**Slowly Changing Dimension Type 2** is a technique to preserve history when data changes.

Example: A customer moves from California to Texas.

| Without SCD2 (overwrites) | With SCD2 (keeps history) |
|---|---|
| California is gone forever | Old record: CA, valid until 2024-06-15 |
| Can't analyse old orders by location | New record: TX, valid from 2024-06-16 |

This project implements SCD2 on `dim_customers`.

### Window Functions
Window functions let you calculate values *across multiple rows* without collapsing them:

```sql
-- Running total of revenue, ordered by date
SUM(net_revenue) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING)

-- Rank customers by spend within their region
RANK() OVER (PARTITION BY region ORDER BY spend DESC)

-- Compare today's revenue to the same day last year
LAG(daily_revenue, 365) OVER (ORDER BY order_date)
```

---

## 📊 Power BI Dashboards

### Page 1 – Sales Overview
What a manager sees every morning:
- Total revenue today / this month / this year
- Year-over-year growth percentage (green if positive, red if negative)
- Revenue trend line with 7-day moving average to smooth daily noise
- Revenue by channel (which marketing channel drives most sales?)
- Top 10 products by revenue
- Revenue map by US region

### Page 2 – Customer 360
Understanding customers deeply:
- How many customers are active this month vs last month?
- **RFM Segmentation**: Champions (buy often + big spends) vs Lost customers (haven't bought in months)
- **Cohort Retention**: Of customers who first bought in January, how many are still buying in month 6?
- **Predicted Lifetime Value**: Which customers are worth investing in?
- **Churn Risk**: Who is about to stop buying?

### Page 3 – Product Performance
Understanding the product catalog:
- **ABC Classification**: A-products (top 20% of revenue) need priority attention
- Return rate per product — high returns = unhappy customers or misleading listings
- Price vs margin scatter plot — which products make the most profit?
- Products below reorder point — need restocking alert

---

## 🐍 Python & NumPy – What Was Built

### Why NumPy instead of regular Python?
Regular Python processes one number at a time. NumPy processes all numbers simultaneously using optimised C code under the hood.

```python
# Regular Python (slow for 1 million rows)
z_scores = []
for val in revenue_list:
    z = (val - mean) / std
    z_scores.append(z)

# NumPy (same result, 100× faster)
z_scores = (revenue_array - np.mean(revenue_array)) / np.std(revenue_array)
```

### Statistical techniques used
| Technique | Where Used | What It Does |
|---|---|---|
| Log-normal distribution | Product price generation | Models realistic price spread (few cheap, few very expensive) |
| Bimodal distribution | Customer age generation | Two peaks = young adults + middle-aged shoppers |
| Poisson distribution | Stock levels | Random but centred around an average |
| Z-score | Outlier detection | Flags values > 3 standard deviations from mean |
| MAD (Median Absolute Deviation) | Robust outlier detection | Better than Z-score for skewed data |
| IQR fence | Outlier capping | Caps at Q1 − 1.5×IQR and Q3 + 1.5×IQR |
| FFT (Fast Fourier Transform) | Seasonality detection | Finds dominant cycles in the revenue time series |
| Welch's t-test | Hypothesis testing | Tests if email subscribers spend significantly more |
| One-way ANOVA | Channel comparison | Tests if revenue differs across marketing channels |
| Exponential Smoothing | Holt-Winters forecasting | Weights recent data more than old data |
| Polynomial regression | Trend projection | Fits a curve to historical data and extends it forward |

---

## ☁️ Microsoft Fabric – Cloud Data Platform

### What is OneLake?
OneLake is like **OneDrive, but for data**. It's a single storage location where all your data lives in the cloud, accessible by all tools in Fabric.

Path format: `workspace/lakehouse.Lakehouse/Tables/bronze/orders`

### What is a Lakehouse?
A Lakehouse combines the best of:
- **Data Lake** (cheap storage for any file type, any size)
- **Data Warehouse** (structured tables with SQL querying)

### How the pipeline works in Fabric
```
Every day at 6:00 AM UTC:
    1. Pipeline starts automatically
    2. Copies new raw files to OneLake Files/raw/
    3. Runs Bronze notebook → Delta tables written
    4. Runs Silver notebook → DQ applied, UPSERT to Silver Delta
    5. Runs Gold notebook  → Star schema refreshed
    6. Validates row count → if 0 rows, send Teams alert
    7. Triggers Power BI dataset refresh
    8. Done by ~6:15 AM
```

---

## 🚀 Quick Start – Run It Yourself

### Step 1 – Prerequisites
Make sure you have these installed:
- **Python 3.10+** — [Download here](https://www.python.org/downloads/)
- **Git** — [Download here](https://git-scm.com/)
- **SQL Server** (optional, for SQL parts) — [Download Developer Edition free](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)
- **Power BI Desktop** (optional, for dashboards) — [Download free](https://powerbi.microsoft.com/desktop/)

### Step 2 – Install Python dependencies
```bash
# Navigate to the project folder
cd smartretail-data-platform

# Install all required packages
pip install -r requirements.txt
```

### Step 3 – Generate synthetic data
```bash
# Generate 100,000 orders (takes ~30 seconds)
python data_generation/generate_data.py --records 100000 --output data/raw

# For a larger dataset (500k orders)
python data_generation/generate_data.py --records 500000 --output data/raw
```

This creates these files in `data/raw/`:
- `customers.parquet` — customer profiles
- `products.parquet` — product catalog
- `orders.parquet` — order headers
- `order_items.parquet` — order line items
- `web_events.parquet` — clickstream events
- `_manifest.json` — summary of what was generated

### Step 4 – Load Bronze layer
```bash
# Load all tables
python ingestion/bronze_ingestion.py --source data/raw --sink data/bronze

# Or load a single table
python ingestion/bronze_ingestion.py --source data/raw --sink data/bronze --table orders
```

### Step 5 – Run Silver transformation
```bash
# Transform all tables (clean + validate)
python transformation/silver_transformation.py --bronze data/bronze --silver data/silver
```

### Step 6 – Build Gold layer
```bash
# Build all Gold tables (star schema + aggregations)
python transformation/gold_aggregation.py --silver data/silver --gold data/gold
```

### Step 7 – Run statistical analysis
```bash
# Descriptive stats, outlier detection, hypothesis tests
python analytics/statistical_analysis.py

# Revenue forecasting (90 days ahead)
python analytics/forecasting.py
```

### Step 8 – Run tests
```bash
# Run all unit tests
pytest tests/ -v

# Run with coverage report
pytest tests/ -v --cov=. --cov-report=term-missing
```

### Step 9 – Set up SQL (optional)
```bash
# Connect to SQL Server and create all tables
sqlcmd -S localhost -d master -Q "CREATE DATABASE RetailDB"
sqlcmd -S localhost -d RetailDB -i sql/ddl/create_bronze_tables.sql
sqlcmd -S localhost -d RetailDB -i sql/ddl/create_gold_tables.sql
sqlcmd -S localhost -d RetailDB -i sql/stored_procedures/usp_refresh_gold.sql
```

---

## 📌 Resume Highlights

Use these bullet points directly on your resume:

```
• Built end-to-end Medallion Architecture (Bronze→Silver→Gold) pipeline on Microsoft Fabric
  using PySpark, Delta Lake, and Python — processing 500K+ records daily

• Designed T-SQL Star Schema with SCD Type 2, Clustered Columnstore Index,
  Window Functions (LAG, RANK, SUM OVER), CTEs, and automated Stored Procedures

• Developed 50+ Power BI DAX measures including time intelligence (YTD/QTD/MTD),
  YoY growth, rolling averages, RFM segmentation, and Row-Level Security (RLS)

• Implemented statistical data quality framework using NumPy Z-score,
  Modified Z-score (MAD-based), and IQR outlier detection across 1M+ rows

• Built revenue forecasting models (Holt-Winters, SARIMA, Prophet) with
  ensemble weighting by inverse MAPE — 90-day forecast output

• Applied hypothesis testing (Welch t-test, ANOVA) and FFT seasonality detection
  to customer spend patterns using NumPy and SciPy

• Wrote 20+ automated unit tests with pytest covering data generation,
  transformation rules, and Gold aggregation correctness
```

| Skill | Proficiency Level Demonstrated |
|---|---|
| Microsoft Fabric | Lakehouse, Notebooks, Pipelines, OneLake, DirectLake |
| SQL / T-SQL | Advanced (window functions, CTEs, MERGE, PIVOT, stored procedures) |
| Python | Intermediate-Advanced (OOP, functional patterns, CLI tools) |
| NumPy | Intermediate (distributions, vectorised ops, outlier detection, FFT) |
| Pandas | Intermediate (wrangling, joins, aggregations, Parquet I/O) |
| Power BI / DAX | Intermediate-Advanced (50+ measures, time intelligence, RLS) |
| Delta Lake | Working knowledge (ACID transactions, UPSERT, OPTIMIZE) |
| Statistical Analysis | Intermediate (distributions, hypothesis testing, forecasting) |
| Data Modeling | Star Schema, SCD Type 2, Medallion Architecture |
| Testing | pytest, unit tests, data validation |

---

## 📚 Data Dictionary (Quick Reference)

| Table | Layer | Rows (est.) | Key Columns |
|---|---|---|---|
| `raw_customers` | Bronze | 50,000 | customer_id, email, age, loyalty_score |
| `raw_orders` | Bronze | 500,000 | order_id, customer_id, order_date, status |
| `raw_order_items` | Bronze | 1,100,000 | order_item_id, order_id, product_id, quantity, unit_price |
| `stg_customers` | Silver | 49,800 | + _dq_score, validated email, capped income |
| `stg_orders` | Silver | 499,200 | + _dq_score, clamped discount, valid status |
| `dim_date` | Gold | 4,018 | date_key (YYYYMMDD), fiscal_year, is_weekend |
| `dim_customers` | Gold | 49,800 | customer_sk, loyalty_tier, age_group, income_band |
| `dim_products` | Gold | 2,000 | product_sk, abc_class, margin_pct |
| `fact_orders` | Gold | 1,100,000 | gross/net/total revenue, discount_amount |
| `agg_daily_sales` | Gold | ~1,460 | daily revenue, 7d MA, MoM growth |
| `agg_customer_ltv` | Gold | 49,800 | rfm_score, predicted_ltv_24m, customer_segment |

Full column-level documentation is in [docs/data_dictionary.md](docs/data_dictionary.md).

---

## 📄 License
MIT — free to use, modify, and include in your portfolio.

