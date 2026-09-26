# Unit 05 — Data Modeling & Warehousing Concepts: Study Guide

**Scope:** OLTP vs OLAP systems · ETL vs ELT patterns · Dimensional modeling (star vs snowflake schema) · Fact & dimension tables · Slowly changing dimensions (SCDs) · Data lake, data warehouse, and data lakehouse architectures

The examples build on the Unit 3 e-commerce schema (`users`, `products`, `orders`, `order_item`, `addresses`) and turn it into an analytical model. SQL examples use **Snowflake** syntax.

---

## Part A — OLTP vs OLAP Systems

### 1. Two kinds of workloads

| | **OLTP** (Online Transaction Processing) | **OLAP** (Online Analytical Processing) |
|---|---|---|
| Purpose | Run the business: place an order, update stock | Analyze the business: revenue by month, top products |
| Typical query | Read or write **one or a few rows** by key | **Big queries** that scan millions of rows with aggregates (`SUM`, `COUNT`), `GROUP BY`, and many `JOIN`s |
| Schema | **Normalized** (3NF), with ACID transactions | **Denormalized** (star / snowflake schema) |
| Users | The application and many concurrent end users | Analysts, BI dashboards, reports, ML pipelines |
| Data | Current state | History (years of data) |
| Examples | PostgreSQL, MySQL, SQL Server | Snowflake, BigQuery, Redshift, Databricks SQL, ClickHouse |

### 2. Case study: the 9 a.m. query on production

An analyst runs a large reporting query (a full-table scan with joins across `orders`, `order_item`, and `products`) directly on the **production OLTP database** at 9 a.m., which is peak traffic.

- **Resource contention:** the query uses up CPU, memory, disk I/O, and the connection pool, so the app's small, fast transactions slow down.
- **Locks and timeouts:** long-running statements hold locks or snapshots for a long time. App transactions queue behind them, hit their **timeouts**, and fail, so customers can't check out.
- **Lesson:** keep analytics **off** the OLTP system. Copy the data into a separate analytical system (a **data warehouse**), or at the very least use a read replica. That's the reason warehouses and ETL/ELT pipelines exist.

### 3. Row-based vs columnar storage

```
Table: orders(id, city, status, amount)

ROW-BASED (OLTP) — each row stored contiguously
[1, HN, paid, 50][2, HN, paid, 20][3, HCM, created, 70] ...

COLUMNAR (OLAP) — each column stored contiguously
id:     [1, 2, 3, ...]
city:   [HN, HN, HCM, ...]
status: [paid, paid, created, ...]
amount: [50, 20, 70, ...]
```

| | Row-based | Columnar |
|---|---|---|
| Good at | Reading or writing a **whole row** (`SELECT * WHERE id = 42`, `INSERT` one order) | Reading **a few columns across many rows** (`SELECT city, SUM(amount) GROUP BY city`) |
| Why | One disk seek fetches the entire record | The query reads **only the columns it needs**, which cuts disk I/O a lot |
| Compression | Limited (neighboring values have different types) | High (neighboring values have the same type and are often repeated) |
| Weak at | Scanning a single column across billions of rows | Single-row updates and inserts (they touch every column file) |

With a 100-column table, `SUM(amount)` on columnar storage reads about 1/100 of the data a row store would read.

### 4. Columnar compression

Values in one column share a type and are often repeated, so they compress well.

**Dictionary encoding** replaces repeated strings with small integers:

```
Dictionary: 1 → abc, 2 → xyz
Stored:     1, 2, 1, 1, 2, 2        instead of   abc, xyz, abc, abc, xyz, xyz
```

**Run-length encoding (RLE)** stores each run of identical consecutive values as *(value, run length)*:

```
Raw:    HN, HN, HCM, HCM, HCM, DN
Stored: (HN, 2), (HCM, 3), (DN, 1)
```

- RLE only pays off when equal values are **next to each other**, so **sorting or clustering** data on low-cardinality columns makes it much more effective.
- Engines often **stack** the techniques: dictionary-encode first, then RLE or bit-pack the integers.
- Many engines can run filters and aggregates **directly on the compressed data**.

### 5. OLTP database vs data warehouse

| Aspect | OLTP database | Data warehouse |
|---|---|---|
| Storage | Row-based | Columnar |
| Schema | Normalized, ACID | Denormalized (star / snowflake) |
| Compute & storage | **Coupled** (same server) | **Decoupled**: object storage (S3 / Azure Blob / GCS) plus separate virtual warehouses for compute |
| Compression | Limited | High |
| Query execution | Mostly one node; each query uses few resources | **MPP** (Massively Parallel Processing) |
| Concurrency pattern | Many small queries | Few large queries |
| Typical latency | Milliseconds per transaction | Seconds to minutes per query |
| **Don't use it for** | Big aggregates with many joins | Single-row lookups, high-frequency small writes, low-latency app backends |

The latency row compares different kinds of work: a warehouse is "slow" per query, but each query processes far more data.

A **data warehouse** is a **centralized** analytical database that brings data from many sources (orders DB, CRM, web logs) into one place. It runs on a **cluster** of machines and uses a **different query execution plan** from OLTP: parallel scans, partition pruning, and distributed joins.

### 6. MPP (Massively Parallel Processing)

```
                 SELECT city, SUM(amount) FROM orders GROUP BY city
                                   │
                            Coordinator / planner
                    ┌──────────────┼──────────────┐
                 Node 1         Node 2         Node 3
             partition A     partition B     partition C
           HN: 70, HCM: 30  HN: 10, DN: 5   HCM: 40, DN: 15   ← partial aggregates
                    └──────────────┼──────────────┘
                          Merge: HN 80, HCM 70, DN 20
```

- **Partition** the data across nodes. Each node scans and aggregates **its own slice** in parallel, and the coordinator **merges** the partial results.
- Joins may need a **shuffle** (redistributing rows so matching keys end up on the same node) or a **broadcast** (copying a small dimension table to every node).
- Adding nodes makes big scans faster, which is the main reason warehouses can query terabytes in seconds.

### 7. Decoupled compute & storage

- **Storage** lives in cheap object storage (S3 / blob), and you pay per TB stored.
- **Compute** runs in **virtual warehouses** that you **turn on and off independently** and pay for only while they run (**pay-as-you-go**).
- **Different teams get different warehouses** (e.g., `BI_WH`, `DE_WH`, `DS_WH`). All of them read the same data, but a data scientist's heavy job can't slow down the finance dashboard. This avoids the conflict from the 9 a.m. case study.
- You can scale storage and compute separately: more data doesn't force you to buy more compute.

### 8. Snowflake: a cloud data warehouse platform

"Snowflake" is the name of a **cloud data warehouse product** and also of a **schema design** (Part C). The two are unrelated.

Snowflake has **three layers**:

| Layer | What it does |
|---|---|
| **Storage** | Data is stored in **micro-partitions**: immutable, columnar, compressed files of about 50–500 MB (uncompressed) in object storage. Snowflake manages them automatically. There are **no indexes to create**. Snowflake keeps min/max metadata for each column in each micro-partition and **prunes** (skips) partitions a query doesn't need. Because micro-partitions are immutable, features like **Time Travel** are possible. |
| **Compute** | **Virtual warehouses** are independent compute clusters. Sizes go from **XS to 6XL**, and each size up doubles both the compute and the credits per hour. Billing is **per second** (60-second minimum each time a warehouse starts). Warehouses auto-suspend and auto-resume, **many can run in parallel** on the same data, and the usual setup is one warehouse per team or workload. |
| **Cloud services** | Query **parsing, optimization, and planning**. It also handles **metadata**, **security** (auth, roles), **transactions**, and the **result cache**: if the same query runs again and the data hasn't changed, the stored result is returned (for up to 24 hours) without using a warehouse. |

```sql
CREATE WAREHOUSE bi_wh WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;      -- suspend after 60 s idle
USE WAREHOUSE bi_wh;
```

---

## Part B — ETL vs ELT Patterns

### 1. The three steps

- **Extract:** pull data from sources such as OLTP databases, APIs, files, and event streams.
- **Transform:** clean, deduplicate, cast types, join, apply business rules, and model the data into facts and dimensions.
- **Load:** write the data into the target system.

The difference between the two patterns is **where and when the transform happens**.

### 2. ETL vs ELT

```
ETL:  Source ──Extract──▶ Transform engine (Spark / Python / SSIS) ──Load──▶ Warehouse (clean data only)
ELT:  Source ──Extract──▶ Load raw into warehouse/lake ──▶ Transform inside it with SQL (e.g., dbt)
```

| | **ETL** | **ELT** |
|---|---|---|
| Transform happens | **Before** loading, on a separate server or engine | **After** loading, **inside** the warehouse |
| What lands in the target | Only cleaned, modeled data | Raw data first, then transformed layers |
| Main reason it exists | On-premises warehouses were expensive and had little compute, so data was shrunk before loading | Cloud warehouses have cheap storage and elastic MPP compute |
| Reprocessing | Hard, because the raw data is gone. Re-extract from the source. | Easy: the raw data is still there, so re-run the SQL |
| Flexibility | The schema is decided up front | New questions can be answered from the raw data later |
| Typical tools | Informatica, SSIS, Talend, custom Python/Spark | Load: Fivetran, Airbyte, Snowpipe, `COPY INTO`. Transform: dbt, SQL. |
| Watch out for | Pipeline bottlenecks and brittle logic | Warehouse compute cost; raw PII sitting in the warehouse (mask or restrict it) |
| Good fit | Heavy non-SQL processing; data that must be masked **before** it lands (compliance) | Most modern cloud analytics stacks |

### 3. Medallion architecture (Bronze → Silver → Gold)

The medallion architecture is how ELT data is usually organized as it's refined in layers:

```
OLTP / APIs / files ─▶ BRONZE (raw) ─▶ SILVER (cleaned) ─▶ GOLD (star schema) ─▶ BI dashboards, ML
```

| Layer | Also called | What happens | Example |
|---|---|---|---|
| **Bronze** | Raw / ingestion layer | An **exact copy** of the source with **no transformation**. Only metadata columns are added: `_loaded_at`, `_source_system`, `_source_file`, `_run_id` (batch id). Kept append-only so any layer can be rebuilt from it. | `bronze.orders_raw`: JSON or text columns exactly as extracted |
| **Silver** | Cleaned / conformed layer | **Deduplicate**, **parse data types** (strings to dates and decimals), **handle nulls**, **lightly flatten** nested data, standardize codes and names, apply **basic business logic**, and conform keys across sources | `silver.orders`: one row per order, correct types |
| **Gold** | Presentation layer | **Fact and dimension tables** (star schema) and business-level aggregates, ready for **BI and ML tools** | `gold.fact_sales`, `gold.dim_customer` |

Why use layers? Each layer has a clear purpose. Bugs can be traced back to a specific layer and fixed. You can rebuild Silver and Gold from Bronze at any time. And each audience gets the right level of access: engineers work in Bronze, while analysts only query Gold.

---

## Part C — Dimensional Modeling: Star vs Snowflake Schema

### 1. Why dimensional modeling?

A normalized OLTP schema is built for writing data safely. An analyst asking "revenue by product category by month by city" would need 6–8 joins against it. **Dimensional modeling** (Ralph Kimball's approach) reshapes the data into:

- **Facts:** the measurements of a business event (how much, how many).
- **Dimensions:** the context of the event (who, what, where, when). These are what you filter and group by.

### 2. The four-step design process

1. **Choose the business process** (e.g., sales / order placement).
2. **Declare the grain**: exactly what one fact row represents (e.g., *one row per order line item*). Always do this first.
3. **Identify the dimensions** (date, customer, product, location).
4. **Identify the facts** (quantity, unit price, gross amount, discount).

### 3. Star schema

```
                     dim_date
                        │
   dim_customer ── fact_sales ── dim_product
                        │
                   dim_location
```

```sql
CREATE TABLE gold.fact_sales (          -- grain: one row per order line
    date_key        INT,                -- FK → dim_date (e.g., 20260924)
    customer_key    INT,                -- FK → dim_customer (surrogate key)
    product_key     INT,                -- FK → dim_product
    location_key    INT,                -- FK → dim_location
    order_id        INT,                -- degenerate dimension
    quantity        INT,
    unit_price      NUMBER(10,2),
    gross_amount    NUMBER(12,2),
    discount_amount NUMBER(12,2)
);

CREATE TABLE gold.dim_product (
    product_key   INT,                  -- surrogate key
    product_id    INT,                  -- natural/business key from OLTP
    sku           VARCHAR,
    product_name  VARCHAR,
    category      VARCHAR,              -- denormalized: stored right here
    department    VARCHAR
);
```

A typical query needs only **one join per dimension**:

```sql
SELECT d.year_month, p.category, SUM(f.gross_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_date d    ON f.date_key = d.date_key
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY 1, 2;
```

### 4. Star schema design rules

These seven rules are condensed from Kimball's essential rules of dimensional modeling:

1. **Model around a business process and declare the grain before anything else.**
2. **Store data at the most atomic grain** (e.g., order line, not daily totals). You can always roll up, but you can never drill down below what you stored.
3. **Every fact in a fact table has the same grain.** Don't mix order-line rows with order-level shipping totals. Put those in a separate fact table, or allocate them down to the line level.
4. **Every fact table has a date dimension**, joined through a date key rather than a raw timestamp.
5. **Dimensions use surrogate keys** (integers generated by the warehouse), not the source system's natural keys.
6. **Descriptive attributes and labels go in denormalized dimensions.** The fact table holds only foreign keys, numeric measures, and degenerate dimensions. No text descriptions in facts.
7. **Use conformed dimensions** (the same `dim_date`, `dim_customer`, `dim_product`) across all fact tables so results can be compared and combined across business processes.

### 5. Snowflake schema

In a snowflake schema, the dimensions are **normalized** into sub-dimensions:

```
dim_department ── dim_category ── dim_product ── fact_sales ── dim_customer ── dim_city ── dim_country
```

### 6. Star vs snowflake

| | **Star** | **Snowflake** |
|---|---|---|
| Dimensions | Denormalized (one flat table each) | Normalized into several related tables |
| Joins per query | Fewer, simpler | More |
| Query performance | Usually faster | Usually slower (more joins) |
| Storage | Some repeated values (e.g., category name on every product row) | Less redundancy |
| Maintenance of dimension attributes | Update the repeated values in many rows | Update in one place |
| Ease for BI users | Easy to understand | Harder to navigate |
| When to use it | **Default choice** for gold/presentation layers | Very large dimensions with deep hierarchies, or a hierarchy shared by several dimensions |

In columnar warehouses, the repeated values in star-schema dimensions compress very well (dictionary encoding and RLE), so the storage savings of snowflaking rarely matter. **Prefer star.**

---

## Part D — Fact & Dimension Tables

### 1. Fact tables

A fact table records **measurements of business events**. It's usually **tall and narrow**: billions of rows, holding foreign keys and numbers.

**Types of facts (measures):**

| Type | Can you SUM it across… | Example |
|---|---|---|
| **Additive** | all dimensions | `quantity`, `gross_amount` |
| **Semi-additive** | some dimensions, but **not time** | Account balance, inventory level. Summing across stores is fine; summing across days is wrong. Use the average or the last value over time instead. |
| **Non-additive** | none | Ratios, percentages, `unit_price`. Store the numerator and denominator, and compute the ratio in the query. |

**Types of fact tables:**

| Type | One row per… | Example |
|---|---|---|
| **Transaction** | Event, at the atomic grain | `fact_sales` (one per order line) |
| **Periodic snapshot** | Entity per period | `fact_inventory_daily` (stock per product per day) |
| **Accumulating snapshot** | Process instance, **updated** as it moves through milestones | `fact_order_fulfillment` with `ordered_date`, `paid_date`, `shipped_date`, `delivered_date` |
| **Factless** | Event with no numeric measure | Student attendance, promotion coverage. You just `COUNT(*)`. |

### 2. Dimension tables

A dimension table holds **descriptive context**. It's usually **short and wide**: thousands to millions of rows, many text columns.

| Concept | Meaning |
|---|---|
| **Surrogate key** | A warehouse-generated integer PK (`customer_key`). It separates the warehouse from source-system key changes, makes SCD Type 2 possible, and joins faster. |
| **Natural / business key** | The ID from the source system (`customer_id`). Kept as an attribute. |
| **Conformed dimension** | One dimension shared by several fact tables with the same keys and meaning. |
| **Role-playing dimension** | One dimension used several times in different roles, e.g., `dim_date` as `order_date_key` and as `ship_date_key`. |
| **Degenerate dimension** | A dimension key stored in the fact table with no dimension table of its own, e.g., `order_id` or an invoice number. |
| **Junk dimension** | Several low-cardinality flags (`is_gift`, `payment_type`, `channel`) combined into one small dimension, so the fact table doesn't need many tiny FKs. |
| **Unknown member** | A special row (e.g., key `-1`, "Unknown") that facts point to when a dimension value is missing. This avoids NULL foreign keys and keeps inner joins from dropping rows. |
| **Date dimension** | Built ahead of time, one row per day, with attributes like `year`, `quarter`, `month_name`, `day_of_week`, `is_weekend`, `is_holiday`. |

---

## Part E — Slowly Changing Dimensions (SCDs)

### 1. The problem

Dimension attributes change over time. A customer moves from **Hà Nội to HCM** on 2026-06-01. Should sales from *before* the move count toward Hà Nội or HCM? Each SCD type gives a different answer.

### 2. SCD types

Starting row: `customer_id = 7, name = An, city = HN`

| Type | Strategy | After the move | History? |
|---|---|---|---|
| **Type 0** | **Retain original**: never update | `city = HN` forever | Only the original value (used for fixed facts like `date_of_birth` or `original_signup_channel`) |
| **Type 1** | **Overwrite** the old value | `city = HCM` | ❌ **None.** All past sales now look like HCM sales. Fine for corrections (typos). |
| **Type 2** | **Add a new row** with a new surrogate key, plus `valid_from`, `valid_to`, and an `is_current` flag | Two rows (below) | ✅ **Full history.** Every row is correct for its time range. |
| **Type 3** | **Add a `previous_value` column** next to the current one | `city = HCM, previous_city = HN` | ⚠️ **Partial**: only one previous value |

SCD Type 2 rows:

| customer_key | customer_id | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 101 | 7 | HN | 2024-03-10 | 2026-06-01 | FALSE |
| 245 | 7 | HCM | 2026-06-01 | 9999-12-31 | TRUE |

Two less common types:

- **Type 4:** keep current values in the dimension and move the change history to a separate history table.
- **Type 6:** a hybrid of 1 + 2 + 3. It adds Type 2 rows, and each row also carries a `current_city` column that is overwritten.

### 3. How SCD Type 2 keeps facts correct

- When a fact row is loaded, it gets the **surrogate key of the dimension version that was valid at the event time**. Sales from before June point to key `101` (HN), and later sales point to `245` (HCM). Revenue by city is correct historically without any extra logic in the query.
- To look up the correct version when loading facts:

```sql
JOIN gold.dim_customer c
  ON c.customer_id = o.user_id
 AND o.order_date >= c.valid_from
 AND o.order_date <  c.valid_to          -- half-open interval: no overlaps, no gaps
```

- To report by **current** attributes, filter `WHERE is_current`, or join on the natural key to the current row.
- **Conventions:**
  - Use a far-future `valid_to` (`9999-12-31`) instead of NULL, so range filters are simple.
  - Allow exactly **one `is_current = TRUE` row per business key**.
  - Compare a **hash of the tracked columns** (`row_hash`) to detect changes cheaply.

### 4. SCD Type 2 with `MERGE` (Snowflake)

A single `MERGE` has to do two things for a changed customer: **expire** the old row (UPDATE) and **insert** the new version (INSERT). One source row can only match once, so the standard trick is to send changed rows through **twice**:

1. once with the real key, so they match and expire the current row;
2. once with a `NULL` merge key, so they never match and get inserted as the new version.

```sql
-- silver.customers: customer_id, full_name, city, segment, updated_at,
--                   row_hash = HASH(full_name, city, segment)
-- gold.dim_customer: customer_key (IDENTITY), customer_id, full_name, city, segment,
--                    row_hash, valid_from, valid_to, is_current

MERGE INTO gold.dim_customer AS tgt
USING (
    -- (1) every incoming row, keyed normally → matches the current version (if any)
    SELECT s.customer_id AS merge_key,
           s.customer_id, s.full_name, s.city, s.segment, s.row_hash, s.updated_at
    FROM silver.customers s

    UNION ALL

    -- (2) changed rows only, with a NULL key → can never match → INSERT the new version
    SELECT NULL AS merge_key,
           s.customer_id, s.full_name, s.city, s.segment, s.row_hash, s.updated_at
    FROM silver.customers s
    JOIN gold.dim_customer d
      ON d.customer_id = s.customer_id
     AND d.is_current
    WHERE d.row_hash <> s.row_hash
) AS src
ON  tgt.customer_id = src.merge_key
AND tgt.is_current

-- changed → close out the old version
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN UPDATE SET
    tgt.valid_to   = src.updated_at,
    tgt.is_current = FALSE

-- brand-new customer, or the new version of a changed one
WHEN NOT MATCHED THEN INSERT
    (customer_id, full_name, city, segment, row_hash, valid_from, valid_to, is_current)
VALUES
    (src.customer_id, src.full_name, src.city, src.segment, src.row_hash,
     src.updated_at, '9999-12-31', TRUE);
```

| Incoming customer | Path (1) | Path (2) | Result |
|---|---|---|---|
| **New** | No match → INSERT | Not produced (no current row) | 1 new current row |
| **Changed** | Match + hash differs → UPDATE (expire) | NULL key → INSERT | Old row closed, new current row |
| **Unchanged** | Match, but hash is equal → nothing | Not produced | No change |

**Alternative (two steps in one transaction):**

1. `UPDATE` to expire the changed current rows.
2. `INSERT` new and changed rows that no longer have a current version.

It's easier to read, but it must be wrapped in a transaction so the two steps apply together.

**Edge cases to consider:**

- **Deletes in the source:** close the row out, or add an `is_deleted` flag.
- **Duplicate updates for the same key in one batch:** deduplicate in Silver first, keeping the latest per key with `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1`.
- **Late-arriving changes.**

---

## Part F — Data Lake, Data Warehouse, and Data Lakehouse

### 1. The three architectures

| | **Data warehouse** | **Data lake** | **Data lakehouse** |
|---|---|---|---|
| Stores | Structured, modeled data | **Any** data: structured, semi-structured (JSON), unstructured (images, logs) | Any data, with warehouse-style tables on top |
| Storage | Proprietary managed columnar storage (often on object storage underneath) | Cheap object storage (S3 / ADLS / GCS) in open file formats (Parquet, ORC, CSV, JSON) | Object storage with **open table formats**: Delta Lake, Apache Iceberg, Apache Hudi |
| Schema | **Schema-on-write**: data must fit the schema when it's loaded | **Schema-on-read**: structure is applied when the data is queried | Schema enforced on write, and the schema can evolve |
| ACID transactions | ✅ | ❌ (files only: concurrent writes can corrupt data, and there's no reliable update/delete) | ✅ (added by the table format's transaction log) |
| Performance for BI | Excellent | Poor to moderate | Good to excellent |
| ML / data science | Limited (data has to be exported) | Excellent (direct file access) | Excellent |
| Cost | Higher | Lowest | Low storage cost, pay for compute |
| Main risk | Cost; rigid for new data types | Turns into a **"data swamp"** (no quality control, no catalog, no governance) | More components to run and manage |
| Examples | Snowflake, BigQuery, Redshift | Raw S3 buckets + Spark / Athena | Databricks (Delta), Iceberg tables queried by Spark / Trino / Snowflake |

### 2. How the architectures evolved

```
1990s–2000s        2010s                                   2020s
Warehouse    →     Lake + Warehouse (two-tier)       →     Lakehouse
(structured        raw data in the lake, a copy            one copy of data on object storage,
 only, costly)     ETL'd into the warehouse →              table formats add ACID, schema,
                   duplicated data, stale copies,          time travel → BI and ML on the same data
                   two systems to govern
```

A **lakehouse** combines the **low cost and flexibility of a lake** with the **reliability and performance of a warehouse**. Its open table formats add, on top of files:

- ACID transactions
- `UPDATE` / `DELETE` / `MERGE`
- schema enforcement and evolution
- time travel
- file statistics used for pruning

The line between the categories is blurring. Warehouses like Snowflake can query Iceberg tables, and lakehouse platforms offer SQL warehouses. The **medallion architecture** (Part B) is used in both.

---

## Key Terms Cheat Sheet

- **OLTP / OLAP:** many small transactional reads and writes / few large analytical queries
- **Data warehouse:** a centralized, columnar, MPP analytical database
- **Row vs columnar storage:** whole rows stored together / each column stored together (reads only the needed columns)
- **Dictionary encoding:** replaces repeated values with small integer codes
- **Run-length encoding (RLE):** stores runs of repeated values as (value, count); works best on sorted data
- **MPP:** partition the data, process each slice in parallel, merge the results
- **Decoupled compute & storage:** cheap object storage plus independently scaled, pay-per-use compute
- **Virtual warehouse:** a Snowflake compute cluster (XS–6XL), per-second billing, one per team
- **Micro-partition:** Snowflake's immutable, compressed, columnar storage unit; pruned using min/max metadata
- **Result cache:** repeated identical queries on unchanged data return instantly (up to 24 hours)
- **ETL / ELT:** transform before loading / load raw data, then transform inside the warehouse
- **Medallion (Bronze / Silver / Gold):** raw → cleaned/conformed → star schema for BI and ML
- **Dimensional modeling:** organizes data into facts (measures) and dimensions (context)
- **Grain:** exactly what one fact row represents
- **Star / snowflake schema:** denormalized dimensions / normalized dimensions
- **Fact table types:** transaction, periodic snapshot, accumulating snapshot, factless
- **Additive / semi-additive / non-additive:** can be summed across all dimensions / across all but time / across none
- **Surrogate key / natural key:** warehouse-generated integer / the source system's ID
- **Conformed, role-playing, degenerate, junk dimension:** shared across facts / reused in several roles / a key stored in the fact table / a bundle of small flags
- **SCD 0 / 1 / 2 / 3:** never change / overwrite / new row with a validity range / previous-value column
- **`row_hash`:** a hash of the tracked columns, used to detect changes
- **Data lake:** cheap object storage for any data format, schema-on-read
- **Data swamp:** a lake without governance or quality control
- **Lakehouse:** a lake plus an open table format (Delta / Iceberg / Hudi) that adds ACID, schema, and time travel
- **Schema-on-write vs schema-on-read:** validate the structure when loading / apply it when querying

---

## Practice Questions

1. **Give three differences between OLTP and OLAP workloads.**
   OLTP runs many small key-based reads and writes on a normalized schema with millisecond latency. OLAP runs few large scans with aggregates and joins on a denormalized schema, and each query takes seconds to minutes.

2. **An analyst's report on the production database at 9 a.m. causes checkout failures. Explain why, and propose a fix.**
   The heavy query competes with app transactions for CPU, I/O, and memory, and holds locks or snapshots for a long time. App transactions wait, time out, and fail. Fix: move analytics to a data warehouse loaded by an ETL/ELT pipeline, or at least use a read replica.

3. **Why is columnar storage faster for `SELECT city, SUM(amount) FROM orders GROUP BY city`?**
   It reads only the `city` and `amount` columns, not every column of every row. Those columns also compress well, so even less data comes off disk.

4. **Why is columnar storage a bad fit for an app that inserts one order at a time?**
   A single-row insert has to write to every column's storage separately. Row storage writes the whole record in one place.

5. **Encode `abc, xyz, abc, abc, xyz, xyz` with dictionary encoding.**
   Dictionary `1 → abc, 2 → xyz`. Stored as `1, 2, 1, 1, 2, 2`.

6. **Encode `HN, HN, HCM, HCM, HCM, DN` with RLE. What makes RLE more effective?**
   `(HN, 2), (HCM, 3), (DN, 1)`. Sorting or clustering the data so identical values sit next to each other.

7. **Explain how MPP runs a `GROUP BY` aggregate.**
   The data is partitioned across nodes. Each node aggregates its own partition in parallel, and the coordinator merges the partial aggregates into the final result.

8. **What are the benefits of decoupling compute and storage?**
   Storage stays cheap. Compute scales and is paid for independently (per second, turned off when idle). Several teams can use separate warehouses on the same data without competing for resources.

9. **Name Snowflake's three layers and one responsibility of each.**
   Storage: micro-partitions in object storage. Compute: virtual warehouses that run queries. Cloud services: optimization, metadata, security, and the result cache.

10. **Why doesn't Snowflake need you to create indexes?**
    It stores min/max metadata for every column in every micro-partition and prunes the partitions a query doesn't need. Columnar scans plus pruning replace indexes.

11. **ETL vs ELT: where does the transformation happen, and why did ELT become popular?**
    ETL transforms on a separate engine before loading. ELT loads raw data and transforms it inside the warehouse. Cloud warehouses made storage cheap and compute elastic, and keeping raw data makes it easy to reprocess.

12. **When would you still choose ETL?**
    When sensitive data must be masked or removed before it reaches the warehouse (compliance), or when the transformation needs heavy non-SQL processing.

13. **Describe Bronze, Silver, and Gold. Where does deduplication happen? Where do fact tables live?**
    Bronze is an untransformed raw copy with load metadata. Silver is cleaned and conformed: deduplication, type parsing, null handling, light flattening. Gold holds the star schema for BI and ML. Deduplication happens in Silver, and facts and dimensions live in Gold.

14. **Why keep Bronze data unchanged?**
    So Silver and Gold can be rebuilt at any time after a bug fix or a logic change, and so there's an audit trail of exactly what the source sent.

15. **What are the four steps of dimensional design? Which comes first after choosing the process?**
    Choose the business process, declare the grain, identify the dimensions, identify the facts. Declaring the grain comes right after choosing the process.

16. **Declare the grain of `fact_sales` for the e-commerce schema, and list its dimensions and measures.**
    One row per order line item. Dimensions: date, customer, product, location. Degenerate dimension: `order_id`. Measures: quantity, unit_price, gross_amount, discount_amount.

17. **Star vs snowflake schema: which is the usual default for BI, and why?**
    Star. It needs fewer joins, it's faster, and it's easier for analysts to understand. In a columnar warehouse, the extra storage from denormalizing is small because repeated values compress well.

18. **Classify each measure: `gross_amount`, `account_balance`, `conversion_rate`.**
    Additive, semi-additive (don't sum across time), non-additive (store the numerator and denominator instead).

19. **Which fact table type fits order fulfillment tracked through ordered → paid → shipped → delivered?**
    An accumulating snapshot: one row per order, updated as each milestone date is filled in.

20. **Why use surrogate keys instead of natural keys in dimensions?**
    They separate the warehouse from source-system key changes and collisions across sources, they make SCD Type 2 possible (one business key can have several versions), and integer joins are fast.

21. **What are role-playing and degenerate dimensions? Give an example of each.**
    Role-playing: one dimension used in several roles, e.g., `dim_date` as order date and as ship date. Degenerate: a dimension key with no dimension table, e.g., `order_id` stored in `fact_sales`.

22. **A customer moves from HN to HCM. Describe the result under SCD Types 0, 1, 2, and 3.**
    Type 0: stays HN. Type 1: overwritten to HCM, no history. Type 2: the old row is closed and a new current HCM row is added with its own surrogate key. Type 3: `city = HCM`, `previous_city = HN`.

23. **With SCD Type 2, how do past sales keep reporting under the old city?**
    Each fact row stores the surrogate key of the dimension version that was valid when the sale happened. Sales before the move point to the HN row.

24. **Why does a single SCD Type 2 `MERGE` send changed rows through twice?**
    A changed customer needs both an UPDATE (expire the old row) and an INSERT (the new version), but one source row can only match once. The copy with a NULL key never matches, so it goes to the INSERT branch.

25. **Data lake vs data warehouse vs lakehouse: what does a lakehouse add to a lake?**
    An open table format (Delta, Iceberg, or Hudi) that adds ACID transactions, `UPDATE`/`MERGE`, schema enforcement and evolution, time travel, and statistics for pruning. That gives warehouse-style reliability and performance on cheap lake storage.

26. **What is a data swamp, and how do you prevent one?**
    A lake full of undocumented, poor-quality, ungoverned data that nobody trusts. Prevent it with a data catalog, clear ownership, quality checks, layered zones (medallion), and access control.
