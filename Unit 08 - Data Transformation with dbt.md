# Unit 08 — Data Transformation with dbt: Study Guide

**Scope:** Intro to dbt and the modern data stack · Advanced modeling with Jinja and macros · Production workflow and optimization

The examples reuse the e-commerce data from earlier units (`orders`, `order_item`, `products`, `customers`), loaded into **Snowflake** as in the lectures. Units 5–7 built the Bronze → Silver → Gold layers by hand, with SQL scripts, Pandas and PySpark. dbt handles the same Bronze → Silver → Gold transformations in the warehouse (staging models read the Bronze sources), as tested, version-controlled SQL. **Naming note:** dbt models follow dbt's conventions (`fct_orders`, `dim_customers`, `ordered_at`), so they differ from the hand-built star schema in Unit 5 (`fact_sales`, `dim_customer`, `order_date`).

---

## Part A — Why dbt, and Where It Fits

### 1. The modern data stack and ELT

| Layer | Job | Example tools |
|---|---|---|
| **Extract + Load (EL)** | Copy raw data from apps and databases into the warehouse, unchanged | Fivetran, Airbyte, custom Python (Unit 6), Snowpipe |
| **Storage + compute** | Cloud warehouse or lakehouse that stores the data and runs the SQL | **Snowflake**, BigQuery, Databricks, Redshift, Fabric |
| **Transform (T)** | Turn raw tables into clean, modeled tables | **dbt** |
| **Orchestration** | Run the steps on a schedule, in the right order | Airflow, Dagster, dbt Cloud jobs |
| **BI / consumption** | Dashboards and analysis | Power BI, Tableau, Looker |

**ETL vs ELT:** In ETL, data is transformed *before* it's loaded, on a separate server. In **ELT**, the raw data is loaded first and then transformed *inside the warehouse*, where cloud compute is cheap and scales. **dbt is the T in ELT.**

### 2. Why not just raw SQL scripts?

| Flaw of raw SQL scripts | How dbt fixes it |
|---|---|
| You write the `CREATE TABLE` / `CREATE VIEW` / `INSERT` scripts by hand | You write **only a `SELECT`**. dbt generates the DDL/DML from the **materialization** |
| You manage **run order** by hand (script 3 must run after scripts 1 and 2) | `ref()` builds a **dependency graph (DAG)**, and dbt works out the run order |
| Renaming a table or column means editing many SQL files | Table names come from `ref()`/`source()`, so you **change a name in one place** |
| SCD Type 2 needs a long, error-prone `MERGE` script | **Snapshots** handle SCD Type 2 in a few lines of config |
| Logic is scattered and undocumented | **Transformation logic is written as code**: in Git, tested, documented, reviewed in PRs (Unit 1) |

### 3. What dbt is (and isn't)

- **A framework, not a database.** dbt **compiles** your SQL + Jinja into plain SQL and **sends it to the warehouse**. **dbt doesn't process data itself: Snowflake does.** No data passes through the machine running dbt.
- **SQL-first.** Models are `SELECT` statements. (Python models exist on Snowflake/Databricks/BigQuery, but SQL is the main path.)
- **Two ways to run it:** **dbt Core** (free, open-source CLI you install with `pip install dbt-snowflake`) and **dbt Cloud** (hosted IDE, scheduler, CI, docs hosting, now branded the "dbt platform").

### 4. Project structure

```
ecommerce_dbt/
├── dbt_project.yml        ← the heart of the project: name, paths, default configs
├── packages.yml           ← external packages (dbt_utils, ...)
├── models/                ← SQL models (the SELECTs) + .yml property files
│   ├── staging/
│   │   ├── _sources.yml   ← declare raw (Bronze) tables
│   │   ├── _stg_models.yml← tests + docs for staging models
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── intermediate/
│   │   └── int_order_items_enriched.sql
│   └── marts/
│       ├── fct_orders.sql
│       └── dim_customers.sql
├── snapshots/             ← SCD Type 2 definitions
├── seeds/                 ← small static CSVs (country codes, currency map)
├── tests/                 ← singular (custom SQL) tests
├── macros/                ← reusable Jinja functions
├── analyses/              ← ad-hoc SQL that's compiled but never built
└── target/                ← generated: compiled SQL, manifest.json, run_results.json
```

`profiles.yml` holds the **connection credentials**. It lives **outside the project** (`~/.dbt/profiles.yml` by default) so passwords never get committed to Git.

```yaml
# ~/.dbt/profiles.yml
ecommerce_dbt:                       # must match `profile:` in dbt_project.yml
  target: dev                        # default target
  outputs:
    dev:
      type: snowflake
      account: xy12345.ap-southeast-1
      user: HIEN
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"   # read from an environment variable
      role: TRANSFORMER
      warehouse: TRANSFORM_WH
      database: ANALYTICS
      schema: DBT_HIEN               # personal dev schema
      threads: 4                     # how many models run in parallel
    prod:
      type: snowflake
      # ... same account, different schema/role
      schema: PROD
      threads: 8
```

```yaml
# dbt_project.yml
name: ecommerce_dbt
version: "1.0.0"
profile: ecommerce_dbt

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]
test-paths: ["tests"]
```

### 5. `source()` and `ref()`

| Function | Points at | Use it for |
|---|---|---|
| `{{ source('shop', 'orders') }}` | A **raw table dbt didn't build** (loaded by an EL tool) | **Reading from Bronze** |
| `{{ ref('stg_orders') }}` | **Another dbt model** (or seed or snapshot) | **Reading from Silver and above** |

```yaml
# models/staging/_sources.yml
version: 2
sources:
  - name: shop                   # logical name used in source()
    database: RAW
    schema: SHOP                 # where the loader lands data (Bronze)
    tables:
      - name: orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after:  {count: 26, period: hour}    # daily load + buffer (same limit as Unit 13's freshness alert)
          error_after: {count: 48, period: hour}
      - name: customers
```

```sql
-- models/staging/stg_orders.sql   (Bronze → Silver: rename, cast, clean)
select
    order_id,
    user_id                         as customer_id,
    lower(trim(status))             as status,
    amount::number(12,2)            as amount,
    order_date::timestamp_ntz       as ordered_at,
    updated_at::timestamp_ntz       as updated_at,
    country_code,
    _loaded_at
from {{ source('shop', 'orders') }}
where order_id is not null
```

```sql
-- models/marts/fct_orders.sql   (Silver → Gold)
select
    o.order_id,
    o.customer_id,
    c.city,
    o.status,
    o.ordered_at::date as order_day,
    o.amount,
    o.updated_at
from {{ ref('stg_orders') }}   o
left join {{ ref('dim_customers') }} c using (customer_id)
```

The rule: **never hard-code a table name.** If you write `from ANALYTICS.DBT_HIEN.STG_ORDERS`, dbt doesn't see the dependency, the DAG breaks, and the name is wrong in prod.

### 6. The lineage graph (DAG)

**dbt builds the dependency graph from `ref()` and `source()`.** Each `ref('x')` inside model `y` means "x must be built before y".

```
source: shop.orders ──► stg_orders ──┐
                                     ├──► fct_orders ──► (dashboard)
source: shop.customers ─► stg_customers ─► dim_customers ──┘
seed: country_codes ─────────────────────────┘
```

- dbt runs models in **dependency order** and runs independent models **in parallel** (up to `threads`).
- If a model fails, everything **downstream of it is skipped**. Unrelated branches keep running.
- `dbt docs generate` + `dbt docs serve` shows this graph interactively.

### 7. Recommended layers (and how they map to Medallion)

| dbt layer | Medallion | Prefix | Typical work | Default materialization |
|---|---|---|---|---|
| **Sources** | Bronze | — | Raw data as loaded (declared, not built) | — |
| **Staging** | Silver | `stg_` | **1:1 with a source table**: rename, cast types, light cleaning. No joins. | **view** |
| **Intermediate** | Silver | `int_` | Joins and reusable logic between staging and marts | **ephemeral** or view |
| **Marts** | Gold | `fct_`, `dim_` | Business-facing **facts and dimensions** (Unit 5 star schema) | **table** or **incremental** |

### 8. Core commands

| Command | What it does |
|---|---|
| `dbt init` | Scaffold a new project |
| `dbt debug` | **Verify the connection to Snowflake** and check that `profiles.yml` / `dbt_project.yml` are valid |
| `dbt deps` | Install packages from `packages.yml` |
| `dbt compile` | **Render Jinja into plain SQL** (in `target/compiled/`) **without running it** |
| `dbt run` | Build models (runs the compiled SQL in the warehouse) |
| `dbt test` | Run data tests |
| `dbt seed` | Load CSVs in `seeds/` as tables |
| `dbt snapshot` | Run snapshots (SCD Type 2) |
| `dbt build` | **seed + run + snapshot + test, in DAG order.** A model's tests run right after it, and if they fail, downstream models are skipped. |
| `dbt source freshness` | Check whether sources were loaded recently enough |
| `dbt docs generate` / `dbt docs serve` | Build / view the documentation site and lineage graph |
| `dbt show --select model` | Preview a model's results without building it |
| `dbt retry` | Re-run only what failed in the last run |

**Node selection** (works with `run`, `test`, `build`, ...):

```bash
dbt run  --select stg_orders            # one model
dbt run  --select +fct_orders           # fct_orders and everything UPSTREAM of it
dbt run  --select stg_orders+           # stg_orders and everything DOWNSTREAM
dbt run  --select +fct_orders+          # both directions
dbt build --select staging              # a folder (models/staging)
dbt build --select tag:daily            # models tagged "daily"
dbt run  --exclude dim_customers
dbt run  --target prod                  # use the prod output in profiles.yml
dbt run  --full-refresh                 # rebuild incremental models from scratch
```

---

## Part B — Materializations

A **materialization** tells dbt **what to create in Snowflake** from your `SELECT`.

### 1. The four materializations

| Materialization | What dbt runs | Pros | Cons | Use for |
|---|---|---|---|---|
| **view** (default) | `CREATE VIEW AS SELECT ...` | Nothing stored, always up to date, instant to build | Query runs **every time someone reads it**, so it's slow for heavy logic | Staging, light logic |
| **table** | **Drop + create + insert all rows, every run** (`CREATE OR REPLACE TABLE AS ...`) | Fast to query | Rebuilds everything each run: slow and costly on big data | Marts that are small/medium, or need full rebuilds |
| **incremental** | First run: create the table. Later runs: **insert/merge only new or changed rows** | Much faster and cheaper on big, growing tables | More complex, logic can miss rows (late data, Part B.4) | Large facts, event logs |
| **ephemeral** | **Nothing created.** The SQL is injected into downstream models as a **CTE** | Keeps the warehouse clean | Can't query it directly, hard to debug, repeated if used many times | Small reusable helper logic (intermediate) |

(dbt also supports **materialized_view** on warehouses that have them, e.g. Snowflake dynamic tables. Not needed for this unit.)

### 2. Configuring materializations by layer

Set **defaults per folder** in `dbt_project.yml`, then **override in a model** when needed. The `+` prefix marks a config (not a sub-folder name).

```yaml
# dbt_project.yml
models:
  ecommerce_dbt:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +schema: marts
```

```sql
-- models/marts/fct_orders.sql  → override just this model
{{ config(materialized='incremental', unique_key='order_id') }}
select ...
```

**Config precedence** (most specific wins): `config()` in the model file → the model's `.yml` properties → `dbt_project.yml` folder configs.

### 3. Table vs incremental, in depth

| | **table** | **incremental** |
|---|---|---|
| Each run | Drop → create → insert **all** records | **Merge/insert only new (or changed) records** into the existing table |
| Cost | Grows with **total** data size | Grows with **new** data size |
| Correctness | Always matches the source | Only as good as your "what's new?" filter |
| Schema change | Picked up automatically | Needs `on_schema_change` or `--full-refresh` |

**Anatomy of an incremental model:**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',              -- rows with the same key get UPDATED, not duplicated
    incremental_strategy='merge',       -- Snowflake default
    on_schema_change='append_new_columns'
) }}

select o.order_id, o.customer_id, c.city, o.status, o.ordered_at::date as order_day, o.amount, o.updated_at
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customers') }} c using (customer_id)

{% if is_incremental() %}
  -- only on incremental runs: take rows newer than what's already in the target
  where o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

- **`is_incremental()`** is true only when the table already exists, the model is incremental, and you didn't pass `--full-refresh`. On the first run the filter is skipped, so the full history loads.
- **`{{ this }}`** = the model's own existing table in the warehouse.
- **`unique_key`**: without it, the merge is an insert, and a changed order would appear twice.

**Incremental strategies:**

| Strategy | Behavior | Notes |
|---|---|---|
| **append** | Insert new rows, never update | Fastest. Only for immutable events (clicks, logs). Duplicates if data is re-sent. |
| **merge** | `MERGE` on `unique_key`: update matches, insert new | Snowflake default. Most common. |
| **delete+insert** | Delete target rows matching the new batch's keys, then insert | Good for large batches where `MERGE` is slow |
| **insert_overwrite** | Replace whole partitions | BigQuery / Spark / Databricks |
| **microbatch** | dbt splits the run into time batches (e.g. one per day) using `event_time`, each batch runs and can be retried on its own | dbt 1.9+. Has a built-in `lookback` config. |

`on_schema_change` options: `ignore` (default), `append_new_columns`, `sync_all_columns`, `fail`.

### 4. Late-arriving data and the look-back window

**Late-arriving data** is a record that shows up in the warehouse **after** records with a later timestamp have already been processed. Examples: a mobile app syncs orders after being offline for a day, a source system re-sends yesterday's file, a payment status updates hours later.

**Why the naive filter misses it:**

```
Run at 02:00: max(updated_at) in target = 2026-09-24 23:59   ← watermark
Order 501 (updated_at 2026-09-24 22:10) arrives in RAW at 03:00
Run at 04:00: where updated_at > '2026-09-24 23:59'  → order 501 is SKIPPED FOREVER
```

**Fix: a look-back window.** Re-process a small, recent slice every run instead of starting exactly at the watermark. With `merge` + `unique_key`, rows you've seen before get updated, not duplicated.

```sql
{% if is_incremental() %}
  where o.updated_at > (select dateadd(day, -3, max(updated_at)) from {{ this }})   -- 3-day look-back
{% endif %}
```

Make the window a variable so it's easy to change:

```sql
where o.updated_at > (select dateadd(day, -{{ var('lookback_days', 3) }}, max(updated_at)) from {{ this }})
```

```bash
dbt run --select fct_orders --vars '{lookback_days: 10}'   # one-off wider catch-up
```

**Choosing the window:** make it wider than the latest arrival you normally see (check how late data really arrives). Too small: rows still get missed. Too large: every run re-scans lots of data and costs more. Run a **periodic `--full-refresh`** (e.g. weekly) as a safety net. With the **microbatch** strategy, dbt does this for you with `lookback=N` (re-processes the previous N batches, default 1).

**Better watermark column:** filter on **when the row was loaded** (`_loaded_at` from Bronze) instead of the business timestamp. A late record has an old `updated_at` but a new `_loaded_at`, so a load-time filter catches it.

### 5. When timestamps can't be trusted: change detection options

The naive filter assumes `updated_at` exists and is always updated. In reality:

- some tables **don't have `updated_at`** at all
- some source systems **don't update `updated_at`** when a row changes (a bulk fix, a trigger that's missing)
- clocks differ between systems

| Approach | How it detects changes | Reliability | Cost |
|---|---|---|---|
| **Timestamp** (`updated_at`) | Rows with a newer timestamp | Only as good as the source | Cheapest |
| **Hash key (hash diff)** | Hash all business columns into one value, and **compare the hash** with the stored one. A different hash = the row changed. | **Most reliable**: catches every change, even with no timestamp | Must hash and compare each row |
| **Append-only** | Doesn't detect changes: **appends every row it receives** | Nothing is lost, but it duplicates. Dedupe downstream (latest row per key). | Cheap to write |

**Hash key example:**

```sql
-- stg_customers.sql: add a hash of the columns that matter
select
    customer_id,
    full_name, email, city,
    {{ dbt_utils.generate_surrogate_key(['full_name', 'email', 'city']) }} as row_hash
from {{ source('shop', 'customers') }}
```

```sql
-- dim_customers.sql (incremental): only merge rows that are new or whose hash changed
{{ config(materialized='incremental', unique_key='customer_id') }}

select s.*
from {{ ref('stg_customers') }} s
{% if is_incremental() %}
left join {{ this }} t on s.customer_id = t.customer_id
where t.customer_id is null          -- new customer
   or t.row_hash <> s.row_hash       -- changed customer
{% endif %}
```

`generate_surrogate_key` (from `dbt_utils`) casts each column to text, replaces nulls with a placeholder, joins them, and hashes with MD5. You'd write the same thing by hand as `md5(coalesce(full_name,'') || '|' || ...)`.

---

## Part C — Snapshots (SCD Type 2)

In Unit 5 you wrote SCD Type 2 by hand: close the old row, insert the new one, track `valid_from` / `valid_to`. **Snapshots do it for you.**

### 1. Defining a snapshot

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True        -- close the row if it's deleted in the source
) }}

select * from {{ source('shop', 'customers') }}

{% endsnapshot %}
```

(From dbt 1.9 you can also define snapshots in YAML, and `invalidate_hard_deletes` became `hard_deletes: invalidate`. The block syntax above still works.)

Run with `dbt snapshot` (or `dbt build`). Reference it downstream with `{{ ref('customers_snapshot') }}`.

### 2. Strategies

| Strategy | Detects a change when | Use when |
|---|---|---|
| **timestamp** | `updated_at` is newer than the stored one | The source has a **reliable** `updated_at` |
| **check** | Any of `check_cols` differs (`check_cols=['email','city']` or `'all'`) | No reliable timestamp. Pairs well with a **hash column**: `check_cols=['row_hash']` |

### 3. What the snapshot table looks like

dbt adds metadata columns:

| customer_id | city | dbt_valid_from | dbt_valid_to | dbt_scd_id | dbt_updated_at |
|---|---|---|---|---|---|
| 7 | Hanoi | 2026-01-01 | 2026-06-15 | a1f… | 2026-01-01 |
| 7 | Saigon | 2026-06-15 | **null** | 9c2… | 2026-06-15 |

- **Current row** = `dbt_valid_to is null`. (Unit 5's hand-built version uses a far-future `valid_to` instead. From dbt 1.9 you can get the same with the snapshot config `dbt_valid_to_current`, e.g. `"to_date('9999-12-31')"`, which also removes the `or ... is null` from the join below.)
- To join facts to the version that was valid at the time: `on f.customer_id = s.customer_id and f.ordered_at >= s.dbt_valid_from and (f.ordered_at < s.dbt_valid_to or s.dbt_valid_to is null)`.

**Snapshot tips:** snapshot **raw sources** (Bronze), not heavily transformed models, because history can't be regenerated later. Run snapshots on a regular schedule, since changes between two runs are lost. Never `--full-refresh` or drop a snapshot table: its history is the only copy.

---

## Part D — Seeds, Tests and Documentation

### 1. Seeds

**Seeds** are small CSV files in `seeds/` that dbt loads as tables with `dbt seed`. They're for **static data that rarely changes** and isn't available from any source system:

- country codes → country names
- currency mapping (`VND`, `USD` → symbol, decimals)
- order status → status group
- small test fixtures for development

```csv
# seeds/country_codes.csv
country_code,country_name,region
VN,Vietnam,APAC
US,United States,AMER
```

```sql
select o.*, cc.country_name
from {{ ref('stg_orders') }} o
left join {{ ref('country_codes') }} cc using (country_code)
```

**Seeds aren't for loading real data.** Large or frequently changing data belongs in the EL tool, not in a CSV in Git.

### 2. Data tests

A dbt test is **a query that returns failing rows. Zero rows = pass.**

**Generic tests** are declared in YAML and reused on any column:

```yaml
# models/marts/_marts.yml
version: 2
models:
  - name: fct_orders
    description: "One row per order."
    columns:
      - name: order_id
        description: "Primary key."
        data_tests:
          - unique
          - not_null
      - name: status
        data_tests:
          - accepted_values:
              values: ['created', 'paid', 'shipped', 'refunded']
      - name: customer_id
        data_tests:
          - relationships:              # foreign key check
              to: ref('dim_customers')
              field: customer_id
              config:
                severity: warn          # warn instead of fail
```

(Older projects use `tests:` instead of `data_tests:`. Both work.)

| Built-in generic test | Checks |
|---|---|
| `unique` | No duplicate values |
| `not_null` | No nulls |
| `accepted_values` | Values are in an allowed list |
| `relationships` | Every value exists in another model (referential integrity) |

More come from packages: `dbt_utils` (`expression_is_true`, `unique_combination_of_columns`, `recency`), `dbt_expectations`.

**Singular tests** are one-off SQL files in `tests/`:

```sql
-- tests/assert_no_negative_amounts.sql
select order_id, amount
from {{ ref('fct_orders') }}
where amount < 0
```

**Unit tests** (dbt 1.8+) check **model logic** against fixed input rows you define in YAML, instead of checking real data. Use them for tricky logic like tier rules or the incremental filter.

Useful test configs: `severity: warn|error`, `error_if` / `warn_if` thresholds (e.g. `">10"`), and `store_failures: true` (saves failing rows to a table for debugging).

### 3. Documentation

- Add `description:` to models and columns in the `.yml` files.
- `dbt docs generate` builds a site from your code + warehouse metadata; `dbt docs serve` opens it with the **lineage graph**.
- **Exposures** declare downstream users (a dashboard) so they appear in the lineage too.

---

## Part E — Advanced Modeling with Jinja and Macros

### 1. Jinja basics

dbt runs every model through **Jinja** (a Python templating language) **before** sending SQL to Snowflake. `dbt compile` shows you the result.

| Syntax | Meaning | Example |
|---|---|---|
| `{{ ... }}` | **Expression**: print a value into the SQL | `{{ ref('stg_orders') }}` |
| `{% ... %}` | **Statement**: logic (if, for, set, macro) that prints nothing | `{% if is_incremental() %}` |
| `{# ... #}` | **Comment**: removed from compiled SQL | `{# TODO: add refunds #}` |
| `{%- ... -%}` | Trim whitespace/new lines around the tag | Cleaner compiled SQL |

**Before and after compile:**

```sql
-- models/marts/fct_orders.sql
select * from {{ ref('stg_orders') }}
```

```sql
-- target/compiled/.../fct_orders.sql
select * from ANALYTICS.DBT_HIEN_staging.stg_orders
```

### 2. Variables, loops and conditions

```sql
-- Pivot revenue by payment method without writing each column by hand
{% set payment_methods = ['card', 'bank_transfer', 'cod', 'wallet'] %}

select
    order_id,
    {% for pm in payment_methods %}
    sum(case when payment_method = '{{ pm }}' then amount else 0 end) as {{ pm }}_amount
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
```

Compiles to:

```sql
select
    order_id,
    sum(case when payment_method = 'card' then amount else 0 end) as card_amount,
    sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
    sum(case when payment_method = 'cod' then amount else 0 end) as cod_amount,
    sum(case when payment_method = 'wallet' then amount else 0 end) as wallet_amount
from ...
group by order_id
```

**Built-in variables and functions you'll use:**

| Item | Meaning |
|---|---|
| `ref()`, `source()` | Model / source references (build the DAG) |
| `this` | The current model's table |
| `is_incremental()` | True on incremental runs |
| `var('name', default)` | Project variable. Set in `dbt_project.yml` (`vars:`) or `--vars` on the CLI. |
| `env_var('NAME')` | Environment variable (secrets, per-environment settings) |
| `target.name`, `target.schema` | The current target (`dev` / `prod`) and its schema |
| `config()` | Model configuration |
| `run_query()` + `execute` | Run a query **at compile time** and use its results in Jinja |

**Dev vs prod logic:**

```sql
select * from {{ ref('stg_orders') }}
{% if target.name == 'dev' %}
where ordered_at >= dateadd(day, -30, current_date)   -- small data in dev = faster + cheaper
{% endif %}
```

**Dynamic values from the warehouse:**

```sql
{% set status_query %} select distinct status from {{ ref('stg_orders') }} {% endset %}
{% if execute %}
  {% set statuses = run_query(status_query).columns[0].values() %}
{% else %}
  {% set statuses = [] %}
{% endif %}
```

`execute` is false during parsing, when dbt only reads the project to build the DAG and doesn't query the warehouse. Guard `run_query` with it.

### 3. Macros

**Macros are reusable Jinja functions**, stored in `macros/`. They're how you follow **DRY** (Don't Repeat Yourself): write the logic once, and change it in one place.

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, scale=2) %}
    round({{ column_name }} / 100.0, {{ scale }})
{% endmacro %}
```

```sql
-- usage in a model
select order_id, {{ cents_to_dollars('amount_cents') }} as amount
from {{ source('shop', 'orders') }}
```

```sql
-- macros/limit_in_dev.sql
{% macro limit_in_dev(date_column, days=30) %}
  {% if target.name == 'dev' %}
    where {{ date_column }} >= dateadd(day, -{{ days }}, current_date)
  {% endif %}
{% endmacro %}
```

**Custom generic test** (a macro that becomes a YAML test):

```sql
-- macros/test_is_positive.sql
{% test is_positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}
```

```yaml
columns:
  - name: amount
    data_tests: [is_positive]
```

**Overriding built-in macros:** dbt's own behavior is also macros, so you can override it. The classic example is **`generate_schema_name`**. By default, dbt names custom schemas `<target_schema>_<custom_schema>` (e.g. `DBT_HIEN_marts`). Many teams override it so prod uses clean names (`MARTS`) while dev stays per developer:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}{% if custom_schema_name %}_{{ custom_schema_name | trim }}{% endif %}
    {%- endif -%}
{%- endmacro %}
```

**Run a macro on its own:** `dbt run-operation grant_select --args '{role: REPORTER}'`.

### 4. Packages

Packages are other people's dbt projects (macros, tests, models) you install.

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

`dbt deps` installs them. Popular **`dbt_utils`** macros:

| Macro | Does |
|---|---|
| `generate_surrogate_key([...])` | Hash columns into one key (surrogate keys, hash diffs) |
| `star(from=ref('x'), except=[...])` | Select all columns except some |
| `date_spine(...)` | Generate one row per day/month (date dimension) |
| `union_relations([...])` | Union tables with different columns |
| `pivot(...)` | Pivot values into columns |
| `deduplicate(...)` | Keep one row per key |

### 5. Hooks

SQL that runs around models or around the whole run:

```yaml
# dbt_project.yml
models:
  ecommerce_dbt:
    marts:
      +post-hook: "grant select on {{ this }} to role REPORTER"
on-run-end:
  - "{{ log('dbt run finished', info=True) }}"
```

| Hook | Runs |
|---|---|
| `pre-hook` / `post-hook` | Before / after each model |
| `on-run-start` / `on-run-end` | Once at the start / end of `dbt run`, `build`, etc. |

(For permissions specifically, the `grants` config is cleaner: `+grants: {select: ['REPORTER']}`.)

**Don't overdo Jinja.** Models should still read like SQL. If a teammate can't understand the compiled logic from the model file, move it into a well-named macro or simplify it.

---

## Part F — Production Workflow

### 1. Environments

| | **Dev** | **Prod** |
|---|---|---|
| Target | `dev` | `prod` |
| Schema | Personal, e.g. `DBT_HIEN` | Shared, e.g. `ANALYTICS.MARTS` |
| Who runs it | You, by hand while developing | The scheduler / CI, never by hand |
| Data | Often limited (last 30 days) | Full |

Everyone develops in their own schema, so nobody overwrites anyone else's tables or breaks production dashboards.

### 2. Git workflow (from Unit 1)

```
1. git checkout -b feature/add-customer-tier
2. write/change models + tests + docs
3. dbt build --select +dim_customers+   (in your dev schema)
4. git commit + push → open a Pull Request
5. CI job builds and tests the changed models in a temporary schema
6. Code review → merge to main
7. Prod job picks up main on its next scheduled run
```

### 3. CI and "Slim CI"

Building the **whole** project on every PR is slow and expensive. **Slim CI** builds only what changed:

```bash
dbt build --select state:modified+ --defer --state ./prod-artifacts
```

| Part | Meaning |
|---|---|
| `state:modified` | Models whose code/config differs from prod's `manifest.json` |
| `+` after it | Also build everything downstream of the changes |
| `--state ./prod-artifacts` | Folder with prod's `manifest.json` to compare against |
| `--defer` | For unchanged upstream models, **read from the prod tables** instead of rebuilding them |

### 4. Scheduling production runs

- **dbt Cloud jobs:** built-in scheduler, CI on PRs, logs, alerts, hosted docs.
- **dbt Core + orchestrator:** Airflow, Dagster, Prefect, or cron running `dbt build --target prod`.

A typical prod job:

```bash
dbt deps
dbt source freshness        # stop early if raw data is stale
dbt build --target prod     # seeds, snapshots, models, tests in DAG order
dbt docs generate
```

**Artifacts** in `target/` after a run: `manifest.json` (the full project + DAG, used by Slim CI and `--defer`), `run_results.json` (timing and status of each node), `catalog.json` (warehouse metadata for docs). Use `run_results.json` to find slow models.

### 5. Model contracts and versions (governance)

- **Contracts** (`contract: {enforced: true}`) make the build **fail** if a model's columns or data types don't match what's declared in YAML. This protects dashboards from surprise schema changes.
- **Model versions** (`v1`, `v2`) let you publish a breaking change while consumers migrate.

---

## Part G — Optimization

### 1. Choose the right materialization

| Situation | Choose |
|---|---|
| Light cleaning, read rarely | **view** |
| Heavy logic, read often, small/medium size | **table** |
| Large and growing (millions+ rows), mostly new rows | **incremental** |
| Small helper logic used by 1–2 models | **ephemeral** |
| Ephemeral model used by many models, or needs debugging | Switch to **view** (ephemeral CTEs get copied into every model that uses them) |

### 2. Make incremental models correct *and* cheap

- Always set a **`unique_key`** for merge, or you'll get duplicates.
- Use a **look-back window** for late data, sized from how late data actually arrives.
- Filter on **load time** (`_loaded_at`) when business timestamps are unreliable, or use **hash diffs**.
- On Snowflake, add `incremental_predicates` to limit how much of the **target** table `MERGE` scans (e.g. only the last 7 days).
- Schedule a periodic `--full-refresh` to fix any drift.

### 3. Snowflake-specific configs

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    cluster_by=['order_day'],            -- clustering key: better pruning on big tables
    snowflake_warehouse='TRANSFORM_WH_L',-- bigger warehouse for this heavy model only
    transient=true,                      -- default: no Fail-safe storage cost
    query_tag='dbt_fct_orders'           -- find this model's queries in query history
) }}
```

- `cluster_by` works like partitioning in Unit 7: Snowflake skips micro-partitions that don't match your filter. Worth it only on large tables.
- Use a **bigger warehouse only for heavy models**, and a small one for the rest.

### 4. Speed up the whole run

- Raise **`threads`** so independent models build in parallel (bounded by the warehouse's capacity).
- **Limit data in dev** with `target.name` checks.
- Run only what you need: `--select`, `state:modified+`, `dbt retry`.
- Push filters and column selection into **staging**, so less data flows downstream (the same "filter early" rule as Spark).
- Avoid `select *` in marts. List columns so changes upstream don't silently flow through.
- Look at `run_results.json` / dbt Cloud's model timing to find the slowest models, then check Snowflake's **Query Profile** for them.

---

## Key Terms Cheat Sheet

- **Modern data stack:** EL tool → cloud warehouse → dbt → orchestrator → BI
- **ELT:** load raw data first, transform inside the warehouse. **dbt = the T**
- **dbt:** a framework that compiles SQL + Jinja and runs it in the warehouse. It doesn't process data itself.
- **dbt Core / dbt Cloud:** open-source CLI / hosted IDE, scheduler, CI and docs
- **`dbt_project.yml`:** the heart of the project: name, paths, default configs
- **`profiles.yml`:** connection credentials and targets, kept outside Git
- **Model:** a `.sql` file with one `SELECT`
- **`source()`:** reference a raw (Bronze) table declared in `sources.yml`
- **`ref()`:** reference another dbt model. Builds the DAG.
- **DAG / lineage graph:** dependency graph built from `ref()`/`source()` that decides run order
- **staging / intermediate / marts:** `stg_` 1:1 cleanup / `int_` joins / `fct_` + `dim_` business tables
- **Materialization:** view, table, incremental, ephemeral
- **Table vs incremental:** drop + rebuild everything / merge only new or changed rows
- **Ephemeral:** injected as a CTE, nothing created in the warehouse
- **Config precedence:** `config()` in model > model YAML > `dbt_project.yml`
- **`is_incremental()` / `{{ this }}`:** true on incremental runs / the model's own existing table
- **`unique_key`:** key used to update instead of duplicate
- **Incremental strategies:** append, merge, delete+insert, insert_overwrite, microbatch
- **`--full-refresh`:** rebuild an incremental model from scratch
- **Late-arriving data:** records that land after later records were already processed
- **Look-back window:** re-process the last N days each run to catch late data
- **Hash diff / hash key:** hash of business columns, compared to detect changes without timestamps
- **Append-only:** insert everything, dedupe downstream
- **Snapshot:** dbt's SCD Type 2. Strategies: `timestamp` or `check`. Columns: `dbt_valid_from`, `dbt_valid_to`.
- **Seed:** small static CSV loaded with `dbt seed` (country codes, currency mapping)
- **Generic tests:** `unique`, `not_null`, `accepted_values`, `relationships`
- **Singular test:** SQL in `tests/` that returns failing rows
- **Unit test:** checks model logic on fixed input rows
- **Source freshness:** warns/errors if raw data is too old
- **Jinja:** `{{ }}` expression, `{% %}` statement, `{# #}` comment
- **Macro:** reusable Jinja function in `macros/`
- **`var()` / `env_var()` / `target`:** project variable / environment variable / current environment
- **`generate_schema_name`:** built-in macro often overridden to control schema names
- **Package / `dbt deps`:** installable dbt code, e.g. `dbt_utils`
- **Hooks:** pre/post-hook per model, on-run-start/end per run
- **`dbt compile` / `dbt debug`:** render SQL without running / check the connection
- **`dbt build`:** seed + run + snapshot + test in DAG order
- **Node selection:** `+model`, `model+`, `tag:`, `state:modified+`
- **Slim CI:** `state:modified+` with `--defer --state` to build only changes
- **`manifest.json` / `run_results.json`:** project graph / run timings and status
- **Model contract:** enforce declared columns and types
- **`cluster_by`, `snowflake_warehouse`, `query_tag`:** Snowflake tuning configs

---

## Practice Questions

1. **What does "dbt is the T in ELT" mean, and does dbt process the data?**

    dbt handles only transformation, after data has been loaded. It compiles models into SQL and sends them to the warehouse. Snowflake does the actual processing, and no data passes through dbt.

2. **List four problems with managing transformations as raw SQL scripts, and how dbt fixes each.**

    Hand-written DDL scripts → materializations generate them. Manual run order → `ref()` builds the DAG. Renames touch many files → names come from `ref()`, change once. Long SCD2 merge scripts → snapshots.

3. **What's the difference between `dbt_project.yml` and `profiles.yml`? Why is `profiles.yml` kept outside the project?**

    `dbt_project.yml` defines the project (name, paths, default configs). `profiles.yml` holds connections and credentials. It's kept out of the repo so secrets aren't committed to Git.

4. **When do you use `source()` vs `ref()`?**

    `source()` for raw Bronze tables that dbt didn't build (declared in `sources.yml`). `ref()` for anything dbt built: models, seeds, snapshots.

5. **Why must you never hard-code table names in a model?**

    dbt wouldn't see the dependency, so the DAG and run order would be wrong, and the name wouldn't change between dev and prod schemas.

6. **What do `dbt debug` and `dbt compile` do?**

    `debug` checks the connection to Snowflake and the config files. `compile` renders Jinja into plain SQL in `target/compiled/` without running anything.

7. **What's the difference between `dbt run` and `dbt build`?**

    `run` only builds models. `build` runs seeds, models, snapshots and tests in DAG order, and skips downstream models if a test fails.

8. **What does `dbt run --select +fct_orders` build?**

    `fct_orders` and all of its upstream dependencies.

9. **Name the four materializations and one good use for each.**

    View: staging. Table: medium-sized marts. Incremental: large, growing facts. Ephemeral: small helper logic used as a CTE.

10. **Staging is a view, intermediate is ephemeral, marts are tables. Where do you set this, and how does one mart become incremental?**

    Folder-level `+materialized` configs in `dbt_project.yml`, overridden with `{{ config(materialized='incremental', ...) }}` inside that model.

11. **Explain what happens on each run of a `table` model vs an `incremental` model.**

    Table: drop, create and insert every record each run. Incremental: the first run loads everything, and later runs only merge or insert new/changed rows into the existing table.

12. **What do `is_incremental()` and `{{ this }}` do in an incremental model?**

    `is_incremental()` is true only when the table exists, the model is incremental, and there's no `--full-refresh`, so the "only new rows" filter applies. `{{ this }}` refers to the existing target table, e.g. to read its `max(updated_at)`.

13. **An incremental model without `unique_key` re-receives an updated order. What happens?**

    It's inserted again, so the order appears twice. With `unique_key='order_id'` and merge, the existing row is updated.

14. **What is late-arriving data? Show how the naive filter misses it.**

    Data that lands after later records were already processed. If the watermark is `max(updated_at) = 23:59` and an order with `updated_at = 22:10` arrives afterwards, `updated_at > 23:59` excludes it permanently.

15. **How does a look-back window fix it, and why doesn't it create duplicates?**

    Each run re-processes the last N days (`> max(updated_at) - N days`). With merge on `unique_key`, rows already present are updated instead of inserted twice.

16. **What are the trade-offs when choosing the look-back size?**

    Too small misses data that arrives later than the window. Too large re-scans a lot of data each run and costs more. Size it from observed lateness, and add a periodic full refresh as a safety net.

17. **Why can `updated_at` be unreliable, and what are the alternatives?**

    Some tables don't have it, and some systems don't update it on every change. Alternatives: hash diffs (compare a hash of the columns, most reliable), filtering on load time (`_loaded_at`), or append-only with deduplication downstream.

18. **How does a hash key detect a changed customer?**

    Hash the business columns (e.g. `generate_surrogate_key(['full_name','email','city'])`), store it, and on each run compare the new hash with the stored one. A different hash means at least one column changed.

19. **Compare `append`, `merge` and `delete+insert`.**

    Append inserts only, with no updates (immutable events). Merge updates matching keys and inserts new ones. Delete+insert removes target rows with matching keys, then inserts the new batch.

20. **What does a snapshot do, and what are its two strategies?**

    Implements SCD Type 2: tracks every version of a row with `dbt_valid_from` / `dbt_valid_to`. `timestamp` detects changes with `updated_at`. `check` compares listed columns (or a hash column).

21. **How do you get the current version of each customer from a snapshot?**

    `where dbt_valid_to is null`.

22. **Why should you snapshot raw sources, and never full-refresh a snapshot?**

    History can't be regenerated later. The snapshot table is the only copy of past versions.

23. **What are seeds for? Give two examples, and one thing they shouldn't be used for.**

    Small, static reference data that rarely changes: country codes, currency mapping. Not for loading large or frequently changing data.

24. **Name the four built-in generic tests. How does dbt decide a test passed?**

    `unique`, `not_null`, `accepted_values`, `relationships`. A test is a query returning failing rows: zero rows = pass.

25. **Write a singular test that fails if any order has a negative amount.**

    `select * from {{ ref('fct_orders') }} where amount < 0` in `tests/assert_no_negative_amounts.sql`.

26. **What do `{{ }}`, `{% %}` and `{# #}` mean in Jinja?**

    Expression (outputs a value), statement (logic, no output), comment (removed from compiled SQL).

27. **Write a Jinja loop that creates a `sum(case when ...)` column for each payment method in a list.**

    `{% set pms = ['card','cod'] %}` then `{% for pm in pms %} sum(case when payment_method = '{{ pm }}' then amount else 0 end) as {{ pm }}_amount{% if not loop.last %},{% endif %} {% endfor %}`.

28. **What is a macro, and why use one? Write one that converts cents to dollars.**

    A reusable Jinja function in `macros/` that avoids repeating logic (DRY). `{% macro cents_to_dollars(col) %} round({{ col }} / 100.0, 2) {% endmacro %}`, used as `{{ cents_to_dollars('amount_cents') }}`.

29. **How do you use less data when developing than in production?**

    Check `target.name` (e.g. `{% if target.name == 'dev' %} where ordered_at >= dateadd(day, -30, current_date) {% endif %}`), ideally wrapped in a macro.

30. **Why do many teams override `generate_schema_name`?**

    By default, dbt prefixes custom schemas with the target schema (`DBT_HIEN_marts`, and also `PROD_marts`). The override keeps clean names like `MARTS` in prod and per-developer schemas in dev.

31. **What does Slim CI run, and what do `state:modified+`, `--state` and `--defer` do?**

    Only changed models and their downstream models. `state:modified+` selects them by comparing with prod's `manifest.json` (given by `--state`). `--defer` reads unchanged upstream models from prod instead of rebuilding them.

32. **Describe the path of a change from your laptop to production.**

    Branch → edit and `dbt build` in your dev schema → PR → CI builds and tests the changes → review → merge to main → the scheduled prod job (`dbt build --target prod`) deploys it.

33. **Give four ways to make a dbt project run faster or cheaper on Snowflake.**

    Use incremental for large tables, `cluster_by` on big tables, bigger warehouses only for heavy models (`snowflake_warehouse`), more `threads`, less data in dev, only build what changed, filter early in staging.

34. **When should an ephemeral model become a view?**

    When many models use it (its SQL gets copied into each one) or when you need to query it for debugging.
