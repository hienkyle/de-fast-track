# Unit 11 — Batch Data Processing on Azure: Study Guide

**Scope:** Azure Data Factory (pipeline orchestration, activities, triggers) · Azure Databricks (managed Spark for large-scale transformation) · Azure Synapse Analytics (data warehousing plus big data analytics) · Building an end-to-end ETL/ELT pipeline

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. Unit 10 set up the stores: PostgreSQL for orders, Cosmos DB for carts and the catalog, and ADLS Gen2 for bronze/silver/gold. This unit **moves and transforms that data on a schedule**. ADF brings it into the lake, Databricks turns it into clean, modeled tables, and Synapse serves it to analysts.

---

## Part A — Batch Processing Basics ➕ Added

**Batch processing** means collecting data over a period and then processing it as one **bounded** chunk, either on a schedule or when a trigger fires. For example: "process all of yesterday's orders at 01:00". A batch job has a clear start and end. It reads a known input, writes an output, and stops.

| Trait | Meaning |
|---|---|
| **Bounded input** | The job knows exactly what it will process: a day, a file, a time window |
| **Scheduled or triggered** | It runs on a clock, per time window, or when a file arrives |
| **Throughput over latency** | It processes large volumes efficiently, so results are minutes to hours old |
| **Rerunnable** | A failed run can be retried for the same input |

**Typical batch jobs:** nightly warehouse loads, hourly or daily incremental extracts, month-end reports, ML training datasets, and housekeeping such as `OPTIMIZE` and `VACUUM`.

**Load strategies:**

| Strategy | How | When |
|---|---|---|
| **Full load** | Copy the whole table every run | Small tables, or sources with no way to track changes |
| **Incremental (delta) load** | Copy only the rows that changed since the last run, using a **watermark** (e.g. a `modified_at` column), CDC, or Change Tracking | Large tables. This is the default choice. |

**ETL vs ELT (recap from Units 5 and 8):**

- **ETL:** transform the data *before* loading it into the warehouse. On Azure this is usually Databricks or ADF Data Flows feeding Synapse.
- **ELT:** load the raw data first, then transform it *inside* the lake or warehouse with SQL, dbt, or Spark SQL. This is the modern default because storage is cheap and compute scales.

**Idempotency:** running a job twice for the same period must give the same result, so a rerun is always safe. Use `MERGE` or overwrite by partition, and never a blind `INSERT`.

---

## Part B — Azure Data Factory (ADF)

### 1. What ADF is

ADF is a **serverless, fully managed data integration and orchestration service**. It **moves data** with 90+ built-in connectors and **orchestrates** work that runs on other services (Databricks, Synapse, SQL, Functions). You build pipelines in a visual, low-code designer, and each one is stored as **JSON**.

ADF is mainly a **conductor**. It does little heavy transformation itself (Mapping Data Flows are the exception) and tells other engines when to run.

### 2. Building blocks

```
Data Factory  adf-ecom-prod
├── Linked service     ls_postgres_orders    (HOW to connect: the connection info, like a connection string)
├── Linked service     ls_sql_etl_control    (the small Azure SQL control database, see section 7)
├── Linked service     ls_adls_lake
├── Dataset            ds_pg_table           (WHAT data: a table or file shape inside a linked service)
├── Dataset            ds_bronze_parquet
├── Integration runtime  AutoResolveIntegrationRuntime   (WHERE the work runs)
├── Pipeline           pl_ingest_orders      (a logical group of activities)
│   ├── Lookup   → get last watermark
│   ├── Copy     → Postgres → bronze
│   └── Stored procedure → update watermark
└── Trigger            tr_daily_0100         (WHEN it runs)
```

| Component | Analogy | Notes |
|---|---|---|
| **Linked service** | Connection string | Authenticate with a **managed identity** (Unit 9), or pull secrets from **Key Vault**. |
| **Dataset** | A named reference to data | Can be **parameterized**, so one dataset can point at any table or path. |
| **Activity** | One step | Movement, transformation, or control flow. |
| **Pipeline** | A workflow: a **DAG** of activities (Unit 7) | Holds parameters, variables, and activity dependencies. |
| **Integration runtime (IR)** | The compute ADF uses | See below. |
| **Trigger** | A scheduler | Starts pipeline runs. |

### 3. Integration runtimes

| IR type | Runs where | Use |
|---|---|---|
| **Azure IR** | Microsoft-managed Azure compute. **AutoResolve** picks the region. | Cloud-to-cloud copies and Data Flows. Turn on the **managed virtual network** option to reach private endpoints. |
| **Self-hosted IR (SHIR)** | Software you install on a VM or on-prem machine | Sources in an **on-prem network or a private VNet**, such as an on-prem SQL Server or files on a server. Only **outbound** HTTPS is needed. |
| **Azure-SSIS IR** | Managed cluster that runs **SSIS (SQL Server Integration Services) packages** | Lifting and shifting legacy SSIS ETL |

### 4. Activities

**Dependency conditions** connect activities (the green, red, blue, and grey arrows in the designer):

| Condition | Next activity runs when the previous one... |
|---|---|
| **Succeeded** | Succeeds (the default) |
| **Failed** | Fails. Use it for error handling and alerts. |
| **Completed** | Finishes either way |
| **Skipped** | Didn't run |

**Data movement:**

- **Copy activity** is the workhorse. You give it a source, a sink, and optionally a **column mapping**. Key settings:
    - **DIUs (Data Integration Units)**: compute power on the Azure IR, set automatically by default
    - **Parallel copies**
    - **Staged copy** through Blob, used for example with PolyBase or COPY into Synapse (both explained in Part D)
    - **Fault tolerance**: skip bad rows and log them
    - **Sink write behavior**: insert or **upsert**
    - Formats: CSV, JSON, **Parquet**, Avro, ORC, Delta. Compression.

**Data transformation:**

| Activity | What it runs |
|---|---|
| **Mapping Data Flow** | Visual, code-free transformations that ADF runs on a **managed Spark cluster** it creates for you |
| **Databricks Notebook / Python / Jar** | Code in Azure Databricks (Part C). ➕ There is also a newer **Databricks Job** activity that triggers an existing Databricks job. |
| **Stored Procedure** | A stored procedure in the **SQL Server family only**: Azure SQL Database, SQL Managed Instance, SQL Server, Synapse |
| **Script** | Any SQL statements (queries, DML, DDL) against several database types |
| **Synapse Notebook / Spark job definition** | Spark code in Synapse |
| **Azure Function / Web / Webhook** | Custom code or any REST API |

**Control flow:**

| Activity | Purpose | Gotchas |
|---|---|---|
| **Lookup** | Read a value or small result set (a config table, a watermark) | Returns at most **5,000 rows / 4 MB** |
| **Get Metadata** | List files, check existence, size, last-modified | Use `childItems` to get a folder's file list |
| **ForEach** | Loop over an array | Runs in **parallel by default** (batch count up to **50**, default 20). Can be set to sequential. **No nested ForEach**, so put the inner loop in a child pipeline. |
| **If Condition / Switch** | Branching | |
| **Until** | Loop until a condition is true | Set a timeout |
| **Execute Pipeline** | Call a child pipeline | Lets you build parent/child ("master") pipelines |
| **Set / Append Variable** | Pipeline variables | Don't set the same variable inside a parallel ForEach |
| **Filter** | Filter an array | |
| **Wait / Validation** | Pause, or wait until a file exists | |
| **Fail** | Fail the pipeline on purpose with a custom message | |
| **Delete** | Delete files | |

➕ **Added — Azure Logic Apps** is Azure's low-code workflow service, with ready-made connectors for email, Microsoft Teams, and hundreds of other apps. ADF has no built-in "send email" activity, so a common pattern is a **Web activity** on the **Failed** path that calls a Logic App, which then sends the alert.

### 5. Parameters and expressions

Values are dynamic through the **expression language**. An expression starts with `@`.

| Expression | Returns |
|---|---|
| `@pipeline().parameters.tableName` | A pipeline parameter |
| `@pipeline().RunId` | This run's ID, which is handy for logging |
| `@activity('LookupWatermark').output.firstRow.last_value` | Another activity's output |
| `@item().table_name` | The current element inside a ForEach |
| `@variables('rowCount')` | A variable |
| `@trigger().outputs.windowStartTime` | The tumbling window start time |
| `@formatDateTime(utcNow(), 'yyyy/MM/dd')` | A date path such as `2026/09/25` |
| `@concat('orders/', formatDateTime(utcNow(),'yyyy/MM/dd'))` | Builds a string |

**String interpolation:** to put an expression *inside* a longer piece of text (such as a SQL query), wrap it in `@{...}`:

```
SELECT * FROM sales.orders WHERE modified_at > '@{variables('oldWatermark')}'
```

- **Parameters** are passed in when the run starts and stay fixed during it. **Variables** can change during the run.
- **Global parameters** are shared across every pipeline in the factory, for example an environment name.
- ⚠️ Anything shaped like a secret belongs in **Key Vault**, not in a parameter.

### 6. Triggers

**Publishing vs saving:** saving your work in the ADF editor doesn't make it live. **Publish** makes the saved changes live, and **only published pipelines and triggers run on a schedule**. A **Debug** run uses your current saved-but-unpublished version, so you can test before publishing (more in section 9).

| Trigger | Fires | Key facts |
|---|---|---|
| **Schedule** | On a wall-clock schedule (every day at 01:00 Asia/Saigon) | **Many-to-many**: one trigger can start several pipelines, and a pipeline can have several triggers. Doesn't backfill. |
| **Tumbling window** | Back-to-back, **fixed-size, non-overlapping** time windows (e.g. every hour) | **One pipeline per trigger.** Passes `windowStartTime`/`windowEndTime`. **Backfill** by setting a past start date. **Retry policy**, **max concurrency**, and **dependencies** on other tumbling window triggers. Best fit for incremental batch loads. |
| **Storage event** | A blob is **created or deleted** in Blob/ADLS Gen2, filtered by path prefix and suffix | Built on **Azure Event Grid**, Azure's event-routing service: the storage account publishes "blob created/deleted" events to Event Grid, and ADF subscribes to them. The `Microsoft.EventGrid` resource provider must be registered on the subscription. Example: start when a `.csv` lands in `landing/`. |
| **Custom event** | An event your own application publishes to a custom **Event Grid topic** | Event-driven integration with other systems |
| Manual | **Trigger now** or **Debug** | |

Each execution of a pipeline is a **pipeline run**. You can look at pipeline runs, activity runs, and trigger runs in the **Monitor** hub.

### 7. Common patterns

**The control database.** The pipeline's own bookkeeping (a control table of sources, a watermark table, a run log) lives in a small **Azure SQL Database**, `sqldb-etl-control`, in the `etl` schema. It's kept separate from the source systems, and because it's in the SQL Server family, ADF's Stored Procedure activity can update it.

**a) Incremental load with a watermark:**

```
Lookup  OldWM   (sqldb-etl-control)  SELECT last_value FROM etl.watermark WHERE table_name = 'orders'
        │
        ▼
Copy   source (PostgreSQL):
         SELECT * FROM sales.orders
         WHERE modified_at >  '@{activity('OldWM').output.firstRow.last_value}'
           AND modified_at <= '@{trigger().outputs.windowEndTime}'
       sink: bronze/postgres/orders/@{formatDateTime(trigger().outputs.windowStartTime,'yyyy/MM/dd')}/
        │ (Succeeded)
        ▼
Stored procedure (sqldb-etl-control)   etl.usp_update_watermark('orders', windowEndTime)
```

The new watermark is only saved **after** the copy succeeds. A failed run therefore re-copies the same range next time instead of skipping it.

The run's boundaries come from the **tumbling window trigger** (section 6), not from the clock: the upper bound is the window end, and the bronze folder is named from the window start. With `utcNow()` or a run-time `MAX(modified_at)`, a rerun would land in a different folder with a different range (and at 01:00 Asia/Saigon, `utcNow()` is still the previous day in UTC). Named by window, the files sit exactly where the Databricks notebook looks for its `run_date` (Part C §5).

**b) Metadata-driven pipeline.** One generic pipeline replaces fifty copies of the same thing:

```
Lookup (sqldb-etl-control)  SELECT * FROM etl.control_table WHERE enabled = 1
                            → [{schema, table, load_type, watermark_col, ...}]
  └── ForEach @activity('GetTables').output.value   (parallel, batch count 10)
        └── Execute Pipeline  pl_copy_one_table (schema=@item().schema, table=@item().table, ...)
```

A new source table is then just a new row in the control table.

### 8. Mapping Data Flows

- A visual transformation designer: source → derived column / filter / join / aggregate / pivot / lookup / **alter row** (upsert/delete) / window → sink.
- ADF compiles the flow to **Spark** and runs it on a cluster it manages. **Cold start takes a few minutes**, which you can cut down by setting a **TTL** (time to live) on the Azure IR so the cluster stays warm between runs.
- Billed per **vCore-hour** of cluster time. Good for teams who don't write code. For complex or large logic, **Databricks** usually gives more control and costs less.

### 9. Security, DevOps, monitoring, cost

| Area | Practice |
|---|---|
| **Identity** | Give the factory a **system-assigned managed identity** and grant it roles such as Storage Blob Data Contributor on `bronze` or a database user. Store any remaining secrets in a **Key Vault linked service**. |
| **Networking** | **Managed VNet + managed private endpoints** for the Azure IR. Use a SHIR for on-prem sources. |
| **Git integration** | Author in feature branches and merge into the **collaboration branch** (`main`). **Publish** generates **ARM templates** (Unit 9), historically in the `adf_publish` branch. |
| **CI/CD** | Deploy the ARM templates dev → test → prod with Azure DevOps or GitHub Actions. Override environment values such as linked-service URLs with parameters. ➕ The `@microsoft/azure-data-factory-utilities` npm package automates publishing. **Stop triggers before a deployment** and restart them after. |
| **Monitoring** | Monitor hub, **alerts** on failed runs, **diagnostic settings → Log Analytics** to keep run history beyond ADF's 45 days and query it with **KQL (Kusto Query Language)** |
| **Retries** | Set **retry count and interval** on activities to ride out brief failures |
| **Cost** | You pay per **activity run** (orchestration), **DIU-hours** (copy), **vCore-hours** (Data Flows), and SHIR hours. Many tiny activities add up. |

➕ **Added — Data Factory in Microsoft Fabric** is ADF's successor inside Fabric. It has the same concepts (pipelines, activities, copy) plus **Dataflow Gen2**. Classic ADF is still fully supported and widely used.
➕ **Added — Apache Airflow** is the code-first alternative orchestrator (Python DAGs). Some teams use Airflow to orchestrate Databricks instead of ADF.

---

## Part C — Azure Databricks

### 1. What it is

Azure Databricks is a **first-party Azure service** jointly built by Microsoft and Databricks. It provides:

- **Managed Apache Spark** (Unit 7), optimized, with the **Photon** engine
- **Delta Lake** as the default table format (ACID, time travel, MERGE)
- **Unity Catalog** for governance
- Notebooks, jobs, SQL warehouses, and ML tooling

Together these make up a **lakehouse**: warehouse features on top of open files in ADLS Gen2.

### 2. Architecture

```
┌──────────── Control plane (managed by Databricks) ────────────┐
│ Web UI, notebooks, job scheduler, cluster manager, Unity Catalog │
└────────────────────────────┬────────────────────────────────────┘
                             │
      ┌──────────────────────┴───────────────────────┐
      ▼                                              ▼
 Classic compute plane                       Serverless compute plane
 (VMs in YOUR subscription, in a              (compute in Databricks' Azure
  managed resource group, optionally           account. Starts in seconds,
  VNet-injected into your VNet)                no VMs for you to manage.)
      │                                              │
      └──────────────► ADLS Gen2 (your data stays in your storage) ◄──┘
```

- **Workspace:** the Azure resource you create. It comes with a **managed resource group** that holds the cluster VMs and disks.
- **VNet injection + secure cluster connectivity (no public IP)** is the standard way to lock down classic compute (Unit 9 networking).

### 3. Compute

| Compute type | Use | Billing |
|---|---|---|
| **All-purpose cluster** | Interactive notebooks and exploration. **Shared** by users. | More expensive DBU rate |
| **Job cluster** | Created **for one job run** and terminated afterwards | **Cheaper DBU rate. Use it for production batch.** |
| **Serverless (notebooks, jobs, pipelines)** | No cluster setup, starts in seconds, autoscales | DBUs only. No separate VM charge. |
| **SQL warehouse** (classic, pro, **serverless**) | SQL queries, BI tools, dbt | DBUs |
| **Instance pools** | Keep idle VMs ready so clusters start faster | You pay for the idle VMs |

**Cost = DBUs (Databricks Units, a unit of processing per hour) + Azure VM cost** for classic compute.

**Key cluster settings:**

- **Databricks Runtime (DBR):** a version bundling Spark, Delta, and libraries. Pick an **LTS** (long-term support) version for production, or the **ML runtime** for machine learning.
- **Driver + workers**, **autoscaling** (min/max workers), **auto-termination** after N idle minutes (always set it on all-purpose clusters).
- **Photon:** a C++ engine that processes data in column batches and speeds up SQL and DataFrame work.
- **Access mode:** **Standard** (formerly *shared*: multi-user, Unity Catalog isolation) or **Dedicated** (formerly *single user*).
- ➕ **Spot instances** for workers are cheaper but can be evicted.

### 4. Unity Catalog (governance)

**Three-level namespace:** `catalog.schema.table`

```
Metastore (one per region, attached to workspaces)
└── Catalog   ecom_prod
    └── Schema   silver
        ├── Table    orders          (managed or external Delta table)
        ├── View     v_orders_recent
        ├── Volume   raw_files       (governed access to non-tabular files, path /Volumes/<catalog>/<schema>/<volume>/)
        └── Function mask_email
```

| Concept | Meaning |
|---|---|
| **Managed table** | Unity Catalog manages both the metadata **and** the files. `DROP TABLE` removes the data. **Recommended default.** |
| **External table** | Metadata only. The files live at a path you control. `DROP` leaves the files. |
| **Storage credential** | How Databricks authenticates to ADLS, usually an **Access Connector for Azure Databricks**, which is a **managed identity** holding Storage Blob Data Contributor |
| **External location** | Storage credential + an `abfss://` path that users are allowed to read from or write to |
| **Grants** | `GRANT SELECT ON TABLE ecom_prod.gold.fact_sales TO \`grp-analysts\`` |
| **Lineage, audit** | Captured automatically at table and column level |

⚠️ **DBFS mounts and storage account keys in notebooks are legacy.** Use Unity Catalog external locations and volumes.

### 5. Delta Lake operations you'll use in batch jobs (Unit 7 recap)

**`dbutils`** is the Databricks utility library that's built into every notebook. It covers **widgets** (notebook parameters), **secrets**, filesystem helpers, and `notebook.exit` for returning a value. Batch notebooks read their run date from a widget instead of hard-coding it, so the same code works for any day and for reruns.

```python
# Bronze → Silver: dedupe + upsert (idempotent)
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

run_date = dbutils.widgets.get("run_date")            # e.g. "2026-09-25", passed in by ADF or a job
path = f"abfss://bronze@stecomlakeprod.dfs.core.windows.net/postgres/orders/{run_date.replace('-', '/')}/"

raw = spark.read.parquet(path)
latest = (raw.withColumn("rn", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.col("modified_at").desc())))
             .filter("rn = 1").drop("rn"))

(DeltaTable.forName(spark, "ecom_prod.silver.orders").alias("t")
   .merge(latest.alias("s"), "t.order_id = s.order_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
```

| Command | Purpose |
|---|---|
| `MERGE INTO` | Upsert or SCD logic (Unit 5) |
| `OPTIMIZE` | Compact small files |
| **Liquid clustering** (`CLUSTER BY`) | Databricks' newer way to lay out data files. It replaces the manual partitioning + **Z-ORDER** from Unit 7 with clustering keys that you can change later without rewriting the table. Recommended for new tables. |
| `VACUUM` | Delete files the table no longer references (default 7-day retention) |
| Time travel `VERSION AS OF` / `TIMESTAMP AS OF` | Audit or roll back a bad load |

### 6. Ingestion inside Databricks

**Auto Loader** (`cloudFiles`) incrementally picks up **new files** that land in a folder, with schema inference and **schema evolution**. It's built on Spark's **Structured Streaming** API, which Unit 12 covers. For batch work, you only need three ideas:

- `readStream` / `writeStream` read and write **only input that hasn't been processed yet**.
- The **checkpoint** is a folder where Auto Loader records which files it has already processed, so a file is never loaded twice.
- **`trigger(availableNow=True)`** makes it behave like a **batch job**: process everything new, then stop.

```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/ecom_prod/bronze/_meta/schemas/clicks")
   .load("abfss://landing@stecomlakeprod.dfs.core.windows.net/clickstream/")
 .writeStream
   .option("checkpointLocation", "/Volumes/ecom_prod/bronze/_meta/checkpoints/clicks")
   .trigger(availableNow=True)
   .toTable("ecom_prod.bronze.clickstream"))
```

- **`COPY INTO`** (Databricks SQL): a command that idempotently loads new files into a Delta table and skips files it has already loaded. Simpler than Auto Loader, suited to thousands of files rather than millions.
- ➕ **Lakeflow Connect**: managed connectors for databases and SaaS apps, similar to ADF Copy but built into Databricks.

### 7. Orchestration inside Databricks

| Feature | Formerly | What it does |
|---|---|---|
| **Lakeflow Jobs** | *Workflows / Jobs* | Multi-task **DAGs** (notebook, Python, SQL, dbt, pipeline tasks). Features: **job clusters or serverless**, schedules, **file-arrival triggers**, retries, parameters, email/webhook alerts, repair-and-rerun of failed tasks only. |
| **Lakeflow Declarative Pipelines** | ***Delta Live Tables (DLT)*** | You **declare** the tables you want and Databricks works out the order, retries, and incremental processing. It builds two kinds of tables, and supports **expectations** for data quality. |

The two table types in a declarative pipeline:

- **Streaming table:** processes **each new source row only once** (incremental), which suits append-only data like Bronze → Silver.
- **Materialized view:** a **stored query result** that the pipeline keeps up to date. It suits aggregates and Gold tables.

```python
# Declarative pipeline (Python API; older code uses `import dlt` and @dlt.table)
from pyspark import pipelines as dp

@dp.table(comment="Cleaned orders")                       # a streaming table
@dp.expect_or_drop("valid_amount", "amount >= 0")   # rows failing the rule are dropped and counted
def silver_orders():
    return spark.readStream.table("ecom_prod.bronze.orders").dropDuplicates(["order_id"])
```

**ADF or Lakeflow Jobs to orchestrate?** If everything happens inside Databricks, Lakeflow Jobs is simpler. If the pipeline spans many services (SHIR sources, SQL procedures, Synapse, Logic Apps), put **ADF** on top and let it call Databricks.

### 8. Development practices

- **Git folders** (formerly *Repos*) keep notebooks and code in Git.
- ➕ **Databricks Asset Bundles (DABs)** define jobs, pipelines, and clusters as YAML, deployed dev → prod with the CLI in CI/CD.
- **Secrets:** secret scopes, including **Key Vault–backed scopes**. `dbutils.secrets.get("kv-scope", "pg-password")`. Values are redacted in notebook output.
- **Parameters:** `dbutils.widgets.get("run_date")` receives values from ADF or jobs.
- **dbt on Databricks** (Unit 8): run dbt as a job task against a SQL warehouse.

### 9. Calling Databricks from ADF

- **Linked service:** Azure Databricks, authenticated with **ADF's managed identity** (grant it access to the workspace).
- Cluster options: a **new job cluster** per run (cheapest and isolated, but slower to start), an **existing all-purpose cluster** (fast to start, more expensive, avoid in prod), or an **instance pool**.
- Pass parameters from ADF (`baseParameters: {"run_date": "@{formatDateTime(trigger().outputs.windowStartTime,'yyyy-MM-dd')}"}`) and read them with `dbutils.widgets.get`.
- Return values with `dbutils.notebook.exit(json.dumps({...}))` and read them in ADF as `@activity('Transform').output.runOutput`.

---

## Part D — Azure Synapse Analytics

### 1. What Synapse is

Synapse is a **unified analytics workspace** that combines, in one resource and one UI (**Synapse Studio**):

| Component | What it is |
|---|---|
| **Dedicated SQL pool** | Provisioned **MPP data warehouse** (formerly *Azure SQL Data Warehouse*) |
| **Serverless SQL pool** | Always-available, **pay-per-query** SQL over files in the lake |
| **Apache Spark pools** | Managed Spark for data engineering and ML |
| **Synapse Pipelines** | **The same engine as ADF**, built into the workspace |
| **Synapse Link** | Near-real-time replication from operational stores into Synapse for analytics |

- A workspace needs a **primary ADLS Gen2 account** and a **managed identity**.
- **Status:** Synapse is generally available and supported, and Microsoft hasn't announced a retirement. But **Microsoft Fabric** is its strategic successor, and new investment goes there. Expect to see Synapse in many existing enterprises, and Fabric or Databricks in new builds. The MPP ideas below carry straight over to Fabric Warehouse and other cloud warehouses.

### 2. Dedicated SQL pool (the MPP warehouse)

**MPP (massively parallel processing) architecture:**

```
            Client (T-SQL)
                 │
          ┌──────▼──────┐
          │ Control node │   parses the query, builds a distributed plan
          └──────┬──────┘
   ┌─────────┬───┴─────┬─────────┐
   ▼         ▼         ▼         ▼
 Compute   Compute   Compute   Compute    ← number depends on DWU
 node      node      node      node
   └── each works on its share of the 60 distributions ──┘
        DMS (Data Movement Service) shuffles rows between nodes when needed
                 │
          Azure Storage (data stored separately from compute)
```

- Every table is split into **60 distributions**. More **DWUs** (Data Warehouse Units, **DW100c → DW30000c**) spread them across more compute nodes.
- **Compute and storage are separate.** You can **pause** compute (you then pay only for storage) and **scale** it up or down.
- It uses **T-SQL** (Unit 10), with some features missing or different.

**Distribution types (the key design decision, like Cosmos partition keys):**

| Type | How rows are placed | Use for |
|---|---|---|
| **Hash** | `HASH(column)` decides the distribution | **Large fact tables** (> ~2 GB). Choose a **high-cardinality column that's often used in joins/GROUP BY**, with **even values** and **not a date** (all of one day's rows would land together). |
| **Round robin** | Rows spread evenly and randomly | **Staging tables** and fast loads. Joins need data movement. |
| **Replicated** | A **full copy cached on every compute node** | **Small dimension tables** (< ~2 GB compressed). Joins need no movement. |

**Goal:** avoid **data skew** (one distribution holding much more than others) and **data movement** (shuffles) in join-heavy queries. `DBCC PDW_SHOWSPACEUSED('dbo.fact_sales')` shows how rows are spread.

**Indexes:**

| Index | Use |
|---|---|
| **Clustered columnstore (CCI)** *(default)* | Large fact tables. Heavy compression, fast scans. |
| **Heap** | Staging or temporary loads (fastest to load) |
| **Clustered / nonclustered rowstore** | Small tables, point lookups |

A columnstore stores rows in **rowgroups**: batches of up to about **1 million rows**, compressed one column at a time. Full rowgroups compress well and scan fast. Small ones compress and scan poorly.

⚠️ **Don't over-partition.** A CCI table is already split 60 ways, so 100 partitions means 6,000 pieces, each too small to fill a rowgroup. Partition mainly for **partition switching**, a metadata-only operation that instantly swaps a whole partition into or out of a table. It's used for fast loads and for archiving old months. Monthly or yearly partitions are usually enough.

**Loading data:**

`COPY INTO` (Synapse's T-SQL loader, not the same command as Databricks' `COPY INTO`) and the older **PolyBase** external tables read **plain files** (Parquet, CSV). ⚠️ They don't read the Delta transaction log. Pointed at a Delta table folder, they would also load old and deleted file versions and create duplicates. So Databricks first writes a **plain Parquet export** of the Gold table, and Synapse loads that.

```python
# Databricks, at the end of the gold job: a clean Parquet snapshot for Synapse
spark.table("ecom_prod.gold.fact_sales").write.mode("overwrite").parquet(
    f"abfss://gold@stecomlakeprod.dfs.core.windows.net/_export/fact_sales/{run_date}/")
```

```sql
-- 1) Load into a round-robin heap staging table
COPY INTO stg.fact_sales
FROM 'https://stecomlakeprod.blob.core.windows.net/gold/_export/fact_sales/2026-09-25/*.parquet'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

-- 2) CTAS (CREATE TABLE AS SELECT) into the final distributed table: fast and fully parallel
CREATE TABLE dbo.fact_sales
WITH (DISTRIBUTION = HASH(customer_key), CLUSTERED COLUMNSTORE INDEX)
AS SELECT * FROM stg.fact_sales;

CREATE TABLE dbo.dim_product
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX)
AS SELECT * FROM stg.dim_product;
```

- **PolyBase / external tables:** the older loading path. COPY INTO is simpler and recommended.
- **Performance tools:** **result set caching**, **materialized views**, **statistics** (keep them updated after loads).
- **Workload management:** **workload groups and classifiers** (or older *resource classes*) reserve resources so ETL loads don't starve BI queries.

### 3. Serverless SQL pool

- **Always on, nothing to provision.** Each workspace gets the `Built-in` endpoint.
- **You pay per TB of data processed** by your queries. **Parquet/Delta + partition pruning + selecting only the columns you need** keep the cost low.
- It **queries files in the lake** directly (CSV, Parquet, **Delta**, JSON). **It stores no data itself** except metadata.

```sql
-- Ad-hoc exploration
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://stecomlakeprod.dfs.core.windows.net/gold/sales/daily_revenue/',
    FORMAT = 'DELTA') AS r;

-- A "logical data warehouse": views over the lake for Power BI
CREATE VIEW gold.v_daily_revenue AS
SELECT * FROM OPENROWSET(BULK '.../gold/sales/daily_revenue/', FORMAT='DELTA') AS r;
```

⚠️ **Delta compatibility:** serverless SQL reads Delta tables through its own reader, which may not support newer Delta features that Databricks can turn on (for example **deletion vectors**, or features that come with liquid clustering). Check which features the Synapse reader supports, or disable the newer features on the tables Synapse will read.

- **External tables** (external data source + file format + table) give a table-like schema over files.
- **CETAS** (`CREATE EXTERNAL TABLE AS SELECT`) **writes query results back to the lake** as Parquet. This is a cheap way to transform data with SQL.

| | **Dedicated SQL pool** | **Serverless SQL pool** |
|---|---|---|
| Storage | Its own managed storage | **None**. It reads the lake. |
| Billing | Per **DWU-hour** (pause to save) | Per **TB processed** |
| Performance | Predictable and high for heavy BI | Good for exploration and light BI. Varies. |
| Use | Enterprise warehouse, many concurrent dashboards | Data discovery, a logical warehouse over gold, quick checks |

### 4. Apache Spark pools

- Managed Spark clusters defined by node size and count, with **autoscale** and **auto-pause**. Notebooks support PySpark, Scala, Spark SQL, and .NET.
- **Lake database / shared metadata:** Spark tables (Parquet/Delta) created in Synapse Spark **show up automatically in serverless SQL**, so analysts can query them with T-SQL.
- Compared with Databricks: simpler and included in the Synapse workspace, but Databricks usually has newer Spark and Delta features, Photon, Unity Catalog, and stronger tooling. Many teams run **Databricks for Spark with Synapse or Fabric for SQL serving**.

### 5. Pipelines, Synapse Link, security

- **Synapse Pipelines ≈ ADF.** They share the same activities, triggers, and expressions, with small feature differences. Use either one, and don't duplicate orchestration across both.
- **Synapse Link** gives near-real-time analytics copies of **Azure SQL / SQL Server**, **Dataverse** (the Power Platform's business data store behind Dynamics 365 and Power Apps), and Cosmos DB. For Cosmos DB, new projects should prefer **Fabric mirroring** (Unit 10).
- **Security:** a **managed virtual network** with **managed private endpoints**, **Synapse RBAC** roles (Synapse Administrator, SQL Administrator, Contributor), Entra authentication, and the workspace **managed identity** for lake access. Dedicated pools also support **row-level security, column-level security, dynamic data masking, and TDE** (Unit 10).

➕ **Added — Microsoft Fabric equivalents:**

| Synapse | Fabric |
|---|---|
| Dedicated SQL pool | **Fabric Warehouse** (no distributions to design, stored as Delta in **OneLake**) |
| Serverless SQL pool | **SQL analytics endpoint** of a Lakehouse |
| Spark pools | **Fabric Spark / Lakehouse** notebooks |
| Pipelines | **Data Factory in Fabric** |

Microsoft provides a **migration assistant** for moving dedicated SQL pools to Fabric Warehouse.

---

## Part E — Which Tool for Which Job? ➕ Added

| Need | Best fit |
|---|---|
| Copy data from 30 sources (including on-prem) into the lake on a schedule | **ADF** (Copy + SHIR + metadata-driven pattern) |
| Orchestrate steps across many Azure services | **ADF** |
| Heavy, code-based transformations on TBs, Delta MERGE, ML | **Databricks** |
| Low-code transformations for a non-coding team | ADF Mapping Data Flows |
| Enterprise warehouse with high-concurrency BI and T-SQL skills | **Synapse dedicated SQL pool** (or Fabric Warehouse / Databricks SQL) |
| Ad-hoc SQL over lake files, paying only when you query | **Synapse serverless SQL** |
| SQL-based ELT with testing and docs | **dbt** on Databricks SQL or Synapse (Unit 8) |
| All-in-one SaaS analytics for a new project | **Microsoft Fabric** |

**The classic Azure batch architecture:** **ADF orchestrates → Databricks transforms → Synapse (or Databricks SQL) serves → Power BI reports**, all on **ADLS Gen2** with the medallion layers.

---

## Part F — Building an End-to-End ETL/ELT Pipeline

### 1. The scenario

Every night, load yesterday's e-commerce data (orders from PostgreSQL, payments from Azure SQL, the product catalog from Cosmos DB, and supplier CSVs dropped on an SFTP server) into a **star schema** (Unit 5) for the sales dashboard.

### 2. Architecture

```
 SOURCES                        INGEST (ADF)                  LAKE: ADLS Gen2 (HNS, ZRS, private endpoints)
 ───────                        ────────────                  ─────────────────────────────────────────────
 PostgreSQL (orders)  ──┐
 Azure SQL (payments) ──┼─► pl_ingest (metadata-driven, ──► bronze/  raw Parquet/JSON, by window date, append-only
 Cosmos DB (catalog)  ──┤     watermark, managed identity)
 SFTP supplier CSVs   ──┘     SHIR for on-prem sources
                                    ▲
                                    │ control table, watermarks, run log
                              sqldb-etl-control (Azure SQL, etl schema)
                                                                 │  Databricks job (job cluster / serverless)
                                                                 ▼  dedupe, cast types, MERGE, data-quality checks
                                                             silver/  clean Delta tables (UC: ecom_prod.silver.*)
                                                                 │  Databricks or dbt: star schema, SCD2 dims, aggregates
                                                                 ▼
                                                             gold/    fact_sales, dim_customer, dim_product, dim_date
                                                                 │    (+ gold/_export/ Parquet snapshots for Synapse loads)
                                SERVE  ──────────────────────────┤
                                 ├─ A: Synapse serverless views over gold Delta (cheap, simple)
                                 └─ B: COPY INTO a Synapse dedicated SQL pool from gold/_export/ (heavy BI concurrency)
                                                                 ▼
                                                           Power BI dashboards

 ORCHESTRATION: ADF pl_master_nightly  ← tumbling window trigger (daily, 01:00 Asia/Saigon)
   Execute pl_ingest → Databricks Job (silver + gold) → Synapse load / refresh → notify
   On Failure → Web activity → Logic App → Teams/email alert;  every step logs to etl.run_log
```

### 3. Step by step

**Step 1: Ingest to Bronze (ADF, the "E" and "L")**

- The **control table** `etl.control_table` in `sqldb-etl-control` lists each source, table, load type (full or incremental), and watermark column.
- **Lookup → ForEach → Copy** writes **raw, unchanged** data to `bronze/<source>/<entity>/yyyy/MM/dd/` (the trigger window's date).
- Incremental extracts from PostgreSQL and Azure SQL use a **watermark** (Cosmos DB can use its `_ts` property). The watermark is saved to `etl.watermark` by a Stored Procedure activity only after a successful copy.
- Log row counts from the Copy output (`@activity('Copy').output.rowsCopied`) to `etl.run_log`.

**Step 2: Bronze → Silver (Databricks, the "T")**

- The notebook reads `run_date` from a widget. It then deduplicates (keeping the latest row per key), casts types, standardizes values (time zones to UTC, currency codes), and **MERGEs** into Silver Delta tables.
- **Data quality:** expectations or checks such as non-null keys, `amount >= 0`, and referential checks. Bad rows go to a **quarantine** table instead of breaking the load.

**Step 3: Silver → Gold (Databricks SQL or dbt)**

- Build the **star schema**: `fact_sales` at the order-line grain, plus **SCD Type 2** `dim_customer` (Unit 5), `dim_product`, and `dim_date`.
- Build pre-aggregated marts (`daily_revenue`). Use `OPTIMIZE` or liquid clustering on large tables.
- If Option B below is used, finish by writing the **Parquet export** of each Gold table to `gold/_export/<table>/<run_date>/`.

**Step 4: Serve (Synapse)**

- **Option A (ELT-light):** serverless SQL **views** over the gold Delta tables. There's no copy, and you pay per query. Keep the Gold tables readable by Synapse's Delta reader (see the compatibility warning in Part D.3).
- **Option B (classic load into a warehouse):** `COPY INTO` staging tables from `gold/_export/`, then **CTAS/MERGE** into hash-distributed CCI facts and replicated dims in a **dedicated SQL pool**. Pause the pool outside business hours.
- Power BI connects to the Synapse endpoint (or directly to a Databricks SQL warehouse).

**Step 5: Orchestrate and operate**

| Concern | How |
|---|---|
| **Scheduling** | **Tumbling window** trigger so each run knows its `windowStart`/`windowEnd` (passed to Databricks as `run_date`) and missed days can be **backfilled** |
| **Modularity** | Master pipeline → **Execute Pipeline** children (ingest, transform, serve) |
| **Error handling** | Activity **retries**. A **Failed** path to a Web activity that calls a Logic App, which sends a Teams or email alert. **Fail** activity with a clear message. |
| **Idempotency** | Watermark ranges bounded by the window end, bronze folders named from the window start (never `utcNow()`), **MERGE** instead of INSERT, overwrite by partition. Rerunning the failed 2026-09-24 window copies the same range into the same folder and gives the same result. |
| **Logging/lineage** | `etl.run_log` in `sqldb-etl-control` (run ID, window, rows read and written, status, duration). ADF Monitor + **Log Analytics**. Unity Catalog lineage. |
| **Security** | **Managed identities** everywhere (ADF, Databricks access connector, Synapse), **Key Vault** for the remaining secrets, **private endpoints** (blob + dfs), least-privilege RBAC and ACLs (Units 9–10) |
| **CI/CD** | ADF Git + ARM deployment. **Databricks Asset Bundles**. dbt in CI. Separate dev/test/prod environments. |
| **Cost** | Job clusters or serverless instead of all-purpose clusters. Auto-terminate and auto-pause. Pause dedicated pools. Parquet/Delta to lower serverless scan costs. |

### 4. ETL vs ELT version of this pipeline

| | **ETL flavor** | **ELT flavor** |
|---|---|---|
| Where transformation happens | Databricks (Spark) **before** loading the warehouse | **Inside** the lakehouse or warehouse with SQL/dbt, after a raw load |
| Warehouse holds | Only modeled gold tables | Raw + staged + modeled data |
| Strength | Heavy, complex logic. Unstructured data. | Simple, SQL-first. Analytics engineers can own it. Easy to replay from raw. |

Most real Azure pipelines are a **hybrid**: ELT into bronze, Spark for heavy cleansing, and SQL/dbt for gold modeling.

### 5. Best-practices checklist

- [ ] Keep bronze **raw and immutable** so you can always reprocess
- [ ] **Parameterize** everything: dates, paths, environments. No hard-coding.
- [ ] Use **incremental loads** with watermarks or CDC, and make every step **idempotent**
- [ ] Keep control, watermark and log tables in a **separate control database**
- [ ] Add **data-quality checks** between layers, with quarantine rather than silent drops
- [ ] Use **job clusters or serverless** for production. Never leave all-purpose clusters running.
- [ ] Use **managed identities + Key Vault**. No keys in notebooks or JSON.
- [ ] Set up **alerting on failure** and **audit logging** of row counts
- [ ] Use **Git + CI/CD** for ADF, Databricks, and SQL objects
- [ ] Watch for **small files** (OPTIMIZE) and **skew** (distribution and partition choices)
- [ ] Load warehouses from **plain Parquet exports**, never directly from Delta folders

---

## Key Terms Cheat Sheet

- ➕ **Batch processing:** bounded input, scheduled or triggered, throughput over latency, rerunnable
- **Full vs incremental load; watermark:** copy everything / only changed rows, tracked by a high-water mark
- **Idempotent:** a rerun gives the same result
- **ADF:** serverless orchestration and data movement service
- **Linked service / dataset / activity / pipeline / trigger / IR:** connection / data shape / step / workflow (DAG) / scheduler / compute
- **Azure IR / self-hosted IR / Azure-SSIS IR:** cloud / on-prem or private network / SSIS packages
- **Dependency conditions:** Succeeded, Failed, Completed, Skipped
- **Copy activity:** source → sink with DIUs, parallel copies, staging, fault tolerance
- **Stored Procedure activity:** SQL Server family only. **Script activity:** SQL on several database types.
- **Lookup (≤ 5,000 rows), Get Metadata, ForEach (parallel ≤ 50, no nesting), If, Switch, Until, Execute Pipeline, Fail**
- ➕ **Logic Apps:** low-code workflows with connectors. Called from ADF's Web activity for alerts.
- **Expressions:** `@pipeline().parameters.x`, `@activity('A').output`, `@item()`, `@trigger().outputs.windowStartTime`. `@{...}` inserts an expression into a string.
- **Publish vs save/debug:** only published pipelines and triggers run on a schedule. Debug runs the saved, unpublished version.
- **Schedule trigger:** clock-based, many-to-many, no backfill
- **Tumbling window trigger:** fixed non-overlapping windows, one pipeline, backfill, retries, dependencies
- **Storage event trigger:** fires on blob created/deleted, via **Event Grid** (Azure's event-routing service)
- **Control database:** Azure SQL DB holding the control table, watermarks and run log
- **Metadata-driven pipeline:** control table + Lookup + ForEach
- **Mapping Data Flow:** visual transformations running on ADF-managed Spark
- **ADF Git:** collaboration branch → publish → ARM templates → CI/CD. Monitoring history in Log Analytics, queried with **KQL**.
- **Azure Databricks:** managed Spark + Delta + Unity Catalog (a lakehouse)
- **Control plane vs compute plane (classic vs serverless)**
- **All-purpose vs job cluster vs serverless vs SQL warehouse;** DBUs + VM cost
- **Photon, DBR LTS, autoscaling, auto-termination, access modes**
- **Unity Catalog:** `catalog.schema.table`. Managed vs external tables. Storage credential (access connector) + external location. Volumes.
- **`dbutils`:** widgets (parameters), secrets, filesystem helpers, `notebook.exit`
- **Liquid clustering:** changeable clustering keys that replace partitioning + Z-ORDER
- **Auto Loader (`cloudFiles`):** new files only, checkpoint tracks progress, `availableNow` = run as batch
- **COPY INTO (Databricks):** idempotent SQL file load into Delta
- **Lakeflow Jobs** (formerly Workflows), **Lakeflow Declarative Pipelines** (formerly DLT) with expectations
- **Streaming table vs materialized view:** each new row processed once / stored query result kept up to date
- **Databricks Asset Bundles, Key Vault–backed secret scopes**
- **Synapse:** workspace with dedicated SQL, serverless SQL, Spark pools, pipelines, Synapse Link. Fabric is its successor.
- **Dedicated SQL pool:** MPP, control node + compute nodes, **60 distributions**, DWUs, pause/scale
- **Hash / round robin / replicated:** big facts / staging / small dims (< 2 GB)
- **Data skew & data movement (DMS):** what to avoid
- **CCI (default), heap; rowgroup ≈ 1M rows; don't over-partition; partition switching**
- **COPY INTO (Synapse), CTAS, PolyBase:** read plain files, **not Delta**. Load from a Parquet export.
- **Result set caching, materialized views, statistics, workload groups**
- **Serverless SQL pool:** pay per TB processed, `OPENROWSET`, external tables, **CETAS**, logical data warehouse. Check Delta feature compatibility.
- **Lake database:** Spark tables visible to serverless SQL
- **Dataverse:** Power Platform's data store (a Synapse Link source)
- **ETL vs ELT; medallion bronze/silver/gold; SCD2; quarantine**

---

## Practice Questions

1. **What makes a job "batch"?**

    It processes a bounded input (a day, a file, a window), runs on a schedule or trigger, favors throughput over latency, and can be rerun for the same input.

2. **ADF: explain the difference between a linked service and a dataset.**

    A linked service is the connection (how to connect and authenticate). A dataset is a named reference to specific data (a table or file path) through that linked service.

3. **Which integration runtime do you need to copy from a SQL Server in the company's data center?**

    A self-hosted IR installed on a machine that can reach the server. It only needs outbound HTTPS.

4. **Your Lookup activity needs to read 20,000 config rows. Problem?**

    Lookup returns at most 5,000 rows / 4 MB. Page the results, filter them down, or restructure (for example, process the rows in batches with child pipelines).

5. **You need a loop inside a loop in ADF. How?**

    ForEach can't be nested directly. Put the inner ForEach in a child pipeline and call it with Execute Pipeline.

6. **Schedule trigger vs tumbling window trigger: which for an hourly incremental load that must backfill the last 30 days?**

    Tumbling window. It supports backfill from a past start time, passes window start and end times, and has retries and dependencies.

7. **A file lands in `landing/suppliers/` at unpredictable times. How do you process it right away?**

    A storage event trigger on BlobCreated, filtered by path prefix `landing/suppliers/` and suffix `.csv`. It works through Event Grid, so the Event Grid resource provider must be registered.

8. **You edited a pipeline and saved it, but last night's scheduled run used the old logic. Why?**

    Saved changes aren't live until they are published. Triggers only run the published version. Debug runs use the saved version.

9. **Write the expression for today's folder path `orders/yyyy/MM/dd`, and show how to put a watermark variable inside a SQL string.**

    `@concat('orders/', formatDateTime(utcNow(), 'yyyy/MM/dd'))`. Inside a string: `... WHERE modified_at > '@{variables('oldWatermark')}'`.

10. **In a watermark pattern, why update the watermark only after the Copy succeeds?**

    If the copy fails, the watermark stays put, so the next run re-copies the same range and no data is lost.

11. **Your source is PostgreSQL. Can ADF's Stored Procedure activity update the watermark table there? Where should that table live?**

    No. The Stored Procedure activity only supports the SQL Server family. Keep control, watermark and log tables in a separate Azure SQL control database (or use the Script activity).

12. **What's a metadata-driven pipeline, and why use one?**

    A generic pipeline driven by a control table (Lookup → ForEach → parameterized Copy). A new table is a new row, not a new pipeline.

13. **Name the four dependency conditions and one use of Failed.**

    Succeeded, Failed, Completed, Skipped. A Failed path can use a Web activity to call a Logic App that sends a Teams or email alert.

14. **How should ADF authenticate to ADLS and Databricks?**

    With its managed identity, granted the right roles. Any remaining secrets go through a Key Vault linked service.

15. **Describe ADF CI/CD with Git.**

    Develop in feature branches and merge to the collaboration branch. Publishing generates ARM templates, which a CI/CD pipeline deploys to test and prod with parameter overrides, stopping and restarting triggers around the deployment.

16. **Mapping Data Flow vs a Databricks notebook: when would you pick each?**

    Data Flow for low-code, moderate transformations by non-coders. Databricks for complex, large-scale, code-based logic, Delta features, and more control over cost.

17. **Databricks: all-purpose cluster vs job cluster vs serverless?**

    Interactive and shared (pricier) / created per job run and cheaper, the production choice / no cluster management, starts fast, billed in DBUs only.

18. **What makes up the cost of classic Databricks compute?**

    DBUs (Databricks charge) plus the Azure VM cost.

19. **Unity Catalog: what's the three-level namespace, and what's an external location?**

    `catalog.schema.table`. An external location is a storage credential (such as an access connector's managed identity) plus an `abfss://` path that Unity Catalog governs.

20. **Managed vs external table: what happens on `DROP TABLE`?**

    A managed table's data is deleted. An external table's files stay.

21. **Why should a batch notebook read its date from `dbutils.widgets` instead of hard-coding it?**

    The same code then runs for any day, ADF or a job can pass the window date, and backfills and reruns need no code changes.

22. **How does Auto Loader run as a batch job, and how does it avoid reprocessing files?**

    With `trigger(availableNow=True)` it processes everything new, then stops. It records processed files in its checkpoint folder.

23. **What were Delta Live Tables and Databricks Workflows renamed to? What's the difference between a streaming table and a materialized view?**

    Lakeflow Declarative Pipelines and Lakeflow Jobs. A streaming table processes each new source row once (incremental). A materialized view is a stored query result that the pipeline keeps up to date.

24. **How do you pass a date from ADF into a Databricks notebook, and return a result?**

    Set `baseParameters` on the Notebook activity and read it with `dbutils.widgets.get`. Return with `dbutils.notebook.exit(...)` and read it in ADF as `output.runOutput`.

25. **Write the bronze → silver logic that makes reruns safe.**

    Deduplicate to the latest row per key, then `MERGE` into the silver Delta table (update when matched, insert when not).

26. **What does liquid clustering replace?**

    Manual partitioning + Z-ORDER. Its clustering keys can be changed later without rewriting the table.

27. **What are the main components of a Synapse workspace?**

    Dedicated SQL pool, serverless SQL pool, Spark pools, pipelines, and Synapse Link, all in Synapse Studio with a primary ADLS Gen2 account.

28. **How many distributions does a dedicated SQL pool table have, and what do DWUs change?**

    60\. More DWUs spread those distributions across more compute nodes, which gives more power.

29. **Choose a distribution for: a 3 TB fact_sales, a 50 MB dim_product, a staging table.**

    Hash (on a high-cardinality join key like `customer_key`), replicated, round robin (usually as a heap).

30. **Why is `order_date` a bad hash distribution column?**

    Rows for a date all land in one distribution, so loads and date-filtered queries hit a single distribution. That's skew and poor parallelism.

31. **What are data skew and data movement, and why do they hurt?**

    Skew means uneven rows per distribution, so the slowest one sets the pace. Data movement means shuffling rows between nodes to satisfy joins or aggregations, which adds time.

32. **What is a rowgroup, and why can too many partitions hurt a CCI table?**

    A batch of up to about 1 million rows compressed column by column. Each partition is split across 60 distributions, so over-partitioning leaves rowgroups far too small, which hurts compression and scan speed.

33. **What is partition switching used for?**

    Instantly swapping a whole partition into or out of a table as a metadata-only operation, for fast loads and for archiving old data.

34. **Why shouldn't Synapse's `COPY INTO` point at a Delta table folder? What do you do instead?**

    It reads plain files and ignores the Delta transaction log, so it would also load old or deleted file versions and create duplicates. Have Databricks write a Parquet export, load that into staging, then use CTAS/MERGE into the final table.

35. **How is serverless SQL billed, and how do you keep it cheap?**

    Per TB processed. Use Parquet/Delta, partition pruning, only the columns you need, and views over curated gold data.

36. **Serverless SQL fails to read a Gold Delta table that Databricks reads fine. Likely cause?**

    The table uses a newer Delta feature (such as deletion vectors) that Synapse's Delta reader doesn't support. Disable the feature on that table, or serve it through another engine.

37. **What does CETAS do?**

    `CREATE EXTERNAL TABLE AS SELECT` writes a query's results to the lake (Parquet) and creates an external table over them.

38. **Dedicated vs serverless SQL pool for a dashboard used by 300 people all day?**

    Dedicated (or Fabric Warehouse / Databricks SQL), for predictable performance and concurrency. Serverless suits exploration and lighter workloads.

39. **Is Synapse deprecated? What should a new project consider?**

    No. It's supported, with no announced retirement, but Microsoft Fabric is the strategic successor. New projects should evaluate Fabric or Databricks.

40. **Synapse Pipelines vs ADF?**

    The same engine and concepts, with small feature differences. Synapse Pipelines live inside the workspace. Pick one per solution.

41. **Draw the classic Azure batch architecture in one line.**

    ADF (ingest and orchestrate) → ADLS bronze → Databricks (silver/gold Delta) → Synapse or Databricks SQL (serve) → Power BI.

42. **Your nightly pipeline failed on 2026-09-23 and 24. How do you recover cleanly?**

    Fix the cause, then rerun the tumbling-window runs for those days (or repair the failed Databricks tasks). Idempotent MERGE/overwrite logic and window-based watermarks make the reruns safe.

43. **Where do data-quality checks belong, and what happens to bad rows?**

    Between layers, mainly bronze → silver, using expectations or tests. Bad rows go to a quarantine table and alerts fire, rather than failing silently or dropping data.

44. **ETL or ELT: which does this describe? "Raw data is loaded into the lake, then dbt models build the star schema in Databricks SQL."**

    ELT. Transformation happens after loading, inside the lakehouse.

45. ➕ **List five cost-control measures for this pipeline.**

    Job clusters or serverless instead of all-purpose clusters. Auto-termination and auto-pause. Pause the dedicated SQL pool off-hours. Incremental loads. Parquet/Delta with compaction to cut serverless scans. Fewer tiny ADF activity runs.

46. ➕ **Design the security for the end-to-end pipeline.**

    ADF's managed identity gets Blob Data Contributor on `bronze`, database users on the sources and the control database. Databricks uses an access connector, with Unity Catalog external locations and grants for silver/gold. Synapse's managed identity reads `gold`. Remaining secrets live in Key Vault (a linked service or a Key Vault–backed secret scope). Private endpoints (blob + dfs), a managed VNet for ADF and Synapse, VNet injection for Databricks, and group-based RBAC/ACLs for analysts.
