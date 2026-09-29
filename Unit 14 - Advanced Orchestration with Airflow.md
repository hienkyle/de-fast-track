# Unit 14 — Advanced Orchestration with Airflow: Study Guide

**Scope:** Airflow architecture and core concepts (DAGs, operators, tasks) · Running Airflow on an Azure VM or a managed service · Building dynamic and data-driven DAGs · Integrating Airflow with Azure services

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. In Unit 11, **ADF** ran the nightly load. Here the same flow (ingest → Databricks silver/gold → dbt → checks) is orchestrated by Airflow as **Python code**. The guide uses **Airflow 3** names. When Airflow 2 used a different name, it appears as *(Airflow 2: …)*, because a lot of course material and existing code still uses version 2.

---

## Part A — Airflow Architecture and Core Concepts

### 1. What Airflow is

**Apache Airflow** is an open-source platform to **author, schedule, and monitor workflows as Python code**. Like ADF, it's a **conductor**. It tells other systems (Databricks, ADF, SQL, dbt) what to run and when. It doesn't process big data itself.

**"Workflows as code"** means:

- DAGs live in **Git**, get reviewed in PRs, and are tested and deployed with CI/CD (Unit 1).
- Loops, config files and functions can **generate** pipelines (Part C).
- Any Python library works, plus hundreds of **provider packages** (Azure, Databricks, Snowflake, dbt…).

**Airflow vs orchestrators you've already seen:**

| | **Airflow** | **ADF** (Unit 11) | **Lakeflow Jobs** (Unit 11) |
|---|---|---|---|
| Authoring | Python code | Visual designer, stored as JSON | UI / YAML (Asset Bundles) |
| Scope | **Any system** (multi-cloud, on-prem, SaaS) | Azure-centric, 90+ connectors | Mostly inside Databricks |
| Data movement | None built in. It calls other tools. | **Copy activity** (a strength) | Lakeflow Connect |
| Hosting | You run it, or use a managed service | Serverless | Serverless / Databricks-managed |
| Best for | Complex dependencies across many tools, dynamic pipelines, engineering-heavy teams | Low-code ingestion and orchestration on Azure | Pipelines that stay inside Databricks |

A common hybrid is **Airflow on top**, triggering ADF for copies and Databricks for transforms.

⚠️ **Airflow isn't for streaming.** It schedules **batch** runs. For always-on streams, use Unit 12's tools.

### 2. Core concepts

| Concept | Meaning |
|---|---|
| **DAG** (Directed Acyclic Graph) | A workflow: tasks plus their dependencies, with no cycles. Defined in a `.py` file. |
| **Task** | One node in the DAG: a unit of work |
| **Operator** | A **template** for a task (`BashOperator`, `PythonOperator`, `AzureDataFactoryRunPipelineOperator`…). A task is an operator **instance** with arguments. |
| **Sensor** | A special operator that **waits** for something (a file, another DAG, a time) |
| **DAG run** | One execution of a DAG, for one **logical date** |
| **Task instance** | One execution of a task inside a DAG run (task + run + try number) |
| **Hook** | Low-level client for an external system (e.g. `WasbHook`). Operators use hooks internally. |
| **Connection** | Stored credentials and host info, referenced by a `conn_id` |
| **Variable** | A global key/value setting (configuration, never secrets) |
| **XCom** | "Cross-communication": small values passed between tasks |
| **Provider** | A package adding operators, hooks and sensors for one system (e.g. the Microsoft Azure provider) |

**Operator families:**

| Family | Examples | Does |
|---|---|---|
| **Action** | `BashOperator`, `PythonOperator` / `@task`, `SQLExecuteQueryOperator` | Runs something |
| **Transfer** | `LocalFilesystemToWasbOperator`, `SFTPToWasbOperator` | Moves **small** data from A to B. Use ADF Copy or Spark for big data. |
| **Sensor** | `WasbBlobSensor`, `ExternalTaskSensor`, `DateTimeSensor` | Waits for a condition |
| **Remote-job** | `DatabricksRunNowOperator`, `AzureDataFactoryRunPipelineOperator` | Starts a job elsewhere and tracks it until it finishes |

### 3. Architecture (Airflow 3)

```
          DAG files (.py) in a DAG bundle (folder or Git repo)
                     │
            ┌────────▼────────┐
            │  DAG processor  │  parses DAG files → stores serialized DAGs in the metadata DB
            └────────┬────────┘
                     │
 ┌───────────────────▼────────────────────┐        ┌──────────────┐
 │           Metadata database            │◄──────►│  Scheduler   │ decides what runs when,
 │ (PostgreSQL): DAG runs, task states,   │        │  + Executor  │ hands tasks to the executor
 │  connections, variables, XComs         │        └──────┬───────┘
 └───────────────────▲────────────────────┘               │ queues tasks
                     │                              ┌─────▼─────┐
            ┌────────┴────────┐  Task Execution API │  Workers  │ run task code
            │   API server    │◄────────────────────┤           │
            │ UI + REST API   │                     └───────────┘
            └─────────────────┘   ┌───────────┐
                                  │ Triggerer │ runs async waits for deferred tasks (Part C.7)
                                  └───────────┘
```

| Component | Role |
|---|---|
| **Scheduler** | Checks schedules and dependencies, creates DAG runs, and queues ready tasks. The heart of Airflow. |
| **Executor** | Runs inside the scheduler and decides **how and where** tasks run (below) |
| **Workers** | Processes, containers or pods that actually run the task code |
| **DAG processor** | Parses DAG files. A **separate, required component** in Airflow 3 (in Airflow 2 it ran inside the scheduler). |
| **API server** | Serves the **web UI**, the **REST API**, and the API that tasks use to report status *(Airflow 2: the "webserver")* |
| **Metadata DB** | The source of truth for all state. **PostgreSQL** in production. SQLite is for local testing only. |
| **Triggerer** | Runs an async loop for **deferred** tasks, so waiting doesn't occupy a worker slot |

**Airflow 3 security change:** task code **no longer connects to the metadata DB directly**. It talks to the API server instead, so a buggy or malicious task can't corrupt Airflow's state.

**Executors:**

| Executor | How tasks run | Use |
|---|---|---|
| **LocalExecutor** | Parallel processes on the scheduler's machine | **One VM** (Part B.1). Simple. |
| **CeleryExecutor** | A pool of worker machines pulling from a **queue** (Redis/RabbitMQ) | Scaling out across VMs |
| **KubernetesExecutor** | **One pod per task**, created and removed on demand | AKS: isolation, per-task resources, scales to zero |
| ➕ Multiple executors | Different executors for different tasks | Mixed workloads |

### 4. What a DAG looks like

**TaskFlow API** (decorators, the modern style). Return values flow between tasks, and that's what creates the dependencies:

```python
@dag(schedule=CronDataIntervalTimetable("0 1 * * *", timezone="Asia/Saigon"),   # interval timetable (Part A.5)
     start_date=datetime(2026, 9, 1), catchup=False)
def ecom_daily_orders():
    @task
    def extract(ds=None): return f"bronze/orders/{ds}/"   # ds = the day being processed (yesterday)
    @task
    def transform(path): ...
    transform(extract())          # extract >> transform, path passed via XCom
```

**Classic style:** create operator instances and link them with `>>`:

- `a >> b`: b runs after a
- `a >> [b, c] >> d`: fan out, then fan in
- `chain(a, b, c)`: a sequence

The two styles can be mixed in one DAG.

**Key DAG arguments:**

| Argument | Meaning |
|---|---|
| `dag_id` | Unique name |
| `schedule` | When it runs (Part A.5) *(Airflow 2: `schedule_interval`)* |
| `start_date` | Earliest date the DAG can have runs for. Must be **fixed**, never `now()`. |
| `catchup` | Whether to create runs for missed past intervals (Part A.6) |
| `default_args` | Settings applied to every task (owner, retries, retry_delay, callbacks) |
| `max_active_runs`, `tags`, `params` | Concurrency limit, UI filtering, run-time inputs |

### 5. Scheduling: logical date and data interval

| Term | Meaning |
|---|---|
| `schedule` | Cron string, preset (`@daily`, `@hourly`), a time interval, `None` (manual/API only), or a list of **Assets** (Part C.5) |
| **Logical date** | The date a run "represents" *(Airflow 2: `execution_date`)* |
| **Data interval** | `data_interval_start` → `data_interval_end`: the period of data the run should process |

⚠️ **The classic trap (interval-based schedules):** a daily run for **2026-09-24** covers 09-24 00:00 → 09-25 00:00, so it **starts after the interval ends**, on 09-25. The run labeled "24th" runs on the 25th. That's the right behavior for batch: you can't process a day before it's over.

➕ **Airflow 3 change:** plain cron schedules now default to a **trigger timetable** (`CronTriggerTimetable`). The run fires *at* the cron time and its logical date is that time, with no interval behind it. If your logic depends on `data_interval_start/end`, choose the interval timetable explicitly (`CronDataIntervalTimetable`, or set `create_cron_data_intervals = True` in the scheduler config) or compute the window yourself. **The examples in this guide use the interval timetable**, so a run that fires at 01:00 has `ds` = the previous day, the day it processes.

**Best practice:** each run processes **its own window** (`ds`, `data_interval_*`), never `now()`. Then any run, including a rerun or backfill, processes exactly its own period. It's the same idea as ADF's tumbling window (Unit 11).

### 6. Catchup, backfill, reruns

- **Catchup:** when the DAG is turned on, Airflow creates runs for **every missed interval** since `start_date`. **Off by default in Airflow 3** (on in Airflow 2, which surprised many people with hundreds of runs).
- **Backfill:** running a DAG for a past date range on purpose. In Airflow 3 the **scheduler** manages backfills, and you can start them from the UI.
- **Clear:** resetting a task instance's state makes the scheduler **rerun** it, optionally with its downstream tasks.
- All three are only safe if tasks are **idempotent** (Unit 11): MERGE or overwrite-by-partition, never a blind INSERT.

### 7. Task lifecycle, retries, trigger rules

**Task instance states:**

```
none → scheduled → queued → running → success
                                 ├──► failed ──(retries left)──► up_for_retry → scheduled…
                                 ├──► up_for_reschedule   (sensor in reschedule mode)
                                 └──► deferred            (waiting in the triggerer)
upstream_failed  (a parent failed)     skipped  (branching / short-circuit)
```

**Reliability settings** (per task, or for all tasks through `default_args`):

| Setting | Purpose |
|---|---|
| `retries`, `retry_delay`, exponential backoff | Ride out brief failures (timeouts, throttling) |
| `execution_timeout` | Kill a task that hangs |
| `on_failure_callback` / `on_success_callback` | Run a function, e.g. post to Teams |
| `max_active_runs` (DAG) | Limit concurrent runs of one DAG (e.g. 1 for loads that must run in order) |
| `max_active_tasks` (DAG), **pools** | Limit parallelism. A pool with 4 slots = at most 4 tasks hitting the Postgres source at once. |
| `depends_on_past` | A task runs only if the same task succeeded in the previous run |

**Trigger rules:** when a task runs, based on the results of its upstream tasks:

| Rule | Runs when upstream tasks… | Use |
|---|---|---|
| `all_success` *(default)* | all succeeded | Normal flow |
| `all_done` | all finished, whatever the result | Cleanup (drop temp tables, stop a cluster) |
| `one_failed` | at least one failed | A failure-alert task |
| `none_failed` | none failed (skipped is fine) | Continuing after an optional step |
| `none_failed_min_one_success` | none failed and at least one succeeded | The usual join after branching |
| `always` | always | Logging |

### 8. XCom: passing data between tasks

- A TaskFlow return value goes into XCom automatically, and passing it to another task reads it back.
- ⚠️ **XCom is for small metadata** (paths, IDs, counts, run IDs). It's stored in the metadata DB by default. **Never pass DataFrames.** Write data to ADLS and pass the **path**.
- ➕ A **custom XCom backend** (e.g. object storage) can store large values in ADLS/Blob automatically and keep only a reference in the DB.

### 9. Connections, variables, and secrets

- **Connection** = `conn_id` + type + host + login/password + extras. Operators take a `*_conn_id` argument.
- **Variables** hold configuration (environment name, a Logic App URL).
- Both can come from the UI/DB, **environment variables**, or a **secrets backend** such as Azure Key Vault (Part D.3). **In production, use the secrets backend.**
- ⚠️ Reading a Variable at the **top level** of a DAG file runs on every parse (Part C.6). Read it inside tasks or in templates.

### 10. Organizing DAGs ➕

- **TaskGroups** group tasks visually and logically. *SubDAGs are removed in Airflow 3.*
- **One DAG per business pipeline.** For dependencies between DAGs, prefer **Assets** (Part C.5) over `ExternalTaskSensor` or `TriggerDagRunOperator`.
- Repo layout: `dags/`, `include/` (SQL and config files), `plugins/`, `tests/`.
- **Testing:** CI checks that **every DAG imports without errors**. `dag.test()` runs a whole DAG in one process for debugging.

---

## Part B — Running Airflow on Azure

### 1. Option 1: self-hosted on an Azure VM (IaaS)

This is the "you manage the OS and runtime" model from Unit 9.

```
vnet-ecom-prod-sea
├── snet-airflow   vm-airflow-prod (Ubuntu)
│                   scheduler + DAG processor + API server + triggerer, LocalExecutor
│                   system-assigned managed identity
│                   NSG: no inbound UI/SSH from the Internet
├── snet-data      private endpoints: PostgreSQL flexible server (metadata DB), Key Vault, ADLS
└── AzureBastionSubnet   admin access + tunnel to the UI
```

**Design decisions:**

| Decision | Choice and reason |
|---|---|
| **Compute** | A VM in a **private subnet** with a **managed identity** (Unit 9 D.3). Admins connect through **Bastion**, not a public SSH port. |
| **Metadata DB** | **Azure Database for PostgreSQL – Flexible Server** (PaaS, Unit 10) with backups and a private endpoint. The state survives if the VM is lost. |
| **Installation** | Pin versions with Airflow's official **constraints file**, which lists dependency versions tested together, so installs are reproducible. |
| **Configuration** | `airflow.cfg` or environment variables. Key settings: DB connection, executor, secrets backend, remote logging. Keep the DB password in Key Vault (or use Entra auth for Postgres). |
| **Process management** | Run each component as an OS **service** that restarts automatically after a crash or reboot |
| **DAG deployment** | From Git: CI tests the DAGs and syncs them to the VM, or Airflow 3 reads a **Git DAG bundle** |
| **UI access** | Through Bastion/VPN, or behind **Application Gateway + Entra ID sign-in**. Never expose it publicly. Use a production auth manager (Airflow 3's built-in *simple auth manager* is for development). |
| **Logs** | Send task logs to Blob storage (Part D.6), since local disk fills up and disappears with the VM |

➕ **Quick learning setups:** Airflow's "standalone" mode (everything on one machine) and the official Docker Compose file. Neither is meant for production.

**Pros and cons of the VM:**

| ✅ | ❌ |
|---|---|
| Full control over version, plugins and packages | **You** patch the OS, upgrade Airflow, back up and monitor (shared responsibility, Unit 9) |
| Cheap for small workloads (one VM) | A single VM is a **single point of failure** unless you add zones, Celery workers and HA schedulers |
| Easy to understand | Scaling means adding Celery workers or moving to AKS |

### 2. Option 2: Kubernetes (AKS) ➕

- The **official Helm chart** deploys Airflow on **AKS** with the **KubernetesExecutor** (or Celery).
- Each task gets its own pod, with its own image and CPU/RAM, and the pod is removed afterwards. The cluster can autoscale.
- **Workload identity** gives pods an Entra identity with no secrets.
- More power, but much more to operate. Choose it for large platforms with a Kubernetes team.

### 3. Option 3: managed Airflow

| Service | Status and notes |
|---|---|
| **ADF Workflow Orchestration Manager** (formerly *Managed Airflow in ADF*) | ⚠️ **Deprecated.** Microsoft points customers to Fabric's Apache Airflow job and publishes a migration guide. Expect it in older material, not in new builds. |
| **Apache Airflow job in Microsoft Fabric** | Microsoft's current managed Airflow. You bring DAGs (or sync from Git), and Fabric runs the environment. It fits the Fabric direction from Units 11–12. |
| ➕ **Astronomer (Astro)** | Commercial managed Airflow from the main Airflow company. It can run on Azure. |

**Self-hosted vs managed:**

| | **Self-hosted VM / AKS** | **Managed** |
|---|---|---|
| Ops work | High: patching, upgrades, HA, DB backups | Low: the provider does it |
| Control | Full | Limited to what the service exposes (versions, packages, networking) |
| Cost | VMs + DB (cheap when small, plus staff time) | Service pricing, less staff time |
| Pick when | Special networking or packages, learning, tight budgets | Production without an Airflow ops team |

---

## Part C — Dynamic and Data-Driven DAGs

"Dynamic" means the DAG's shape or parameters come from **data or config** instead of being hard-coded.

| Level | Decided when | Tool |
|---|---|---|
| **Templated values** | At run time, per run | Jinja templates, params |
| **Dynamic tasks** | At run time, from upstream output | **Dynamic task mapping** |
| **Dynamic DAGs** | At **parse** time, from config | DAG factory |
| **Data-driven scheduling** | When data changes | **Assets**, sensors |

### 1. Jinja templating

Operator fields listed in its `template_fields` are **rendered per run**. For example, a path like `bronze/orders/{{ ds }}/` becomes `bronze/orders/2026-09-24/` for that run.

| Template variable | Value |
|---|---|
| `{{ ds }}` / `{{ ds_nodash }}` | Logical date `2026-09-24` / `20260924` |
| `{{ data_interval_start }}` / `{{ data_interval_end }}` | The run's window |
| `{{ logical_date }}` | Logical date as a datetime *(Airflow 2: `execution_date`)* |
| `{{ params.x }}` | A DAG parameter |
| `{{ var.value.env }}`, `{{ conn.my_conn.host }}` | Variables and connection fields |
| `{{ run_id }}`, `{{ ti.try_number }}` | Run metadata |
| `{{ macros.ds_add(ds, -1) }}` | Date math |

SQL kept in `.sql` files is templated too.

### 2. Params

**Params** are run-time inputs declared on the DAG (e.g. `tables: array`, `full_refresh: boolean`), each with a type and a default. When you trigger the DAG manually, the UI shows a **form**, and the values are **validated** against a JSON schema. Tasks read them as `{{ params.full_refresh }}` or from the task context.

### 3. Dynamic task mapping (run-time fan-out)

Creates **N copies of a task at run time**, one per item from an upstream result. It's Airflow's version of ADF's **Lookup → ForEach** (Unit 11).

```python
tables = get_tables()                    # task returning e.g. [{"table": "orders"}, {"table": "payments"}]
results = copy_table.expand(cfg=tables)  # one copy_table instance per table, at run time
summarize(results)                       # receives all outputs as a list
```

| Method | Meaning |
|---|---|
| `.expand(x=[...])` | One task instance per element (several mapped arguments → cross product) |
| `.partial(a=1).expand(x=...)` | Fixed arguments + mapped arguments |
| `.expand_kwargs([{...}, {...}])` | Each dict gives one instance's full argument set |

- Each mapped instance has its own state, retries and logs (`copy_table[0]`, `[1]`…).
- Limit parallelism per task (`max_active_tis_per_dag`) to protect the source.
- There's a cap on instances (`max_map_length`, default 1024).
- ✅ Adding a table = adding a **row in the control table**. No code change.

### 4. DAG factory (parse-time generation)

A single DAG file **loops over a config file** (e.g. a YAML list of sources, each with its own schedule and ADF pipeline name) and creates **one DAG per entry**. Each generated DAG has its own schedule, owner, run history and UI page.

**Mapping vs factory:**

| | **Dynamic task mapping** | **DAG factory** |
|---|---|---|
| Decided | At **run time**, from task output | At **parse time**, from a file |
| Result | Many tasks in **one** DAG run | Many **separate DAGs** |
| Use | Same schedule, list changes per run | Different schedules or owners, list changes rarely |

### 5. Data-driven scheduling with Assets

*(Airflow 2.4–2.10: "Datasets")*

Instead of running on a clock, a DAG runs **when the data it needs has been updated**.

```python
silver_orders = Asset("abfss://silver@stecomlakeprod.dfs.core.windows.net/orders")

@task(outlets=[silver_orders])                   # producer: success = "silver_orders updated"
def merge_orders(): ...

@dag(schedule=[silver_orders], ...)              # consumer: runs after each update
def build_gold(): ...
```

- `schedule=[a, b]` runs when **both** have been updated since the last run. Use `a | b` for **either**, and combine with `&` / `|`.
- An asset is just a **URI label**. Airflow doesn't look at the files. It only records that the producing task **succeeded**.
- ✅ It replaces fragile setups like "gold runs at 02:00 and hopes silver has finished", and it splits a giant DAG into **smaller DAGs owned by different teams**.
- ➕ The UI's **Assets view** shows lineage between producers and consumers.
- ➕ **Event-driven scheduling (Airflow 3):** an **AssetWatcher** can trigger a DAG from an external message queue. Check which queues your provider versions support.

### 6. Parse-time rules ⚠️

The DAG processor **re-parses every DAG file about every 30 seconds**. Code at the **top level** of a DAG file runs on every parse.

- ❌ No database queries, API calls, Variable reads or heavy imports at the top level. They slow parsing and can overload the source.
- ✅ Read a **local config file** (fast, as in the DAG factory), or put the work inside tasks (with mapping).
- The DAG structure must be **deterministic**: the same file must always produce the same DAG. No random values or `now()` in IDs or structure.

### 7. Sensors and deferrable operators

**Sensors** wait for a condition, e.g. `WasbPrefixSensor` waits for a file under `landing/suppliers/{{ ds }}/`. Key settings: `poke_interval` (how often to check), `timeout` (when to give up), `soft_fail` (mark the task *skipped* instead of *failed* on timeout).

| Mode | Behavior | Cost |
|---|---|---|
| `poke` *(default)* | Holds a worker slot and sleeps between checks | Wastes a slot for hours |
| `reschedule` | Releases the slot, and the scheduler schedules the next check | Better for long waits |
| **Deferrable** | Hands the wait to the **triggerer** (async), using no worker at all | Best: thousands of waits on one process |

Many Azure and Databricks operators support **deferrable mode**. For example, the ADF operator can start a pipeline and then wait in the triggerer until it finishes.

### 8. Branching and conditional logic

- **`@task.branch`** returns the `task_id`(s) to follow, and the others are **skipped**. Example: a full reload on the 1st of the month, incremental loads on other days. Join the branches with `trigger_rule="none_failed_min_one_success"`.
- **`@task.short_circuit`** skips everything downstream when it returns `False`, e.g. "no new files today".

---

## Part D — Integrating Airflow with Azure Services

### 1. The Azure providers

The **Microsoft Azure provider** and the **Databricks provider** add:

| Azure service | Airflow building blocks |
|---|---|
| **Blob / ADLS Gen2** | `WasbHook`, `AzureDataLakeStorageV2Hook`, `WasbBlobSensor`, `WasbPrefixSensor`, transfer operators (local/SFTP → Blob) |
| **Data Factory** | `AzureDataFactoryRunPipelineOperator`, `AzureDataFactoryPipelineRunStatusSensor` |
| **Databricks** | `DatabricksRunNowOperator` (run an existing job), `DatabricksSubmitRunOperator` (one-off run), `DatabricksWorkflowTaskGroup`, SQL operators |
| **Synapse** | `AzureSynapseRunPipelineOperator`, `AzureSynapseRunSparkBatchOperator` |
| **Azure SQL / PostgreSQL** | `SQLExecuteQueryOperator` with an `mssql` / `postgres` connection |
| **Cosmos DB** | Insert-document operator, document sensor |
| **Container Instances** | Run any container as a task |
| **Service Bus** | Send/receive messages, manage queues |
| **Key Vault** | `AzureKeyVaultBackend` (secrets backend) |
| **Blob logs** | Remote task logging to WASB |

### 2. Authentication: managed identity first

The same rule as Unit 9: **no secrets when running inside Azure.**

- Give the Airflow VM (or AKS pods, through workload identity) a **managed identity**.
- Grant it **data-plane roles**: *Storage Blob Data Contributor* on the needed containers, *Data Factory Contributor* on the factory, access to the Databricks workspace, *Key Vault Secrets User*.
- Create Azure connections **without a client secret**. The Azure hooks then fall back to **`DefaultAzureCredential`** (managed identity, then CLI login…). For a user-assigned identity, put its client ID in the connection's extras.
- For Databricks, prefer Entra-based auth (a service principal or managed identity) over personal access tokens.

### 3. Azure Key Vault as the secrets backend

| Setting | Value / meaning |
|---|---|
| Backend | `AzureKeyVaultBackend` from the Azure provider, set in the `[secrets]` section of the config |
| `vault_url` | e.g. `https://kv-ecom-prod.vault.azure.net/` |
| `connections_prefix` / `variables_prefix` | e.g. `airflow-connections` / `airflow-variables` |
| Secret naming | Connection `pg_orders` → secret **`airflow-connections-pg-orders`**. Key Vault names allow only letters, digits and dashes, so dashes are the separators. The value is the connection URI or JSON. |
| Auth to the vault | The VM's managed identity |

**Lookup order:** secrets backend → environment variables → metadata DB. Secrets never sit in the Airflow DB, the UI or Git.

### 4. Orchestrating ADF, Databricks and dbt

Unit 11's nightly pipeline, with Airflow as the orchestrator:

```
DAG ecom_nightly   (CronDataIntervalTimetable, daily 01:00 → ds = previous day, catchup off, max_active_runs=1, retries=2, failure callback → Teams)

  adf_ingest ─────────────┐   AzureDataFactoryRunPipelineOperator, pl_ingest, run_date={{ ds }}, deferrable
                          ├──► dbx_silver_gold ──► dbt_marts ──► freshness_check
  wait_supplier_file ─────┘    DatabricksRunNowOperator      dbt build      max(order_ts) recent enough?
  WasbPrefixSensor,             job_parameters run_date={{ ds }}              (raise → task fails → alert)
  reschedule, soft_fail         trigger_rule=none_failed
                                outlets=[gold_sales] → triggers asset-scheduled DAGs
```

The one line that wires it together:

```python
[adf_ingest, wait_supplier_file] >> dbx_silver_gold >> dbt_marts >> freshness_check
```

**Why each choice matters:**

| Choice | Reason |
|---|---|
| ADF does the copy, not a Python task | ADF's Copy activity handles large parallel transfers (and on-prem through a SHIR). Python on the Airflow VM would be slow and overload the orchestrator. |
| `run_date={{ ds }}` passed to ADF and Databricks | Each run processes its own day. The notebook reads it with `dbutils.widgets.get` (Unit 11). Reruns and backfills just work. |
| `deferrable` ADF operator | Long ADF runs wait in the triggerer, not in a worker slot |
| Sensor with `soft_fail` + downstream `none_failed` | A late supplier file doesn't block the whole night's load |
| `outlets=[gold_sales]` | Downstream teams' DAGs run as soon as gold is fresh (Assets) |
| Freshness check at the end | Catches the "succeeded but loaded nothing" case (Unit 13) |

**Who does what:** ADF **moves** data, Databricks **transforms** it, dbt **models** it, and Airflow **coordinates** everything, with retries, backfill and one view.

➕ **dbt in Airflow:** the simple option is one task that runs `dbt build`. The **Cosmos** library (by Astronomer) turns each dbt model into its own Airflow task, so you can retry a single model.

### 5. Triggering in both directions ➕

- **Airflow → Azure:** the operators above, or an HTTP operator for any REST API (e.g. a Logic App).
- **Azure → Airflow:** an **Azure Function** reacting to Event Grid (e.g. a blob landed) calls the Airflow **REST API** to start a DAG run or update an Asset. The stable API is `/api/v2` in Airflow 3 (`/api/v1` in Airflow 2).

### 6. Logging, monitoring and alerting on Azure

| Need | How |
|---|---|
| **Task logs that survive the VM** | **Remote logging to a Blob container** through a WASB connection |
| **Failure alerts** | An `on_failure_callback` that posts the DAG, task, run ID and log link to a **Logic App → Teams** (the Unit 13 pattern). Airflow's alerts complement Azure Monitor alerts. |
| **Metrics** | Airflow emits **StatsD** or ➕ **OpenTelemetry** metrics (scheduler heartbeat, task failures, DAG durations, pool usage). Send them to Azure Monitor or Managed Grafana (Unit 13). |
| **VM and DB health** | Azure Monitor Agent + alerts on the VM (CPU, disk, process down) and on the Postgres server |
| **Scheduler liveness** | Airflow's **health endpoint** reports the scheduler heartbeat. Alert if it's stale. |
| **SLAs** | Airflow 2's `sla=` was removed in Airflow 3. Newer 3.x versions add **deadline alerts**. A data-freshness check (Unit 13) works in every version. |

### 7. CI/CD for DAGs ➕

1. PR → CI runs lint, the **DAG import test**, unit tests on helper code, and optionally a test run against dev.
2. Merge → deploy to **dev**, then **prod** (sync to the VM, build a new image for AKS, or Git sync in Fabric's Airflow job).
3. Keep environment differences (connection IDs, factory names) in **Variables and Key Vault**, not in code.

---

## Part E — Best-Practices Checklist

- [ ] Airflow **orchestrates**. Heavy processing runs in Databricks, ADF, SQL or dbt, not in Python tasks on the Airflow machine.
- [ ] Every task is **idempotent** and processes **its run's window** (`ds`, `data_interval_*`), never `now()`
- [ ] Fixed `start_date`, explicit `catchup`, and `max_active_runs` where order matters
- [ ] **Retries** + `execution_timeout` on every task. **Pools** to protect source systems.
- [ ] XCom holds only small values. Data lives in ADLS.
- [ ] No heavy top-level code in DAG files
- [ ] Sensors in `reschedule` mode, or **deferrable** operators
- [ ] **Managed identity + Key Vault secrets backend**. No passwords in connections, DAGs or Git.
- [ ] Metadata DB on managed **PostgreSQL** with backups. Remote logs in Blob.
- [ ] UI never publicly exposed. A real auth manager and roles in prod.
- [ ] DAGs in Git with import tests in CI
- [ ] Failure callbacks + platform alerts + data-freshness checks

---

## Key Terms Cheat Sheet

- **Airflow:** open-source workflow orchestration, pipelines as Python code, batch only
- **DAG / task / operator / sensor / hook / provider**
- **DAG run / task instance / try number**
- **Scheduler, executor, workers, DAG processor, API server, triggerer, metadata DB** *(Airflow 2: webserver; DAG parsing inside the scheduler)*
- **LocalExecutor / CeleryExecutor / KubernetesExecutor**
- **TaskFlow API:** `@dag`, `@task`, return values → XCom
- **`>>`, `chain()`**: dependencies
- **`schedule`:** cron, preset, interval, `None`, Assets *(Airflow 2: `schedule_interval`)*
- **Logical date, data interval** *(Airflow 2: `execution_date`)*. The run for interval X starts when X ends.
- ➕ **Trigger timetable:** Airflow 3's default for cron (no data interval). Use `CronDataIntervalTimetable` when each run should process the previous interval.
- **Catchup** (off by default in Airflow 3), **backfill**, **clear**
- **Task states:** scheduled, queued, running, success, failed, up_for_retry, up_for_reschedule, deferred, upstream_failed, skipped
- **Retries, retry_delay, execution_timeout, callbacks, max_active_runs, pools, depends_on_past**
- **Trigger rules:** all_success (default), all_done, one_failed, none_failed, none_failed_min_one_success, always
- **XCom:** small values between tasks. ➕ Object-storage XCom backend for large ones.
- **Connections, variables, secrets backend** (Key Vault → env vars → DB)
- **TaskGroup** (SubDAGs removed)
- **Jinja templating:** `{{ ds }}`, `{{ data_interval_start }}`, `{{ params.x }}`, `{{ var.value.x }}`, `template_fields`
- **Params:** validated trigger-time inputs with a UI form
- **Dynamic task mapping:** `.expand()`, `.partial()`, `.expand_kwargs()`, run-time fan-out
- **DAG factory:** generate DAGs from config at parse time
- **Assets** *(Airflow 2: Datasets)*: `outlets` = producer, `schedule=[asset]` = consumer, `&` / `|`. ➕ AssetWatcher for event-driven runs.
- **Parse-time rules:** top-level code runs about every 30 s. Keep it light and deterministic.
- **Sensor modes:** poke / reschedule. **Deferrable operators** + triggerer. `soft_fail`.
- **`@task.branch`, `@task.short_circuit`**
- **Azure VM hosting:** managed identity, PostgreSQL flexible server, constraints file, auto-restarting services, Bastion, no public UI
- ➕ **AKS + Helm chart + KubernetesExecutor + workload identity**
- **Managed Airflow:** ADF Workflow Orchestration Manager (**deprecated**) → **Apache Airflow job in Microsoft Fabric**. ➕ Astronomer.
- **Azure provider:** WASB/ADLS hooks and sensors, ADF, Synapse, Cosmos DB, Container Instances, Service Bus, Key Vault backend
- **`DatabricksRunNowOperator` / `DatabricksSubmitRunOperator`**
- **Remote logging to Blob, StatsD/OTel metrics, `on_failure_callback` → Logic App → Teams**
- **REST API** `/api/v2` *(Airflow 2: `/api/v1`)*

---

## Practice Questions

1. **What does Airflow do, and what shouldn't it do?**

    It schedules and orchestrates batch workflows defined in Python, with retries, dependencies and monitoring. It shouldn't do heavy data processing itself or run streaming. It should trigger Spark, ADF, SQL or dbt.

2. **Airflow vs ADF: give two strengths of each.**

    Airflow: code-based pipelines (Git, tests, dynamic generation) and orchestration across any system or cloud. ADF: serverless with no ops work, and a built-in Copy activity with connectors and a SHIR for on-prem sources.

3. **Operator vs task vs task instance?**

    An operator is a template class. A task is an operator instance with arguments inside a DAG. A task instance is one execution of that task in a specific DAG run.

4. **Name the Airflow 3 components and one job of each.**

    Scheduler: creates runs and queues ready tasks. Executor: decides where tasks run. Workers: run task code. DAG processor: parses DAG files. API server: UI, REST API and task communication. Triggerer: async waits for deferred tasks. Metadata DB: stores all state.

5. **Which executor for: (a) one VM, (b) several worker VMs, (c) AKS with per-task resources?**

    (a) Local, (b) Celery, (c) Kubernetes.

6. **Why use PostgreSQL rather than SQLite as the metadata DB in production?**

    SQLite doesn't support concurrent access, so it can't run tasks in parallel. PostgreSQL handles many concurrent connections and, as a managed service, gives backups and survives VM loss.

7. **A daily DAG has `start_date=2026-09-01` and an interval-based schedule. When does the run for 2026-09-01 start, and why?**

    Right after 2026-09-02 00:00. It processes the full interval 09-01 → 09-02, and that interval has to finish first.

8. **Why is `start_date=now()` a bad idea?**

    The start date moves on every parse, so the scheduler may never find an interval to run. Use a fixed date.

9. **What does catchup do? What's the default in Airflow 3?**

    It creates runs for every missed interval since `start_date`. It's off by default in Airflow 3 (on in Airflow 2).

10. **What makes a backfill of last month safe?**

    Tasks are idempotent and process only their own window (`ds` / data interval), using MERGE or overwrite-by-partition.

11. **What does `max_active_runs=1` protect against?**

    Two runs of the same DAG overlapping, e.g. a slow run still loading when the next one starts, or backfill days running out of order.

12. **You want at most 3 tasks across all DAGs querying the production Postgres at once. How?**

    Create a pool with 3 slots and assign those tasks to it.

13. **A cleanup task must run whether upstream tasks succeed or fail. Which trigger rule?**

    `all_done`.

14. **After branching, the join task never runs. Why, and how do you fix it?**

    With the default `all_success`, the skipped branch makes the join skip too. Use `none_failed_min_one_success`.

15. **Why shouldn't a task return a 2 GB DataFrame?**

    XCom stores values in the metadata DB and is meant for small values. Write the data to ADLS and return the path (or use an object-storage XCom backend).

16. **Where can Airflow read a connection from, and in what order?**

    The secrets backend (e.g. Key Vault), then environment variables, then the metadata DB.

17. **What do `{{ ds_nodash }}` and `{{ data_interval_start }}` give you?**

    The run's logical date without dashes (`20260924`), and the start of the run's data window.

18. **Dynamic task mapping vs DAG factory?**

    Mapping creates task copies at run time from upstream output, all inside one DAG run. A factory creates separate DAGs at parse time from a config file.

19. **How would you rebuild ADF's Lookup → ForEach → Copy pattern in Airflow?**

    A task that reads the control table and returns a list, then `.expand()` over that list with a copy task, with a per-task parallelism limit.

20. **Your DAG file queries the source database at the top level to list tables. What goes wrong?**

    The file is re-parsed about every 30 seconds, so the query runs constantly, slows parsing and loads the source. Move it into a task (with mapping) or into a config file.

21. **What are Assets, and what problem do they solve?**

    URI labels for data that producer tasks mark as updated (`outlets`). Consumer DAGs scheduled on those assets run after the update. This replaces guessing with time schedules and splits big DAGs into smaller ones linked by data.

22. **Does Airflow check that an asset's files actually changed?**

    No. It only records that the producing task succeeded.

23. **A sensor waits up to 6 hours for a file. Why use reschedule mode or a deferrable operator?**

    In poke mode the sensor holds a worker slot the whole time. Reschedule frees the slot between checks. Deferrable moves the wait to the triggerer, using no worker at all.

24. **What does `soft_fail` do on a sensor that times out?**

    The sensor is marked skipped instead of failed.

25. **Describe a secure Airflow setup on an Azure VM.**

    A VM in a private subnet with a managed identity. The metadata DB on PostgreSQL flexible server with a private endpoint. Components run as auto-restarting services. Admin access through Bastion and no public UI (or App Gateway with Entra sign-in). Key Vault as the secrets backend. Remote logs in Blob. An NSG blocking inbound traffic from the internet.

26. **Why install Airflow with a constraints file?**

    Airflow has many dependencies. The constraints file pins versions that are tested together, so installs are reproducible and don't break.

27. **What happened to ADF's managed Airflow, and what replaces it?**

    ADF Workflow Orchestration Manager is deprecated. Microsoft recommends migrating to the Apache Airflow job in Microsoft Fabric.

28. **Self-hosted or managed Airflow for a small team with no Kubernetes or Linux ops skills?**

    Managed (Fabric Airflow job or Astronomer). It removes patching, upgrades, HA and DB backups.

29. **How should Airflow on an Azure VM authenticate to ADLS and ADF?**

    With the VM's managed identity (Azure connections without a secret fall back to `DefaultAzureCredential`), granted Storage Blob Data roles and Data Factory Contributor.

30. **With `connections_prefix="airflow-connections"`, which Key Vault secret holds connection `pg_orders`?**

    `airflow-connections-pg-orders`, with the connection URI or JSON as its value.

31. **In the nightly DAG, why does ADF do the copy instead of a Python task?**

    ADF's Copy activity is built for large, parallel transfers (and on-prem through a SHIR). Python on the Airflow VM would be slow and would overload the orchestrator.

32. **How does the run date reach the Databricks notebook?**

    Airflow passes `run_date={{ ds }}` as a job parameter, and the notebook reads it with `dbutils.widgets.get("run_date")`.

33. **Why make the ADF operator deferrable?**

    ADF pipelines can run for a long time. Deferring moves the wait into the triggerer, so no worker slot is held.

34. **How do Airflow task failures reach a Teams channel?**

    An `on_failure_callback` posts the DAG, task, run ID and log link to a Logic App, which posts to Teams (the Unit 13 pattern).

35. **Task logs vanish when the VM is replaced. Fix?**

    Turn on remote logging to a Blob container.

36. **Name three Airflow 2 → 3 renames or removals.**

    Any three of: `schedule_interval` → `schedule`, `execution_date` → `logical_date`, Datasets → Assets, webserver → API server, SubDAGs removed, `sla=` removed, `/api/v1` → `/api/v2`, DAG processor split out as its own component.

37. ➕ **How can an Azure event (a blob landing) start an Airflow DAG right away?**

    Event Grid → Azure Function → Airflow REST API (start a DAG run or update an Asset). A sensor also works, but it polls instead of reacting instantly.

38. ➕ **What should CI check before a DAG change reaches production?**

    That every DAG imports without errors, plus lint, unit tests for helper code, and optionally a test run against dev.
