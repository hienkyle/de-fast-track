# Unit 13 — Monitoring & Operations on Azure: Study Guide

**Scope:** Azure Monitor (logging, metrics, and diagnostics) · Alerts and action groups for pipeline failures · Cost management and optimization best practices · Lecture: pipeline failure zones

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus or the lecture notes. Everything else comes from the syllabus or your lecture notes.

The examples continue the e-commerce platform. Units 11 and 12 built the pipelines: nightly **ADF** loads, **Databricks** medallion jobs, **Event Hubs → Stream Analytics / Structured Streaming**. This unit covers keeping them **running, visible, and affordable**. The goals are to know when the nightly load fails before the business does, to find out why it failed, and to stop paying for clusters nobody uses.

---

## Part A — Azure Monitor (Logging, Metrics, and Diagnostics)

### 1. What it is

**Azure Monitor** is Azure's built-in platform for **collecting, analyzing, and acting on** telemetry from every Azure resource, application, and VM. You don't install it. Every resource already sends some data to it.

```
 Sources                          Azure Monitor data platform                 Use it
 ───────                          ───────────────────────────                 ──────
 Subscription (Activity log) ─┐   ┌─ Metrics  (numeric time series, fast) ─┐   Metrics explorer, charts
 Resources (ADF, Databricks,  ├─► │                                        ├─► Log Analytics (KQL queries)
   Event Hubs, ASA, Storage)  │   └─ Logs     (records in a Log Analytics  │   Workbooks / dashboards / Grafana
 Apps (App Insights SDK/OTel) ┤        workspace, queried with KQL)      ─┘   Alerts → action groups
 VMs (Azure Monitor Agent)  ──┘                                                Autoscale, exports
```

### 2. The three pillars

| Pillar | What it is | Example | Where it lives |
|---|---|---|---|
| **Metrics** | Numbers sampled over time, lightweight, near real time | `PipelineFailedRuns`, Event Hubs `ThrottledRequests`, ASA `Watermark delay`, storage `UsedCapacity` | Azure Monitor Metrics DB. **Platform metrics are collected automatically** and kept for **93 days**. |
| **Logs** | Structured records with many columns, suited to deep investigation | Every ADF activity run with its error message | **Log Analytics workspace**, queried with **KQL** |
| **Traces** / ➕ | Request flows across services (distributed tracing) | A FastAPI order endpoint (Unit 4) calling SQL | **Application Insights** (stored in a workspace) |

**Rule of thumb:** use **metrics to detect** (cheap, fast, good for alerting) and **logs to diagnose** (rich detail, the *why*).

### 3. Types of logs

| Log | Level | What it records | Collected by default? |
|---|---|---|---|
| **Activity log** | Subscription (control plane) | *Who did what to which resource*: create, delete, config changes, role assignments. Also Service Health events. | **Yes**, kept 90 days. Send it to a workspace to keep it longer or query it with KQL. |
| **Resource logs** (formerly "diagnostic logs") | Each resource (data plane) | What happened **inside** the resource: ADF pipeline/activity/trigger runs, Databricks job and cluster events, Event Hubs operations, ASA execution errors | **No. You must create a diagnostic setting.** |
| ➕ **Microsoft Entra logs** | Tenant | Sign-ins, audit | Via Entra diagnostic settings |

### 4. Diagnostic settings (the key "turn it on" step)

A **diagnostic setting** on a resource says **which log categories and metrics** to send **where**. Without one, resource logs are simply not kept.

**Destinations:**

| Destination | Use for |
|---|---|
| **Log Analytics workspace** | Querying with KQL, log alerts, workbooks. **The usual choice.** |
| **Storage account** | Cheap long-term archive or audit |
| **Event Hubs** | Streaming logs to an external SIEM, Splunk, or your own consumer (Unit 12) |
| Partner solutions | Datadog, Elastic, and similar |

A resource can have up to **5** diagnostic settings, for example one to Log Analytics and one to storage.

**ADF example:** Data factory → *Diagnostic settings* → *Add* → select `Pipeline runs log`, `Activity runs log`, `Trigger runs log` → send to `log-ecom-prod` → destination table **Resource specific**.

- **Resource-specific mode** (recommended) writes to dedicated tables: `ADFPipelineRun`, `ADFActivityRun`, `ADFTriggerRun`.
- **Azure diagnostics mode** (legacy) writes everything into one wide `AzureDiagnostics` table.

⚠️ **Why this matters for ADF:** ADF Studio's *Monitor* tab keeps run history for only **45 days**. To keep it longer, query it, or alert on it with logs, you need a diagnostic setting.

**Enforce it at scale:** use ➕ **Azure Policy** (the built-in "Deploy diagnostic settings for X to Log Analytics" policies) or declare it in **Bicep** (Unit 9), so no new resource slips through without logging.

```bicep
resource adfDiag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'to-log-analytics'
  scope: dataFactory
  properties: {
    workspaceId: logWorkspace.id
    logAnalyticsDestinationType: 'Dedicated'          // resource-specific tables
    logs: [
      { category: 'PipelineRuns', enabled: true }
      { category: 'ActivityRuns', enabled: true }
      { category: 'TriggerRuns',  enabled: true }
    ]
    metrics: [ { category: 'AllMetrics', enabled: true } ]
  }
}
```

### 5. Log Analytics workspace

- The **storage and query engine** for Azure Monitor Logs. Data lands in **tables**.
- **Design:** usually one central workspace per environment (`log-ecom-dev`, `log-ecom-prod`), in the same region as the resources. Split workspaces only for data sovereignty, access boundaries, or billing reasons.
- **Access:** workspace-level RBAC (*Log Analytics Reader / Contributor*) or **resource-context** access, where users see only logs from resources they can access.
- **Table plans** (a cost lever, see Part C):

| Plan | Cost | Query | Use for |
|---|---|---|---|
| **Analytics** | Highest ingestion price | Full KQL, alerts, fast | Pipeline run logs, anything you alert on |
| **Basic** | Much cheaper ingestion, pay per query | Limited KQL, 30-day interactive | High-volume debug logs you rarely search |
| **Auxiliary** | Cheapest | Slower, limited | Verbose or audit logs kept "just in case" |

- **Retention:** *interactive* retention (queryable, default 30 days, about the first month included in the price) plus *long-term* retention (archived cheaply, up to 12 years, restored or searched when needed). Set it **per table**.

### 6. KQL essentials

KQL (Kusto Query Language) reads top to bottom, with each `|` passing results to the next step, like the DataFrame chains in Spark.

```kusto
// Failed ADF pipeline runs in the last 24 hours
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project TimeGenerated, PipelineName, RunId, FailureType
| order by TimeGenerated desc
```

```kusto
// Which activity failed, and why
ADFActivityRun
| where TimeGenerated > ago(24h) and Status == "Failed"
| project TimeGenerated, PipelineName, ActivityName, ActivityType, ErrorCode, ErrorMessage, PipelineRunId
```

```kusto
// Pipeline duration trend: is the nightly load getting slower?
ADFPipelineRun
| where Status == "Succeeded" and PipelineName == "pl_master_nightly"
| extend duration_min = datetime_diff('minute', End, Start)
| summarize avg(duration_min), max(duration_min) by bin(TimeGenerated, 1d)
| render timechart
```

| SQL | KQL |
|---|---|
| `SELECT a, b` | `project a, b` |
| `WHERE` | `where` |
| `GROUP BY` + aggregate | `summarize count() by x` |
| `ORDER BY ... DESC` | `order by ... desc` / `sort by` |
| `LIMIT 10` | `take 10` / `top 10 by x` |
| Computed column | `extend` |
| `DATE_TRUNC` | `bin(TimeGenerated, 1h)` |

### 7. What to monitor per service (the data platform)

| Service | Key metrics | Key logs (tables) |
|---|---|---|
| **ADF** | `PipelineFailedRuns`, `ActivityFailedRuns`, `TriggerFailedRuns`, integration runtime CPU/queue | `ADFPipelineRun`, `ADFActivityRun`, `ADFTriggerRun` |
| **Databricks** | Job run status and duration (➕ **system tables** `system.lakeflow.*`) | Diagnostic logs: `DatabricksJobs`, `DatabricksClusters`, `DatabricksAccounts`, and more (Premium plan) |
| **Synapse** | Dedicated pool DWU %, failed pipeline runs | `SynapseIntegrationPipelineRuns`, SQL request logs |
| **Event Hubs** | Incoming/outgoing messages, **ThrottledRequests**, ServerErrors | Operational logs, ➕ consumer lag via SDK or Kafka tools |
| **Stream Analytics** | **Watermark delay**, **SU % utilization**, backlogged input events, runtime and data conversion errors | Execution logs |
| **Storage / ADLS** | Availability, E2E latency, transactions, **UsedCapacity** | `StorageBlobLogs` (who read or wrote what) |
| **Azure SQL** | DTU/vCore %, deadlocks, storage % | Query Store, `AzureDiagnostics` (SQL insights) |

### 8. Visualizing and exploring

- **Metrics explorer:** chart any metric, split by a dimension (e.g. failed runs **by pipeline name**), pin to a dashboard.
- **Workbooks:** interactive reports that mix KQL, metrics, parameters, and text. **The standard way to build a "pipeline health" page.**
- **Azure dashboards:** simple tile boards pinned from anywhere.
- ➕ **Azure Managed Grafana:** Grafana with Azure Monitor as a data source, for teams that already use Grafana.
- **Insights:** prebuilt experiences, such as Storage insights, Container insights, and **VM insights**.

### 9. ➕ Added — Application Insights and custom telemetry

- **Application Insights** (workspace-based) monitors **your own code**: the FastAPI service (Unit 4), Azure Functions, and custom Python jobs. It records requests, dependencies, exceptions, traces, and custom metrics.
- The modern way to instrument Python is **OpenTelemetry** with the Azure Monitor distro:

```python
import os, logging
from azure.monitor.opentelemetry import configure_azure_monitor

configure_azure_monitor(connection_string=os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"])
log = logging.getLogger("ecom.etl")
log.info("loaded orders", extra={"rows": 125_430, "load_date": "2026-09-24"})   # → traces table
```

- **Custom business signals** (rows loaded, rejected records, data freshness) are just as important as "did it succeed". A pipeline that "succeeds" with 0 rows is still broken.

### 10. ➕ Added — Service Health, Resource Health, and the Azure Monitor Agent

- **Service Health:** Azure-wide incidents, planned maintenance, and advisories **for the regions and services you use**. You can alert on it, which tells you whether a failure is your fault or Azure's.
- **Resource Health:** whether a *specific* resource is available right now, and its recent history.
- **Azure Monitor Agent (AMA) + Data Collection Rules (DCRs):** collect OS logs and performance counters from VMs (such as a **self-hosted integration runtime** machine). DCRs can also **filter or transform data before ingestion**, which cuts cost.

---

## Part B — Alerts and Action Groups for Pipeline Failures

### 1. How alerting fits together

```
 Alert RULE                                       Alert (instance)                ACTION GROUP
 ─────────────                                    ────────────────                ─────────────
 Scope:      adf-ecom-prod                        Fired  → Severity 1             Notify: email, SMS, push, voice
 Condition:  PipelineFailedRuns > 0 (5 min)  ──►  state: New/Acknowledged/Closed ──► Act: Logic App → Teams post
 Actions:    ag-dataeng-oncall                    Resolved when condition clears        Function, webhook, ITSM,
 Details:    Sev 1, auto-resolve                                                         Automation runbook, Event Hub
                        ▲
            Alert processing rules (suppress during maintenance, add action groups at scale)
```

- An **alert rule** says *what to watch and when to fire*.
- An **action group** says *who gets told and what runs*. It's a **reusable** object. Many rules can share one group (e.g. `ag-dataeng-oncall`).

### 2. Types of alert rules

| Type | Signal | Latency | Pipeline example |
|---|---|---|---|
| **Metric alert** | A platform or custom metric | ~1 min, near real time | `PipelineFailedRuns > 0` on the data factory |
| **Log search alert** | A **KQL query** run on a schedule | Minutes (1–15 min evaluation) | "Any `ADFActivityRun` failure where `ActivityType == 'Copy'`", with the error message in the alert |
| **Activity log alert** | An Activity log event | Minutes | "Someone deleted a linked service", **Service Health** incident in your region, **Resource Health** becomes unavailable |
| ➕ Smart detection / Prometheus | App Insights anomalies, Prometheus rules | varies | Sudden spike in API failures |

### 3. Anatomy of an alert rule

| Part | Settings |
|---|---|
| **Scope** | One resource, a resource group, or a subscription (metric alerts can cover many resources of the same type in one region) |
| **Condition** | Signal; **static threshold** (> 0) or ➕ **dynamic threshold** (machine-learned normal range, good for "duration is unusually high"); aggregation (count, average, max); **aggregation granularity** (the window, e.g. 5 min); **evaluation frequency** (how often it checks, e.g. every 1 min) |
| **Dimensions** | **Split by** e.g. pipeline name, so each failing pipeline produces its own alert and the alert says which one |
| **Actions** | One or more action groups |
| **Details** | **Severity** (Sev 0 Critical → Sev 4 Verbose), name, description, **auto-resolve** (stateful) or not |

**Alert states:**

- **Monitor condition** (set by the system): **Fired** → **Resolved** once the condition clears (for stateful alerts).
- **User response** (set by the team): **New** → **Acknowledged** → **Closed**.

### 4. Action groups

| Category | Options | Notes |
|---|---|---|
| **Notifications** | Email, **Email Azure Resource Manager role** (e.g. everyone with Owner), SMS, Azure mobile app push, voice call | Rate limits apply, e.g. SMS and voice about **1 per 5 minutes** per number, email about 100 per hour per address |
| **Actions** | **Logic App**, **Azure Function**, **Webhook** / **Secure webhook** (Entra-authenticated), **ITSM** (ServiceNow and similar), **Automation runbook**, **Event Hub** | Use them to post to **Teams/Slack**, open a ticket, restart something, or call PagerDuty |

- Use the **common alert schema** so every alert payload has the same JSON shape (`essentials` + `alertContext`). One Logic App can then handle every alert type.
- Name groups by audience and urgency: `ag-dataeng-oncall` (SMS + Teams, Sev 0–1), `ag-dataeng-ticket` (opens a ticket for working hours, Sev 2) and `ag-dataeng-fyi` (email only, Sev 3–4).

### 5. Pipeline failure alerting patterns

**Pattern 1 — Platform alert (outside the pipeline)**

- A metric alert on `PipelineFailedRuns` **and** `TriggerFailedRuns` (a failed trigger never starts a pipeline run, so it isn't counted as a failed pipeline run), or log alerts on `ADFPipelineRun` / `ADFTriggerRun` where `Status == "Failed"`.
- ✅ Catches **every recorded failure**, including failures before any activity runs and pipelines someone forgot to wire up. Runs that never start at all produce no failure record, so they need the heartbeat alert (below).
- ❌ The message is generic unless you use a log alert that includes the error details.

**Pattern 2 — In-pipeline failure handling (inside ADF)**

ADF activities have four dependency conditions: **Succeeded, Failed, Completed, Skipped** (shown in ADF Studio as *Upon Success / Failure / Completion / Skip*). Connect a **Web activity** (calling a Logic App) or a **Fail activity** to the **On failure** path:

```
 [Copy orders] ──success──► [Databricks: silver] ──success──► [Databricks: gold]
       │ failure                    │ failure                        │ failure
       └──────────────┬─────────────┴────────────────────────────────┘
                      ▼
          [Web: POST Logic App]  body: pipeline().Pipeline, pipeline().RunId,
                                       activity('Copy orders').error.message
```

- ✅ **Rich, specific context** (which step failed, the error text, a link to the run).
- ⚠️ Watch the **"try-catch" gotcha**: if the failure path *succeeds*, ADF may mark the whole pipeline as **Succeeded**, which hides the failure from Pattern 1. Add a **Fail activity** after the notification so the run still shows as failed.

**Best practice: use both.** The platform alert is the safety net. The in-pipeline alert gives the detail.

**Other services:**

| Service | How to alert |
|---|---|
| **Databricks** | Job/task **notifications** (email, webhook, Teams/Slack) on failure, on duration over threshold, or on streaming backlog. Plus diagnostic logs → Log Analytics → log alert. |
| **Synapse pipelines** | Same model as ADF (metrics + `SynapseIntegrationPipelineRuns`) |
| **Stream Analytics** | Metric alert on **Watermark delay** rising, runtime errors > 0, SU % > 80 |
| **Event Hubs** | Metric alert on **ThrottledRequests** > 0, incoming messages dropping to 0 (producer died) |
| ➕ **Data freshness** | Log alert or scheduled check: "the newest order in `gold.fact_sales` is older than 26 hours", which catches silent failures |
| **Heartbeat / "no run"** | A log alert when a pipeline has had no successful run within its expected interval. It catches stuck triggers and dropped runs, which never produce a "Failed" event (see Part D §1). |

### 6. Alert processing rules

Rules applied **after** an alert fires, across many alert rules:

- **Suppress notifications** during a planned maintenance window (e.g. Sunday 01:00–03:00), while alerts are still recorded.
- **Add an action group** to every alert in a resource group or subscription, without editing each rule.

### 7. Alerting best practices

- [ ] **Every alert should require action.** Otherwise it causes **alert fatigue**, and people start ignoring all alerts.
- [ ] Map **severity → response**: Sev 0–1 pages on-call, Sev 2 means a ticket during working hours, Sev 3–4 goes to email or a dashboard.
- [ ] **On-call rotation for Sev 0–1 only.** Someone is always reachable for failures and missed SLAs. Everything else waits for working hours.
- [ ] **Split by dimension** so the alert names the failing pipeline.
- [ ] Include a **runbook link** in the alert description (what to check, how to rerun).
- [ ] Alert on **symptoms the business cares about** (late or missing data), not just on causes.
- [ ] Deploy alert rules and action groups as **code** (Bicep/Terraform, Unit 9) so dev and prod stay the same.
- [ ] **Test** action groups (the portal has a *Test action group* button) and review noisy alerts regularly.

---

## Part C — Cost Management and Optimization

### 1. Microsoft Cost Management

The built-in tool (free for Azure usage) for **seeing, allocating, and controlling** spend.

| Feature | What it does |
|---|---|
| **Cost analysis** | Explore costs by **service, resource group, resource, tag, meter**, over time. Actual vs **forecast**. Views saved and shared. |
| **Budgets** | A spending limit for a scope (subscription, resource group, or filtered by tag) per month, quarter, or year. **Alert thresholds** on **actual** or **forecasted** cost (e.g. 50%, 80%, 100%, forecast 110%). Can trigger an **action group**. |
| **Cost alerts** | Budget alerts, **anomaly alerts** (unusual spend detected on a subscription), credit alerts, reservation utilization alerts |
| **Exports** | Scheduled cost data exports to storage (CSV/Parquet) for your own reporting, e.g. into the lakehouse and Power BI |
| **Advisor cost recommendations** | Idle or underused resources, right-sizing, reservation suggestions |

⚠️ **A budget does not stop spending.** It only alerts. To actually stop resources, connect the action group to an **Automation runbook or Function** (e.g. stop dev clusters or pause a Synapse pool). Cost data can also lag by several hours or up to a day.

### 2. Cost allocation with tags

- **Tags** are key/value labels on resources: `env=prod`, `project=ecom`, `owner=dataeng`, `cost-center=CC-1234`.
- Use **Azure Policy** to **require** or **inherit** tags (e.g. inherit `cost-center` from the resource group), so costs aren't left unassigned.
- In cost analysis, **group by tag** to show cost per team or project (**showback / chargeback**).
- Databricks custom tags on clusters and jobs flow through to the underlying VM costs.

### 3. Pricing models

| Model | How it works | Saving | Fits |
|---|---|---|---|
| **Pay-as-you-go** | Per second, hour, or GB | — | Unpredictable or new workloads |
| **Reservations** | Commit to **1 or 3 years** of a specific resource (VM size, SQL vCores, Synapse DWU, storage capacity, **Databricks DBCU**) | Large (varies by service) | Steady 24/7 workloads |
| **Azure savings plan for compute** | Commit to an **hourly spend** on compute for 1 or 3 years, flexible across VM sizes and regions | Somewhat less than reservations, more flexible | Steady but changing compute |
| **Spot VMs** | Spare capacity, **can be evicted** at short notice | Very large discount | Fault-tolerant batch: **Databricks worker nodes** |
| **Azure Hybrid Benefit** | Reuse existing Windows Server / SQL Server licenses | License portion | SQL VMs, Azure SQL |
| **Dev/Test pricing** | Discounted rates on Dev/Test subscriptions | Varies | Non-production environments |

### 4. Service-by-service cost levers (data engineering)

| Service | What you pay for | Optimization |
|---|---|---|
| **ADF** | Orchestration (per 1,000 activity runs), **data movement** (DIU-hours), **data flows** (vCore-hours), IR hours | Avoid tiny per-row loops (`ForEach` over thousands of items). Right-size DIUs. Use **incremental loads** (watermarks, Unit 11). Keep data flow **TTL** short. Watch self-hosted IR VM size. |
| **Databricks** | **DBUs** (per workload type) **+ the VMs** | **Job clusters, not all-purpose** clusters for scheduled work (much lower DBU rate). **Auto-termination** (e.g. 20 min idle). **Autoscaling**. **Spot workers** with an on-demand driver. **Cluster policies** to cap size. Photon or serverless when faster means cheaper. `availableNow` instead of always-on streams (Unit 12). ⚠️ Caps set too low cause failures (Part D §3). |
| **Synapse** | Dedicated pool DWU-hours, serverless **per TB scanned**, Spark pool vCore-hours | **Pause** dedicated pools when idle. For serverless, query **Parquet/Delta with partition pruning** and select only the columns you need (it bills by data scanned). |
| **Storage / ADLS** | GB stored per **access tier**, transactions, egress | **Lifecycle management**: hot → cool → cold by age (**archive** only on non-ZRS accounts, Unit 10), delete old bronze copies. Columnar compressed formats (Parquet/Delta). `VACUUM` old Delta files. Avoid the small-files problem (more transactions). Watch **early deletion** fees on cool, cold, and archive. ⚠️ `VACUUM` also limits Delta time travel/rollback (Part D §4). |
| **Event Hubs** | TUs/PUs per hour, Capture, ingress events | **Auto-inflate never scales down.** Reset TUs after spikes. Batch sends. |
| **Stream Analytics** | **SU-hours while the job runs** | Stop dev jobs. Right-size SUs (watch SU %). |
| **Azure SQL** | vCores/DTUs, storage | **Serverless** tier with auto-pause for dev. Elastic pools for many small DBs. Right-size. |
| **Log Analytics** (yes, monitoring costs too) | **GB ingested** + retention beyond the included period | Collect only the log categories you need. **Basic/Auxiliary** plans for noisy tables. **DCR transformations** to drop columns or rows. **Commitment tiers** at high volume. **Daily cap** in dev (⚠️ a cap in prod means losing logs). |

### 5. ➕ Added — FinOps way of working

The FinOps Foundation lifecycle: **Inform → Optimize → Operate**.

- **Inform:** everyone can see what they spend (tags, cost analysis views, a Power BI cost report from exports, ➕ Databricks `system.billing.usage`).
- **Optimize:** act on waste (idle clusters, oversized pools, wrong storage tiers) and commit with reservations or savings plans once usage is steady.
- **Operate:** budgets and anomaly alerts, policies (require tags, restrict expensive VM sizes, enforce auto-termination), a regular review (e.g. monthly), and a **cost estimate before building** with the **Azure Pricing Calculator**.

**Quick wins checklist:**

- [ ] Delete **orphaned resources**: unattached disks, old public IPs, forgotten dev clusters, stopped-but-allocated VMs
- [ ] **Auto-shutdown / auto-pause** everything in dev
- [ ] Use **job clusters + spot workers** for Databricks batch
- [ ] Set **storage lifecycle policies** on the lake
- [ ] Make streaming **only as real-time as the business needs**
- [ ] Right-size log collection, and set **budgets with forecast alerts** on every subscription

---

## Part D — Pipeline Failure Zones

Parts A–C cover the tools. This part is the checklist of **what goes wrong at each step** of the medallion pipeline, how to **prevent** it, and how you'd **see** it with Azure Monitor.

```
 Sources (DBs, APIs, files)            CDC / Event Hubs
        │ ① Ingestion                        │ ①a–c CDC & streaming
        └──────────────┬─────────────────────┘
                       ▼
               ② Bronze / Raw          landing, idempotency, audit, storage layout, recovery
                       │ ③ Transformation   quality, business rules, skew, OOM, cluster size
                       ▼
               ④ Silver / Enriched     Delta, history (SCD), access control
                       ▼
               ⑤ Gold / Aggregated     metric consistency, late data
                       ▼
               ⑥ Warehouse / marts → BI    history, performance, cost
 ─────────────────────────────────────────────────────────────────
 Cross-cutting (Part E): observability · alerting · recovery · ownership
```

### 1. Ingestion

| Failure | Prevention | How you detect it |
|---|---|---|
| **Connection failures:** network drops, DB or API unreachable, **expired credentials** | **Retry** with exponential backoff (ADF activity *Retry* + *Retry interval*; `tenacity` in Python). **Check connectivity before the load** (a Lookup or Web activity). **Log every attempt.** ➕ Use **managed identities** (no secret to expire), or Key Vault with expiry notifications. | `ADFActivityRun` `ErrorCode`/`ErrorMessage`; alert on repeated retries |
| **Job execution:** dropped runs, stuck trigger, partial success, wrong schedule | Stamp every batch with a **run ID + load date**. Use **idempotent writes** (MERGE or partition overwrite). Add **dependency checks** (is upstream finished? tumbling-window dependencies, Airflow sensors in Unit 14). | `TriggerFailedRuns`, `ADFTriggerRun`, plus a **heartbeat alert** (below) |
| **Rate limits** | Respect **HTTP 429 + `Retry-After`**. Use backoff **with jitter**. Cap concurrency (ForEach *batch count*). Unlimited fast retries look like a **DoS attack** to the provider and can get you blocked. | Count 429s in logs |
| **No new data** | Check before processing (Get Metadata `exists`, a Lookup row count, an If condition) and **skip cleanly** instead of failing. But tell apart "no data today is normal" from "the source is broken". | Data freshness alert (Part B §5) |
| **Schema changes** (common with sources still in development) | **Additive** (a new column): accept it into bronze with schema evolution (Auto Loader `addNewColumns` / rescued data, Delta `mergeSchema`), then **discuss** before adding it to silver. **Breaking** (type change, rename, dropped column): **stop the pipeline and escalate** to the source team. ➕ Agree on **data contracts** with source owners. | Job fails with a schema mismatch or mapping error |

**Heartbeat alert:** stuck triggers and dropped runs **produce no "Failed" event**, so a failure alert never fires. Alert on the *absence* of success:

```kusto
// Pipelines with no successful run in the last 26 hours
ADFPipelineRun
| where TimeGenerated > ago(7d) and Status == "Succeeded"
| summarize last_success = max(TimeGenerated) by PipelineName
| where last_success < ago(26h)
```

### 1a–1c. CDC and streaming

- **Event Hubs lag:** the Event Hubs service itself rarely lags. **Consumer lag** is common, though: a slow or crashed consumer falls behind while Event Hubs keeps buffering (Unit 12). Watch backlogged input events and **watermark delay** (Part A §7).
- **CDC sends duplicates after a connector restart.** Connectors are **at-least-once** and replay from their last committed position. Deduplicate on the event's unique key (primary key + LSN / commit timestamp / event ID) **within a time window**, e.g. **3 hours**:

```python
deduped = (events
    .withWatermark("event_ts", "3 hours")
    .dropDuplicatesWithinWatermark(["event_id"]))   # Spark 3.5+
```

  - The window **limits state**. Without it, the dedup state keeps growing (Unit 12).
  - ⚠️ A duplicate arriving **more than 3 hours later** gets through, because its key has already been removed from state. That's the accepted risk. The safety net is an **idempotent MERGE on key + version** at the sink, so a late duplicate updates the row instead of adding one.
- **A new column appears in the CDC stream:** same rule as schema changes. **Discuss**, then accept or reject it. ➕ A **schema registry** (Event Hubs has one) makes producers declare changes.

### 2. Bronze / Raw layer

| Problem | What to do |
|---|---|
| **Landing reliability + idempotency:** missing batch, duplicate batch, **partial file read as complete** | **Only insert data that isn't already there:** MERGE, or an anti-join on `_batch_id` / file name. Auto Loader records processed files in its checkpoint. Only read files that are **complete**: a `_SUCCESS` or manifest marker, or write to a temp path and rename. Compare **expected vs actual row counts**. |
| **Auditability:** where did each row come from, when, and in which batch? | Add metadata columns: `_loaded_at`, `_source_system`, `_source_file` (`_metadata.file_path` on Databricks), `_run_id`. Keep an **audit/control table** per run: run_id, source, rows read and written, status (Unit 11 control database). This also makes the lakehouse easier to manage. Lineage tooling comes in Unit 15 (Purview). |
| **Big data storage:** very uneven file sizes, **small files**, fragmentation, slow scans, bad partitioning in PySpark | Inside each dataset's folder (`source/entity/`, Unit 10), organize **by date first** (`ingest_date=2026-09-24/`), then optionally by a field with few distinct values (`country=VN/`). ⚠️ `city` may have too many distinct values and create thousands of tiny files. Aim for large partitions (roughly ≥ 1 GB). Compact with `OPTIMIZE` / auto compaction. ➕ **Liquid clustering** is Databricks' modern alternative to folder partitioning. |
| **Storage cost by access pattern** | Four tiers: **hot** (frequent access), **cool**, **cold**, and **archive** (long-term, rarely read). ⚠️ Archive is **offline** and takes **hours to rehydrate** before Spark can read it. Archive also isn't supported on ZRS/GZRS accounts such as our lake, so Bronze ages to cold there (Unit 10). Automate the moves with lifecycle policies (Part C §4). |
| **Recovery:** recover missing or corrupted data | Keep bronze **raw and immutable** so any layer can be **replayed** from it. Turn on ADLS **soft delete**. Use Delta **time travel** (§4). |

### 3. Transformation

**Data quality dimensions:**

| Dimension | Question | Example check |
|---|---|---|
| **Completeness** | Is anything missing? | `customer_id` not null; today's row count within ±20% of the 7-day average |
| **Uniqueness** | Any duplicates? | `order_id` unique |
| **Validity** | Values in the allowed range or format? | `status IN (...)`, `amount >= 0` |
| **Consistency** | Do systems and layers agree? | Silver order total = source total |
| **Timeliness** | Did it arrive on time (**SLA**)? | Gold ready by 06:00 (Part E) |
| ➕ **Accuracy** | Does it match reality? | Spot-check against the source system |

**Transformation failures:**

| Failure | Prevention |
|---|---|
| **Business rule implemented wrong** | Test transformations with known inputs and outputs, dbt tests (Unit 8), code review |
| **Join key is null or not unique** | Test `not null` + `unique` **before** joining. Null keys silently drop rows in inner joins. Duplicate keys **multiply rows** (fan-out). Compare row counts before and after the join. |
| **Reference data out of date** (e.g. an old exchange-rate table) | Freshness check on reference tables |
| **Bad user-entered records fail the whole job** | **Quarantine** bad rows (Unit 11), `badRecordsPath`, rescued data column. Fail the job only if the bad-row % goes over a threshold. |
| **Incremental watermark skips records** (late updates, or rows committed after the watermark was read) | Re-read a **lookback overlap** (watermark − N minutes/hours) and dedupe with an idempotent MERGE, or switch to CDC |

**Big data failures (Spark, Unit 7):**

- **Data skew:** a few keys hold most of the rows, so a few tasks run far longer than the rest (visible in the Spark UI). Fixes:
  - **AQE skew-join handling** (`spark.sql.adaptive.skewJoin.enabled`)
  - **Broadcast** the small side of the join
  - ➕ **Salt** hot keys
  - Pre-aggregate before joining
- **OOM on large shuffles:**
  - **AQE** coalesces and splits shuffle partitions
  - Filter rows and select columns **before** the join, and use **partition pruning** so only the needed data is read and joined
  - Avoid `collect()` / `toPandas()` on large data
  - Tune `spark.sql.shuffle.partitions`, or use memory-optimized nodes
- **Cluster too small:** a rarer infrastructure problem. Autoscaling max or policy limits are set too low, so jobs crawl or fail. Set **min/max constraints** on purpose. ⚠️ These are the same cluster policies Part C uses to cap cost, so balance cost against headroom.

### 4. Silver / Enriched layer

- **Delta format:** Parquet data files **plus a transaction log** (`_delta_log`). That gives ACID writes, schema enforcement, and **time travel**, so you can roll back a bad load:

```sql
DESCRIBE HISTORY silver.orders;
RESTORE TABLE silver.orders TO VERSION AS OF 41;
```

  ⚠️ You can only roll back as far as `VACUUM` retention allows (default **7 days**). `VACUUM` saves storage cost (Part C) but shortens your recovery window.
- **History:** overwriting a table **loses the historical truth**, e.g. which segment a customer was in last month. Handle it with **SCD Type 2** (Unit 5; dbt snapshots in Unit 8) and keep bronze append-only.
- **Governance:** **access control** on silver tables (Unity Catalog grants, RBAC/ACLs). PII masking and cataloging are covered in Unit 15.

### 5. Gold / Aggregation layer

- **Metric drift:** two marts calculate "revenue" differently. Define each metric **once** (a dbt model or semantic layer).
- **Double counting:** fan-out joins, or rerunning a non-idempotent `INSERT` of aggregates. Use overwrite-by-partition or MERGE.
- **Late-arriving data:** recompute the **last N days** of aggregates, not just today.
- **Reconciliation:** check that gold totals equal silver totals.

### 6. Data warehouse and data marts

- **History / SCD:** some dimensions need their full change history (Type 2). Others are fine being overwritten (Type 1). Decide per attribute (Unit 5).
- **Performance and cost:** slow queries and high cost. Options:
  - Pre-aggregate in gold
  - Distribution and partitioning (Synapse hash distribution) or clustering
  - Materialized views and result caching
  - Right-size or pause compute
  - Scan less on serverless, which bills per TB (Part C §4)

---

## Part E — Operations Playbook (Putting It Together) ➕ Added

```
                        ┌─────────────── Log Analytics workspace: log-ecom-prod ───────────────┐
 ADF (diag settings) ───┤ ADFPipelineRun / ADFActivityRun                                      │
 Databricks (diag) ─────┤ DatabricksJobs / DatabricksClusters                                  ├──► Workbook "Data Platform Health"
 Event Hubs / ASA ──────┤ metrics + logs                                                       │    (failures, durations, freshness,
 Storage ───────────────┤ StorageBlobLogs                                                      │     lag, watermark delay, cost)
 Activity log ──────────┘ AzureActivity                                                        │
                        └──────────────────────────────────────────────────────────────────────┘
        │ metric alerts (fast)          │ log search alerts (detailed)    │ Service Health alerts
        └───────────────┬───────────────┴─────────────────────────────────┘
                        ▼
            ag-dataeng-oncall (Sev 0–1): SMS + Logic App → Teams channel + ticket
            ag-dataeng-ticket (Sev 2):   ITSM ticket, handled in working hours
            ag-dataeng-fyi   (Sev 3–4): email
 Cost Management budget (rg-ecom-prod, monthly, 80% actual / 100% forecast) ──► ag-finops (email + runbook in dev)
```

### Cross-cutting operations

| Concern | Practice |
|---|---|
| **Observability** | **Structured logs**, as JSON rather than free text, with the same fields every time: `run_id`, `pipeline`, `step`, `rows_in`, `rows_out`, `rows_rejected`, `status`, `duration_s`. They're easy to query with KQL (Part A §6, §9). |
| **Alerting** | On **job failure** *and* on **SLA missed** (late or missing data), routed to an on-call rotation for Sev 0–1 (Part B §7) |
| **Recovery** | **Idempotent reruns**: rerunning any day gives the same result, so recovery means "fix, then rerun or backfill by date parameter" |
| **Ownership** | Every dataset has a named **data owner** (business, accountable for meaning and quality) and a **technical owner** (the pipeline). Record them in tags (`owner=`) and later in the catalog (Unit 15). |

```python
log.info("stage_complete", extra={"run_id": run_id, "pipeline": "pl_master_nightly",
         "step": "silver_orders", "rows_in": 125_430, "rows_out": 125_102,
         "rows_rejected": 328, "status": "succeeded", "duration_s": 212})
```

**Incident flow when the nightly load fails:**

1. **Detect:** the alert fires (Sev 1) and posts to Teams with the pipeline name and run ID.
2. **Triage:** check Service Health (is it Azure?), then `ADFActivityRun` for the error message.
3. **Fix and rerun or backfill:** ADF *Rerun from failed activity*. Pipelines must be **idempotent** (Unit 11) so reruns are safe.
4. **Verify:** check the data freshness query and row counts.
5. **Learn:** write a short post-incident note and add or adjust the alert or runbook.

**Key operational measures:** ➕ **SLA / SLO** (e.g. "gold tables ready by 06:00 on 99% of days"), **MTTD** (mean time to detect) and **MTTR** (mean time to recover).

---

## Key Terms Cheat Sheet

- **Azure Monitor:** Azure's unified platform for metrics, logs, and alerts
- **Metrics vs logs:** numbers for detecting (93 days) vs records for diagnosing (Log Analytics, KQL)
- **Activity log** (control plane, on by default, 90 days) vs **resource logs** (data plane, **need a diagnostic setting**)
- **Diagnostic setting:** categories → Log Analytics / storage / Event Hubs / partner. **Resource-specific** vs `AzureDiagnostics` mode.
- **ADF tables:** `ADFPipelineRun`, `ADFActivityRun`, `ADFTriggerRun`. The ADF Monitor tab keeps only 45 days.
- **Log Analytics workspace:** tables, **Analytics / Basic / Auxiliary** plans, interactive vs long-term retention
- **KQL:** `where`, `project`, `extend`, `summarize ... by bin()`, `order by`, `take`, `render`
- **Metrics explorer, workbooks, dashboards,** ➕ **Managed Grafana, Insights**
- ➕ **Application Insights + OpenTelemetry, Service Health / Resource Health, AMA + DCRs**
- **Alert rule:** scope + condition + actions + details. **Metric / log search / activity log** alerts.
- **Static vs dynamic thresholds, aggregation granularity vs evaluation frequency, split by dimensions**
- **Severity Sev 0–4; stateful (auto-resolve); Fired/Resolved; New/Acknowledged/Closed**
- **Action group:** reusable notifications (email, SMS, push, voice) + actions (Logic App, Function, webhook, ITSM, runbook, Event Hub). **Common alert schema.**
- **Alert processing rules:** suppress during maintenance, add action groups at scale
- **ADF dependency conditions:** Succeeded / Failed / Completed / Skipped. The **try-catch gotcha**, fixed with a **Fail activity**.
- **Cost Management:** cost analysis, **budgets (alert only, don't stop spend)**, anomaly alerts, exports, Advisor
- **Tags + Azure Policy** for cost allocation (showback / chargeback)
- **Pay-as-you-go, reservations, savings plan, spot, Hybrid Benefit, Dev/Test**
- **Levers:** job clusters, auto-termination, spot workers, pause Synapse, serverless scans per TB, storage lifecycle tiers, auto-inflate doesn't scale down, stop ASA jobs, Log Analytics ingestion control
- ➕ **FinOps: Inform → Optimize → Operate; SLA/SLO, MTTD/MTTR, data freshness checks**
- **Failure zones:** ingestion → CDC/streaming → bronze → transformation → silver → gold → warehouse, plus cross-cutting operations
- **Ingestion:** retry + backoff + jitter, respect 429, pre-flight checks, run IDs, idempotent writes, dependency checks, **heartbeat alerts**, additive vs breaking schema changes
- **CDC:** at-least-once, so dedupe on key within a window (e.g. 3 h), plus an idempotent MERGE as the safety net. Consumer lag ≠ Event Hubs lag.
- **Bronze:** only insert what's new, completion markers, audit columns + control table, date-first partitioning (watch partition-field cardinality), hot/cool/cold (archive only on non-ZRS accounts), immutable raw for replay
- **Quality dimensions:** completeness, uniqueness, validity, consistency, timeliness (➕ accuracy)
- **Transform failures:** bad rules, null or duplicate join keys (fan-out), stale reference data, quarantine bad records, watermark lookback
- **Spark failures:** skew (AQE skew join, broadcast, salting), OOM (AQE, prune before joins), cluster size limits vs cost policies
- **Delta:** Parquet + transaction log, time travel/`RESTORE`, limited by `VACUUM` retention. **SCD2** to keep history.
- **Cross-cutting:** structured logs, alert on failure + SLA, idempotent reruns/backfills, data owner + technical owner

---

## Practice Questions

1. **What's the difference between metrics and logs in Azure Monitor, and when do you use each?**
   Metrics are lightweight numeric time series, collected automatically and near real time, and are good for detecting and alerting. Logs are detailed records in a Log Analytics workspace, queried with KQL, and are good for diagnosing the cause.

2. **Your ADF pipeline failures don't appear in Log Analytics. Why?**
   Resource logs aren't collected by default. You need a diagnostic setting that sends the PipelineRuns, ActivityRuns, and TriggerRuns categories to the workspace.

3. **Activity log vs resource logs?**
   The activity log records control-plane operations on resources (who created, changed, or deleted what) and is on by default. Resource logs record what happens inside a resource (e.g. each pipeline run) and need a diagnostic setting.

4. **Name three destinations for a diagnostic setting and one use for each.**
   Log Analytics for querying and alerting, a storage account for cheap archiving, and Event Hubs for streaming logs to an external SIEM or tool.

5. **Why choose resource-specific mode over Azure diagnostics mode?**
   It writes to dedicated tables with proper columns (`ADFActivityRun`), which are easier and cheaper to query than one wide `AzureDiagnostics` table.

6. **You need ADF run history from 3 months ago. Where is it?**
   Only in Log Analytics (or storage) if a diagnostic setting was sending it. ADF's own Monitor view keeps just 45 days.

7. **Write a KQL query that counts failed pipeline runs per pipeline over the last 7 days.**
   `ADFPipelineRun | where TimeGenerated > ago(7d) and Status == "Failed" | summarize failures = count() by PipelineName | order by failures desc`

8. **What is a Log Analytics table plan, and when would you use Basic?**
   A pricing and feature tier per table. Use Basic for high-volume logs you rarely query, since ingestion is cheaper but queries are limited and billed per query.

9. ➕ **How would you make a Python ETL script send custom metrics like "rows loaded" to Azure Monitor?**
   Instrument it with the Azure Monitor OpenTelemetry distro (Application Insights) and log or record the value as a custom property or metric.

10. **Name the three main types of alert rules and give a pipeline example for each.**
    Metric (`PipelineFailedRuns > 0`), log search (a KQL query for failed Copy activities with the error message), and activity log (someone deleted a linked service, or a Service Health incident).

11. **What are the four parts of an alert rule?**
    Scope, condition, actions (action groups), and details (severity, name, auto-resolve).

12. **What's the difference between aggregation granularity and evaluation frequency?**
    Granularity is the time window aggregated (e.g. the last 5 minutes). Frequency is how often the rule checks (e.g. every 1 minute).

13. **Why "split by dimension" on a failed-runs metric alert?**
    Each pipeline then gets its own alert, so the notification says which pipeline failed.

14. **What is an action group, and why is it reusable?**
    A named set of notifications and automated actions. Many alert rules can point to the same group, so you manage recipients in one place.

15. **List four action types in an action group.**
    Four of: Logic App, Azure Function, webhook / secure webhook, ITSM connector, Automation runbook, Event Hub (plus email, SMS, push, and voice notifications).

16. **How do you get pipeline failure alerts into a Teams channel?**
    Action group → Logic App (or webhook) that posts the alert payload (common alert schema) to Teams.

17. **What's the ADF "try-catch gotcha", and how do you fix it?**
    If you handle a failure with an On-failure activity that succeeds, the pipeline can be marked Succeeded, which hides the failure from platform alerts. Add a Fail activity after the notification step.

18. **Why combine platform alerts with in-pipeline failure handling?**
    Platform alerts (on failed pipeline runs and failed trigger runs) catch every recorded failure, including unwired pipelines. In-pipeline handling adds specific context (the step and error message).

19. **You're doing maintenance Sunday 1–3 AM and don't want pages. What do you use?**
    An alert processing rule that suppresses notifications for that scope and time window.

20. **Stateful vs stateless alerts?**
    Stateful alerts fire once and auto-resolve when the condition clears. Stateless alerts fire every time the condition is met.

21. **The nightly pipeline "succeeded" but loaded 0 rows. How would you catch that?**
    Monitor business signals: a row-count or data-freshness check (e.g. max `order_ts` older than 26 hours) as a log alert or a validation step that fails the pipeline.

22. **What causes alert fatigue, and how do you prevent it?**
    Too many non-actionable or noisy alerts. Alert only on things that need action, use severities that route correctly, and tune or remove noisy rules.

23. **Does an Azure budget stop resources when reached?**
    No, it only alerts. To stop resources, have its action group trigger a runbook or Function.

24. **Actual vs forecasted budget alerts?**
    Actual fires when spend has crossed the threshold. Forecasted fires when spend is projected to cross it by the end of the period, which gives earlier warning.

25. **How do you show cost per team?**
    Tag resources (`cost-center`, `owner`), enforce or inherit the tags with Azure Policy, and group cost analysis by tag.

26. **Reservation vs savings plan vs spot?**
    A reservation commits to a specific resource for 1 or 3 years (biggest discount). A savings plan commits to an hourly compute spend with flexibility. Spot uses spare capacity at a large discount but can be evicted.

27. **Give four ways to cut Databricks cost.**
    Job clusters instead of all-purpose, auto-termination, autoscaling, and spot worker nodes (also cluster policies, and `availableNow` instead of always-on streams).

28. **Why is Synapse serverless SQL cost sensitive to file format and query shape?**
    It bills per TB scanned. Parquet/Delta with partition pruning and selecting only needed columns scans much less data than CSV and `SELECT *`.

29. **How do you reduce storage cost for old bronze data in ADLS?**
    Lifecycle management policies that move data to cool or cold by age (archive only on non-ZRS accounts) and delete it after the retention period, while watching early-deletion fees.

30. **An Event Hubs namespace auto-inflated to 20 TUs during Black Friday. What's the cost risk?**
    Auto-inflate doesn't scale down, so you keep paying for 20 TUs until someone lowers it manually or with automation.

31. **Monitoring itself is costing too much. What can you do?**
    Collect only the needed log categories, move noisy tables to Basic/Auxiliary, filter with DCR transformations, shorten retention, and use a commitment tier at high volume (a daily cap in dev only).

32. ➕ **What are the three FinOps phases?**
    Inform (visibility and allocation), Optimize (remove waste, commit to discounts), and Operate (budgets, policies, regular reviews).

33. ➕ **Describe the steps when the nightly load fails.**
    Detect (alert), triage (Service Health, then activity logs for the error), fix and rerun from the failed activity (idempotent pipelines), verify freshness and row counts, and learn (post-incident note, alert or runbook update).

34. **Name the six failure zones of a medallion pipeline.**
    Ingestion (including CDC/streaming), bronze, transformation, silver, gold, and warehouse/marts, plus cross-cutting operations.

35. **A trigger got stuck and the pipeline never ran. Why didn't the failure alert fire, and what catches it?**
    Nothing failed, so there was no failure event. A **heartbeat** or freshness alert on "no successful run in X hours" catches it.

36. **How should an ingestion job handle an API rate limit?**
    Respect 429 / `Retry-After`, retry with exponential backoff and jitter, and cap concurrency. Unlimited fast retries look like a DoS attack.

37. **A source adds a column. Another source renames one. How do you treat each?**
    Additive: accept it into bronze with schema evolution, then discuss before adding it to silver. Breaking: stop the pipeline and escalate to the source owner.

38. **Why do CDC connectors send duplicates, and what are the limits of a 3-hour dedup window?**
    They're at-least-once and replay after a restart. The window limits state, but a duplicate that arrives more than 3 hours later gets through. An idempotent MERGE on key + version at the sink catches it.

39. **How do you stop a half-written file being loaded as complete?**
    Use completion markers or manifests (`_SUCCESS`), write-then-rename, or Auto Loader's file tracking. Then check row counts against the expected count.

40. **What audit metadata should every bronze row carry?**
    Ingest timestamp, source system, source file, and run/batch ID, plus a per-run audit table.

41. **Why might partitioning bronze by `date/city` be a bad idea?**
    City may have too many distinct values, which creates many tiny files. Partition by date, plus a field with few values if needed, and compact files (or use liquid clustering).

42. **List the five data quality dimensions, with one check each.**
    Completeness (not null, row count in range), uniqueness (unique key), validity (allowed values), consistency (totals match across layers), timeliness (ready by SLA).

43. **A join suddenly doubled the row count. What happened?**
    The join key isn't unique on one side (fan-out). Test uniqueness before joining.

44. **One Spark task runs 40 minutes while the others take 1 minute. Diagnose and fix.**
    Data skew on the join or group key. Enable AQE skew-join handling, broadcast the small table, or salt the hot keys.

45. **A bad load corrupted `silver.orders` 3 days ago. How do you recover, and what could stop you?**
    `RESTORE TABLE ... TO VERSION AS OF` a version before the bad load, or replay from immutable bronze. A `VACUUM` with retention shorter than 3 days would have removed the old files needed to restore.
