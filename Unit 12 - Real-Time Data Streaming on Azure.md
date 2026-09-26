# Unit 12 — Real-Time Data Streaming on Azure: Study Guide

**Scope:** Batch vs stream processing · Azure Event Hubs (high-throughput ingestion that works like Kafka) · Azure Stream Analytics (real-time analytics in SQL) · Introduction to Structured Streaming on Azure Databricks

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. In Unit 11, the data moved **on a schedule**: every night, ADF loaded yesterday's orders into the medallion layers. This unit handles data **as it happens**. Click events and orders flow into **Event Hubs**. **Stream Analytics** feeds a live sales dashboard and fraud alerts. **Databricks Structured Streaming** keeps the bronze/silver/gold Delta tables up to date within seconds. **Naming note:** the event payloads use `total_amount` and `order_ts` (event time). The batch units call these `amount` and `order_date`.

---

## Part A — Batch vs Stream Processing

### 1. The core difference

| | **Batch** (Unit 11) | **Stream** |
|---|---|---|
| Input | **Bounded**: a known chunk (a day, a file) | **Unbounded**: events keep arriving with no end |
| When it runs | On a schedule or trigger, then stops | **Always running**, processing events as they arrive |
| Latency | Minutes to hours | Milliseconds to seconds |
| Unit of work | A whole dataset | One event, or a small **micro-batch** |
| Typical outputs | Warehouse loads, reports, ML training sets | Alerts, live dashboards, real-time features, fresh tables |
| Hard parts | Volume, idempotent reruns | **Time, ordering, late data, state, failures while running** |

**Rule of thumb:** use streaming only when the business actually needs **fresh data or a fast reaction**. Examples are fraud detection, live inventory, monitoring, and personalization. Streaming costs more to run (it's always on) and is harder to debug. A nightly report doesn't need it.

### 2. Key streaming concepts

**Event:** an immutable fact that something happened, with a timestamp. Example: `{"event":"add_to_cart","customer_id":1042,"product_id":"P-77","ts":"2026-09-25T10:15:03Z"}`.

**Event time vs processing time:**

| Time | Meaning | Example |
|---|---|---|
| **Event time** | When the event **actually happened** (a field inside the event) | The phone recorded the click at 10:15:03 |
| **Ingestion (enqueued) time** | When the broker received it | Event Hubs got it at 10:15:04 |
| **Processing time** | When your job processes it | Spark read it at 10:15:09 |

Analytics should almost always use **event time**. A phone in a tunnel might send a 10:15 click at 10:40, and that click still belongs in the 10:15 window.

**Out-of-order and late events:** networks, retries, and offline devices mean events don't arrive in order.

**Windows** divide an unbounded stream into finite groups you can aggregate:

```
Tumbling (5 min):   |----1----|----2----|----3----|       fixed size, no overlap, each event in exactly 1 window
Hopping (10 min, hop 5): |---------A---------|
                             |---------B---------|       fixed size, overlapping, an event can be in several windows
Sliding (5 min):    a window for every point where an event enters or leaves; "the last 5 minutes at any moment"
Session (gap 30 min): |--clicks--|    gap    |--clicks------|   size depends on activity; closes after inactivity
```

**Watermark:** the engine's estimate of *"I've probably seen all events up to time T."* It's usually calculated as *max event time seen − allowed lateness*. Once the watermark passes the end of a window, the engine **finalizes** that window and can **drop its state**. Events that arrive later than that are **dropped** (or handled separately).

- A bigger lateness allowance gives more complete results, but with more delay and more memory used.
- A smaller allowance gives faster results, but more late events get dropped.

**State:** anything the job must remember between events: running counts, open windows, deduplication keys, the unmatched side of a join. State must be **checkpointed** so a restarted job can continue where it stopped. State must also be **bounded** (by watermarks or timeouts), or memory keeps growing.

**Delivery guarantees:**

| Guarantee | Meaning | Risk |
|---|---|---|
| **At-most-once** | Each event is processed 0 or 1 times | Data loss |
| **At-least-once** | Each event is processed 1 or more times | **Duplicates**. The most common guarantee. |
| **Exactly-once** (effectively once) | Each event affects the result once | Needs a **replayable source + checkpointing + an idempotent or transactional sink** |

**"Exactly-once" is an end-to-end property.** Event Hubs is at-least-once. Spark gets exactly-once by replaying from checkpointed offsets **into Delta**, which commits transactionally. If you write to a plain REST API, you're back to at-least-once and must deduplicate on the receiving side.

**Backpressure:** what happens when events arrive faster than the job can process them. A log-based broker like Event Hubs **buffers** them durably, so consumers fall behind (**lag**) instead of losing data. You scale out (more partitions or consumers) to catch up.

**Micro-batch vs continuous:**

- **Micro-batch** (Spark Structured Streaming by default) collects a few seconds of events and processes them as a small batch. Latency is seconds, and throughput and fault tolerance are simple to get right.
- **Continuous / record-at-a-time** (Stream Analytics, Flink) processes each event as it arrives, with lower latency.

### 3. ➕ Added — Lambda vs Kappa architecture

| | **Lambda** | **Kappa** |
|---|---|---|
| Design | Two paths: a **batch layer** (accurate, slow) and a **speed layer** (fast, approximate), merged in a serving layer | **One streaming path** for everything. Reprocess by **replaying** the log. |
| Pro | Batch results correct any streaming errors | One codebase, one logic |
| Con | **Two codebases** that must give the same answer | Needs a replayable log with long retention, and a strong stream engine |

On Azure, a common approach is a **Kappa-style lakehouse**: Event Hubs → Structured Streaming → Delta bronze/silver/gold. **Event Hubs Capture** keeps a raw copy in the lake so you can replay. Because Spark uses the **same DataFrame API for batch and streaming**, the two worlds largely merge.

### 4. Streaming use cases for the e-commerce platform

| Use case | Latency need | Likely tool |
|---|---|---|
| Live sales dashboard (revenue per minute) | Seconds | Stream Analytics → Power BI |
| Card fraud alerts (many orders from different countries in 1 min) | Seconds | Stream Analytics → Event Hubs / Functions |
| Low-stock alerts | Seconds to minutes | Stream Analytics or Databricks |
| Clickstream → bronze/silver Delta for analysts | Minutes | Databricks Structured Streaming |
| Real-time recommendation features | Seconds | Databricks (feature tables) |
| Nightly sales star schema | Hours | **Batch** (Unit 11). No need to stream it. |

---

## Part B — Azure Event Hubs

### 1. What it is

Event Hubs is a **fully managed, distributed event streaming platform** (a log-based "event ingestor"). It can take in **millions of events per second** from apps, devices, and services, store them **durably for a retention period**, and let **many independent consumers** read them at their own pace.

- It's the **front door** of a streaming pipeline. It **decouples** the producers (your web app) from the consumers (Stream Analytics, Databricks, Functions).
- It exposes an **Apache Kafka–compatible endpoint**. Existing Kafka clients can connect by changing their configuration only, with no Kafka cluster for you to run.

### 2. Kafka ↔ Event Hubs vocabulary

| Kafka | Event Hubs |
|---|---|
| Cluster | **Namespace** (`evhns-ecom-prod.servicebus.windows.net`) |
| Topic | **Event hub** (`clickstream`, `orders`) |
| Partition | Partition |
| Consumer group | Consumer group |
| Offset | Offset (plus a **sequence number**) |
| Broker nodes, ZooKeeper/KRaft | Managed by Azure. You don't see them. |

### 3. Architecture

```
Producers                         Event Hubs namespace: evhns-ecom-prod
─────────                         ────────────────────────────────────────
Web app ──┐                        Event hub: orders   (retention 7 days)
Mobile ───┼── HTTPS / AMQP / ──►   ┌ Partition 0: [e0][e1][e2][e3]... → newest
Services ─┘    Kafka protocol      ├ Partition 1: [e0][e1][e2]...
                                   ├ Partition 2: [e0][e1][e2][e3][e4]...
                                   └ Partition 3: [e0][e1]...
                                          │   each partition = an ordered, append-only log
               ┌──────────────────────────┼───────────────────────────┐
               ▼                          ▼                           ▼
   Consumer group "$Default"   Consumer group "cg-asa"      Consumer group "cg-databricks"
   (Azure Function)            (Stream Analytics job)       (Structured Streaming)
   each group keeps its own position (offset) in every partition
               │
               └── Capture ──► ADLS Gen2  bronze/eventhubs/orders/...  (Avro / Parquet)
```

### 4. Core concepts

**Partitions**

- A partition is an **ordered sequence of events**. **Order is guaranteed only within a partition**, not across the whole event hub.
- Partitions are the unit of **parallelism**. Within one consumer group, you usually have **at most one active reader per partition**. With 4 partitions, adding a 5th consumer doesn't speed anything up.
- **Choosing the count:** plan for your peak number of parallel consumers. In the **Standard tier, the count is fixed when you create the hub**. Premium and Dedicated let you **increase** it, but never decrease it. Increasing it changes which partition a key maps to, which breaks per-key ordering during the change.
- Limits (check current docs): Standard allows up to **32** partitions per event hub. Premium and Dedicated allow far more.

**Partition key**

- The producer can set a **partition key** (e.g. `customer_id` or `order_id`). Event Hubs hashes it, so **all events with the same key go to the same partition, in order**. Example: all events for order 5001 (`created → paid → shipped`) arrive in order.
- With no key, events are spread round-robin, which balances load best but gives no ordering.
- ⚠️ A **hot key** (one huge customer or bot) overloads one partition. That's the same skew problem as in Unit 11.

**Event:** a body (bytes, usually JSON or Avro) plus **properties** (your own metadata) plus **system properties** (`EnqueuedTimeUtc`, `Offset`, `SequenceNumber`, `PartitionKey`). The max size is **1 MB** (Standard and above). Send large payloads to Blob storage and put only the link in the event (the "claim check" pattern ➕).

**Retention:** events are **not deleted when they are read**. They stay until retention expires, which is what makes **replay** possible. Several consumer groups can read the same events, and a consumer can rewind.

**Consumer groups:** a **view** of the whole event hub with its own read positions. **One consumer group per downstream application** (ASA, Databricks, Functions), so they don't interfere with each other. `$Default` exists automatically.

**Checkpointing (consumer side):** the consumer **saves its offset** per partition, usually in a **Blob container**, so that after a restart it continues from there. Event Hubs doesn't track what you've processed. **The consumer is responsible for its position.** If you checkpoint after processing, you get **at-least-once** delivery: a crash between processing and checkpointing means some events are processed again, so make processing idempotent.

### 5. Tiers and capacity

| Tier | Capacity unit | Key features |
|---|---|---|
| **Basic** | Throughput units (TUs) | 1 consumer group, 1-day retention, **no Kafka**, **no Capture**. For dev and tests only. |
| **Standard** | **Throughput units** | Kafka, Capture (extra charge), up to 7-day retention, multiple consumer groups, **auto-inflate** |
| **Premium** | **Processing units (PUs)** | Dedicated-like resource isolation, longer retention (up to 90 days), more partitions, Capture included, geo-replication |
| **Dedicated** | **Capacity units (CUs)** | A single-tenant cluster for the largest workloads |

**Throughput unit (Standard):** each TU gives about **1 MB/s or 1,000 events/s ingress** and **2 MB/s egress**. If you go over, requests are **throttled** (`ServerBusy`). **Auto-inflate** raises TUs automatically up to a maximum you set. It does **not** scale back down on its own.

### 6. Event Hubs Capture

- Automatically writes the stream to **ADLS Gen2 / Blob** in **Avro** by default (Parquet is available through the no-code editor), in time-based or size-based files. Example path: `{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}`.
- It gives you a **raw bronze archive** with **no code**. You can replay or batch-process it later (for example with Auto Loader from Unit 11) and keep history beyond the retention period.

### 7. Producing and consuming

**Python SDK (`azure-eventhub`) with a managed identity or Entra login:**

```python
import json
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential

producer = EventHubProducerClient(
    fully_qualified_namespace="evhns-ecom-prod.servicebus.windows.net",
    eventhub_name="orders",
    credential=DefaultAzureCredential())          # no connection string in code (Unit 9)

with producer:
    batch = producer.create_batch(partition_key="5001")   # same key → same partition → in order
    batch.add(EventData(json.dumps({"order_id": 5001, "customer_id": 1042, "product_id": "P-77",
                                    "card_hash": "9c2e41", "country": "VN",
                                    "total_amount": 59.90, "status": "created",
                                    "order_ts": "2026-09-25T10:15:03Z"})))
    producer.send_batch(batch)                     # send in batches for throughput
```

```python
# Consumer with blob checkpointing (pip install azure-eventhub-checkpointstoreblob)
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore

store = BlobCheckpointStore(blob_account_url="https://stecomlakeprod.blob.core.windows.net",
                            container_name="eh-checkpoints", credential=DefaultAzureCredential())
consumer = EventHubConsumerClient(fully_qualified_namespace="evhns-ecom-prod.servicebus.windows.net",
                                  eventhub_name="orders", consumer_group="cg-order-service",
                                  credential=DefaultAzureCredential(), checkpoint_store=store)

def on_event(partition_context, event):
    handle(json.loads(event.body_as_str()))        # must be idempotent
    partition_context.update_checkpoint(event)     # save position after processing

with consumer:
    consumer.receive(on_event=on_event, starting_position="-1")   # "-1" = from the beginning if no checkpoint
```

With a checkpoint store, several consumer instances in the same group **share out the partitions automatically** (load balancing).

**Kafka clients:** keep your Kafka code and change the configuration:

```properties
bootstrap.servers=evhns-ecom-prod.servicebus.windows.net:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="$ConnectionString" password="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...";
```

(OAuth with Entra, `sasl.mechanism=OAUTHBEARER`, is the more secure option.) Port **9093**, always TLS.

### 8. Security and operations

| Area | Practice |
|---|---|
| **Auth** | **Entra ID + RBAC**: *Azure Event Hubs Data Sender*, *Data Receiver*, *Data Owner*, given to **managed identities**. **SAS policies** (shared access keys, scoped to the namespace or one hub, with Send/Listen/Manage rights) are the legacy option. Keep them in Key Vault and disable local auth if you can. |
| **Network** | **Private endpoints**, IP firewall, "trusted Microsoft services" exception (so ASA and Capture can reach it) |
| **Schemas** | ➕ **Azure Schema Registry** (built into the namespace): central Avro/JSON schemas with compatibility rules, so producers can't break consumers |
| **Reliability** | Availability zones. **Geo-disaster recovery** (metadata alias) or **geo-replication** (Premium/Dedicated, replicates data too) |
| **Monitoring** | Metrics: incoming/outgoing messages, **throttled requests**, **consumer lag** (how far behind a group is). Alerts on throttling and lag. |

### 9. ➕ Added — Event Hubs vs other Azure messaging services

| Service | Model | Use when |
|---|---|---|
| **Event Hubs** | Log-based **event streaming**, high throughput, retention, replay | Telemetry, clickstream, logs, and anything you'll **analyze as a stream** |
| **Event Grid** | **Push**-based **discrete event** routing (seen in Unit 11 storage triggers) | Reacting to occasional events ("a blob was created", "a resource changed") |
| **Service Bus** | **Enterprise message broker**: queues and topics, transactions, sessions, dead-lettering, each message handled once | **Commands and business workflows** ("process payment 5001") where every message must be handled exactly once by one worker |
| **IoT Hub** | Event Hubs–compatible ingestion **plus device management** and cloud-to-device messages | Fleets of IoT devices |

Rule of thumb: **Event Hubs = a stream of facts to analyze. Service Bus = messages to act on. Event Grid = notifications to react to.**

---

## Part C — Azure Stream Analytics (ASA)

### 1. What it is

A **fully managed, serverless real-time analytics service**. You write a **SQL-like query** (Stream Analytics Query Language) over streams, and ASA runs it **continuously**. There's no cluster to manage and no code beyond SQL (plus optional JavaScript/C# UDFs). Typical latency is **sub-second to seconds**.

### 2. Job anatomy

```
Stream Analytics job  asa-ecom-realtime
├── Inputs
│   ├── Stream:    [evh-orders]      ← Event Hubs (consumer group cg-asa), IoT Hub, or Blob/ADLS
│   ├── Stream:    [evh-payments]
│   └── Reference: [ref-products]    ← Blob/ADLS file or Azure SQL: slowly changing lookup data
├── Query  (SQL)
└── Outputs
    ├── [pbi-live-sales]     Power BI (real-time dashboard)
    ├── [evh-fraud-alerts]   Event Hubs → Function/Logic App sends alerts
    ├── [sql-order-metrics]  Azure SQL Database
    └── [adls-silver]        ADLS Gen2 (Parquet / Delta Lake)
```

- **Stream inputs:** Event Hubs, IoT Hub, Blob/ADLS (new files treated as a stream).
- **Reference data:** a static or slowly changing table **loaded into memory** for lookups (product catalog, customer tier). It can refresh on a schedule.
- **Outputs:** Blob/ADLS (CSV, JSON, Parquet, **Delta Lake**), Azure SQL, Synapse, Cosmos DB, **Power BI**, Event Hubs, Service Bus, Azure Functions, Table storage, and more.
- **Authenticate with the job's managed identity** wherever the input or output supports it.

### 3. Query language basics

It's familiar SQL plus **time**. Every event carries a timestamp, and `GROUP BY` on a stream **must include a window**.

**`TIMESTAMP BY`** chooses **event time**. Without it, ASA uses **arrival time** (`EventEnqueuedUtcTime` for Event Hubs).

```sql
-- Live revenue every 1 minute (tumbling) → Power BI
SELECT
    System.Timestamp() AS window_end,        -- end time of the window
    COUNT(*)           AS orders,
    SUM(total_amount)  AS revenue
INTO [pbi-live-sales]
FROM [evh-orders] TIMESTAMP BY order_ts
WHERE status = 'created'
GROUP BY TumblingWindow(minute, 1)
```

**Window functions:**

| Window | ASA syntax | Example |
|---|---|---|
| Tumbling | `TumblingWindow(minute, 5)` | Orders per 5 minutes |
| Hopping | `HoppingWindow(minute, 10, 5)` (size 10, hop 5) | "Last 10 minutes, updated every 5" |
| Sliding | `SlidingWindow(minute, 1)` | Output **only when an event enters or leaves**. "More than 3 orders in any 1-minute span" |
| Session | `SessionWindow(minute, 30, 120)` (timeout 30, max 120) | A user's browsing session |
| Snapshot | `GROUP BY System.Timestamp()` | Events with exactly the same timestamp |

**Several outputs from one job**, using `WITH` steps (like CTEs):

```sql
WITH enriched AS (
    SELECT o.order_id, o.customer_id, o.card_hash, o.country, o.total_amount,
           p.category                                         -- reference data join: no time bound needed
    FROM [evh-orders] o TIMESTAMP BY order_ts
    JOIN [ref-products] p ON o.product_id = p.product_id
)

-- 1) Fraud rule: same card, 3+ orders from 2+ countries within any 1 minute
SELECT card_hash, COUNT(*) AS orders, COUNT(DISTINCT country) AS countries,
       System.Timestamp() AS detected_at
INTO [evh-fraud-alerts]
FROM enriched
GROUP BY card_hash, SlidingWindow(minute, 1)
HAVING COUNT(*) >= 3 AND COUNT(DISTINCT country) >= 2

-- 2) Revenue by category every 5 minutes → SQL
SELECT category, SUM(total_amount) AS revenue, System.Timestamp() AS window_end
INTO [sql-order-metrics]
FROM enriched
GROUP BY category, TumblingWindow(minute, 5)
```

**Stream-to-stream join:** it **must have a time bound** (`DATEDIFF`) so ASA knows how long to keep state:

```sql
-- Orders that were NOT paid within 15 minutes (abandoned checkout)
SELECT o.order_id, o.customer_id
INTO [evh-abandoned]
FROM [evh-orders] o TIMESTAMP BY order_ts
LEFT JOIN [evh-payments] p TIMESTAMP BY paid_ts
  ON o.order_id = p.order_id
 AND DATEDIFF(minute, o, p) BETWEEN 0 AND 15
WHERE p.order_id IS NULL
```

**Other useful features:**

- `LAG()`, `ISFIRST()`, `LAST()` for comparing with earlier events (e.g. "the status changed from X to Y").
- Built-in **anomaly detection** (`AnomalyDetection_SpikeAndDip`, `AnomalyDetection_ChangePoint`) for spotting sudden spikes or drops.
- Geospatial functions, JavaScript/C# **UDFs**, and `CROSS APPLY GetArrayElements()` to flatten JSON arrays (such as the items in an order). The examples in this unit keep one `product_id` per order event to stay short. A real multi-item order would carry an `items` array flattened this way.

### 4. Time handling policies

| Setting | Meaning |
|---|---|
| **Late arrival tolerance** | How late (event time vs arrival time) an event can be and still count |
| **Out-of-order tolerance** | How long ASA waits to put events back in order |
| **Action for events outside tolerance** | **Adjust** (change the timestamp so it's counted) or **Drop** |

This is ASA's version of the watermark. More tolerance means more complete results, but output comes later.

### 5. Scaling, reliability, and operations

- **Streaming Units (SUs):** the compute (CPU and memory) given to a job. More complex queries, more state, and more partitions need more SUs. Watch the **SU % utilization** metric and keep it under about 80%.
- **Parallelism:** a job scales best when it's **"embarrassingly parallel"**: input partitions → query steps → output partitions all line up, so each partition is processed independently. `PARTITION BY PartitionId` makes this explicit (newer compatibility levels do much of it automatically).
- **Delivery:** ASA processes events exactly once internally, but most outputs get **at-least-once** delivery. Design sinks to handle duplicates (upsert on a key in SQL or Cosmos DB).
- **Starting a job:** *Now*, *Custom time*, or **When last stopped** (resumes without gaps after maintenance).
- **Monitoring:** metrics for **watermark delay** (how far behind real time the job is), input/output events, **backlogged input events**, runtime errors, and **data conversion errors** (bad JSON or types). Send diagnostic logs to Log Analytics (Unit 11).
- **Dev workflow:** test queries on **sample data** in the portal, use the VS Code extension for local testing, and use CI/CD with ARM/Bicep (Unit 9).
- **Cost:** you pay per SU-hour **while the job runs**, whether or not events are flowing. Stop dev jobs you aren't using.

➕ **No-code editor:** a drag-and-drop ASA designer inside **Event Hubs** (for example, "capture to ADLS as Parquet/Delta" or "filter and route"), which generates an ASA job for you.

### 6. When to use ASA

| ✅ Good fit | ❌ Poor fit |
|---|---|
| SQL-skilled team, fast time to value | Complex logic, ML models, big custom libraries |
| Windowed aggregates, filters, alerts, simple joins | Very large state or long windows (days) |
| Feeding Power BI, SQL, Cosmos DB in real time | Unifying with batch code and the lakehouse (Databricks fits better) |

➕ **Added — Microsoft Fabric Real-Time Intelligence** is the newer SaaS direction: **Eventstream** (no-code ingestion and routing), **Eventhouse / KQL database** (fast analytics on events with KQL), **Real-Time Dashboards**, and **Activator** (alerts and actions). ASA is still fully supported and widely used, much like Synapse vs Fabric in Unit 11.

---

## Part D — Structured Streaming with Azure Databricks

### 1. The model: a stream is an unbounded table

Structured Streaming (Unit 7 Spark, Unit 11 Auto Loader) treats a stream as a **table that keeps getting new rows**. You write the **same DataFrame / SQL code** as for batch, and Spark runs it **incrementally**, processing only the new data on each trigger.

```
 Input stream  ──►  [ unbounded input table ]  ──query──►  [ result table ]  ──►  sink
 (new rows appended)   rows t1, t2, t3, ...        same DataFrame ops         written per output mode
```

Batch vs streaming code differs in only a few places:

| Batch | Streaming |
|---|---|
| `spark.read` | `spark.readStream` |
| `df.write...save()` / `.saveAsTable()` | `df.writeStream...start()` / `.toTable()` |
| Runs once | Runs **until stopped**, one **micro-batch** per trigger |
| — | Needs a **checkpoint location** |

### 2. Sources and sinks

**Sources:** **Kafka** (including **Event Hubs** through its Kafka endpoint), **Auto Loader** (`cloudFiles`, new files, e.g. Event Hubs Capture output), **Delta tables** (read a table as a stream of new rows), Delta **change data feed** ➕, plus `rate` for tests.

**Sinks:** **Delta tables** (the standard choice), Kafka / Event Hubs, **`foreachBatch`** (any batch logic, such as MERGE or writing to SQL), and `memory` / `console` for debugging.

### 3. Reading Event Hubs from Databricks (Kafka connector)

```python
from pyspark.sql import functions as F, types as T

EH_NAMESPACE = "evhns-ecom-prod"
conn_str = dbutils.secrets.get("kv-scope", "evh-orders-listen")   # Key Vault–backed secret scope (Unit 11)

kafka_options = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": "orders",                              # event hub name = Kafka topic
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config":
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";',
    "kafka.group.id": "cg-databricks",                  # consumer group; must already exist in Event Hubs
    "startingOffsets": "earliest",                      # used only on the FIRST run; afterwards the checkpoint decides
    "maxOffsetsPerTrigger": "100000",                   # cap per micro-batch (rate limiting)
}

raw = spark.readStream.format("kafka").options(**kafka_options).load()
# Columns: key, value (binary), topic, partition, offset, timestamp (enqueued time), ...
```

- On Databricks the Kafka classes are **shaded**, which is why the path starts with `kafkashaded.`.
- ➕ The older dedicated `azure-event-hubs-spark` connector still exists, but the **Kafka connector is the recommended path**. OAuth with a service principal or managed identity is an option in place of a connection string.
- `startingOffsets` applies **only when there's no checkpoint**. Once a checkpoint exists, Spark always resumes from it.

### 4. Bronze → silver → gold as streams

```python
order_schema = T.StructType([
    T.StructField("order_id", T.LongType()),
    T.StructField("customer_id", T.LongType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("card_hash", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("total_amount", T.DecimalType(10, 2)),
    T.StructField("status", T.StringType()),
    T.StructField("order_ts", T.TimestampType()),
])

CKPT = "/Volumes/ecom_prod/bronze/_meta/checkpoints"

# BRONZE: land the raw payload + metadata, append only
(raw.select(F.col("value").cast("string").alias("payload"),
            "partition", "offset", F.col("timestamp").alias("enqueued_ts"))
    .writeStream
    .option("checkpointLocation", f"{CKPT}/orders_bronze")
    .trigger(processingTime="30 seconds")          # a micro-batch every 30 s
    .toTable("ecom_prod.bronze.orders_stream"))

# SILVER: parse, validate, deduplicate (Delta table as a streaming source)
silver = (spark.readStream.table("ecom_prod.bronze.orders_stream")
          .select(F.from_json("payload", order_schema).alias("o")).select("o.*")
          .filter("order_id IS NOT NULL AND total_amount >= 0")
          .withWatermark("order_ts", "10 minutes")
          .dropDuplicatesWithinWatermark(["order_id", "status"]))   # removes at-least-once duplicates

(silver.writeStream
   .option("checkpointLocation", f"{CKPT}/orders_silver")
   .toTable("ecom_prod.silver.order_events"))
```

### 5. Windows and watermarks in Spark

```python
# GOLD: revenue per 5-minute tumbling window by event time
revenue_5m = (spark.readStream.table("ecom_prod.silver.order_events")
    .filter("status = 'created'")
    .withWatermark("order_ts", "10 minutes")                     # accept events up to 10 min late
    .groupBy(F.window("order_ts", "5 minutes"))                  # tumbling
    #  F.window("order_ts", "10 minutes", "5 minutes")           → hopping (sliding in Spark's terms)
    #  F.session_window("order_ts", "30 minutes")                → session
    .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")))

(revenue_5m.writeStream
   .outputMode("append")               # a window is written ONCE, after the watermark passes its end
   .option("checkpointLocation", f"{CKPT}/revenue_5m")
   .toTable("ecom_prod.gold.revenue_5m"))
```

⚠️ **Naming trap:** Spark calls a window with a slide interval a *sliding window*. It behaves like an ASA/Kafka **hopping** window.

**How the watermark behaves here:**

- Watermark = max `order_ts` seen − 10 minutes.
- The window 10:00–10:05 is **emitted and its state dropped** once the watermark passes 10:05, which happens after an event with `order_ts` ≥ 10:15 arrives.
- An event for 10:03 that arrives after that point is **dropped**.
- Without a watermark, state for aggregations and deduplication **grows forever**.

### 6. Output modes

| Mode | Writes each trigger | Works with |
|---|---|---|
| **Append** (default) | Only **new, final** rows | Stateless queries. Aggregations **only with a watermark** (results arrive after the window closes). |
| **Update** | Only rows that **changed** since the last trigger | Aggregations. Needs a sink that can upsert (e.g. `foreachBatch` + MERGE). |
| **Complete** | The **whole result table** every time | Aggregations with small results (e.g. counts per category) |

### 7. Triggers

| Trigger | Behavior | Use |
|---|---|---|
| *(default)* | Next micro-batch **as soon as** the previous one finishes | Lowest latency with micro-batches |
| `processingTime="30 seconds"` | One micro-batch every 30 s | Balance latency and cost, fewer small files |
| `availableNow=True` | Process **everything available, then stop** | **Incremental batch** on a schedule (Unit 11 Auto Loader). Much cheaper than always-on. |
| `once=True` | Old version of availableNow (single batch) | Deprecated |
| `continuous="1 second"` | Experimental record-at-a-time mode, very limited operations | Rarely used |

**Tip:** the same streaming code can run **always-on** (seconds of latency) or **every 15 minutes with `availableNow`** (minutes of latency, much cheaper), just by changing the trigger. Choose based on how fresh the data really needs to be.

### 8. Checkpoints and exactly-once

The **checkpoint location** stores:

- The **source offsets** of each micro-batch (which Event Hubs offsets or files were processed)
- The **commit log** (which batches finished)
- The **state store** (windows, dedup keys, join buffers). ➕ On Databricks you can use the **RocksDB** state store for large state.

**End-to-end exactly-once = replayable source (Event Hubs/Kafka, files, Delta) + checkpoint + idempotent sink (Delta).** After a crash, Spark re-runs the unfinished micro-batch, and Delta ignores the duplicate commit for the same batch ID.

⚠️ Rules:

- **One checkpoint per query.** Never share one.
- **Don't delete a checkpoint** unless you mean to start over (which means reprocessing or skipping data).
- Some code changes (such as changing aggregation keys or stateful operators) **aren't compatible** with an existing checkpoint and need a new checkpoint plus a backfill plan.

### 9. Upserts with `foreachBatch`

Streaming writes to Delta are **append** by default. To keep a **current-state** table (the latest status per order), run a MERGE on each micro-batch:

```python
from delta.tables import DeltaTable
from pyspark.sql import Window

def upsert_orders(batch_df, batch_id):
    latest = (batch_df.withColumn("rn", F.row_number().over(
                  Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())))
              .filter("rn = 1").drop("rn"))                    # dedupe within the micro-batch
    (DeltaTable.forName(spark, "ecom_prod.silver.orders_current").alias("t")
       .merge(latest.alias("s"), "t.order_id = s.order_id")
       .whenMatchedUpdateAll("s.order_ts >= t.order_ts")         # ignore older, out-of-order updates
       .whenNotMatchedInsertAll()
       .execute())

(spark.readStream.table("ecom_prod.silver.order_events")
   .writeStream.foreachBatch(upsert_orders)
   .option("checkpointLocation", f"{CKPT}/orders_current")
   .start())
```

Inside `foreachBatch`, the micro-batch is a **normal batch DataFrame**, so any batch API works (MERGE, JDBC to Azure SQL, writing to several tables). Make it **idempotent**, because a batch can run again after a failure.

### 10. Joins

| Join | Needs | Example |
|---|---|---|
| **Stream–static** | Nothing special. The static side (a Delta table) is re-read each micro-batch. | Enrich orders with `silver.products` |
| **Stream–stream** | **Watermarks on both sides + a time-range condition**, so state can be cleaned up | Orders joined to payments within 15 minutes |

```python
orders   = spark.readStream.table("ecom_prod.silver.order_events").withWatermark("order_ts", "10 minutes")
payments = spark.readStream.table("ecom_prod.silver.payments").withWatermark("paid_ts", "10 minutes")

paid = orders.alias("o").join(payments.alias("p"), F.expr("""
        o.order_id = p.order_id AND
        p.paid_ts BETWEEN o.order_ts AND o.order_ts + INTERVAL 15 MINUTES"""))
```

### 11. Running in production

- **Lakeflow Jobs** (Unit 11) run streaming queries with **retries on failure** (an always-on job restarts from its checkpoint) or on a schedule with `availableNow`.
- **Lakeflow Declarative Pipelines** (formerly DLT): **streaming tables** are exactly this, declared instead of hand-written. Expectations handle data quality, and checkpoints are managed for you. For a new bronze → silver streaming pipeline, this is often the simplest option.
- **Monitoring:**
  - `query.status` and `query.lastProgress` (input rows/sec vs **processed rows/sec**, batch duration)
  - The Spark UI **Structured Streaming tab**
  - A `StreamingQueryListener` to push metrics to Log Analytics
  - **Consumer lag** in Event Hubs. If processed rows/sec stays below input rows/sec, you're falling behind.
- **Small files:** frequent micro-batches create many small Delta files. Use a longer trigger interval, **optimized writes / auto compaction**, and `OPTIMIZE` / liquid clustering (Unit 11).
- **Compute:** a job cluster or serverless. Size it for **peak** input rate. Autoscaling works less smoothly for streaming, so test it.

### 12. ASA vs Databricks Structured Streaming

| | **Stream Analytics** | **Databricks Structured Streaming** |
|---|---|---|
| Language | SQL (+ JS/C# UDFs) | Python / Scala / SQL (full Spark) |
| Ops | Serverless, SUs | Clusters or serverless, more knobs |
| Latency | Sub-second to seconds | Seconds (micro-batch) |
| Strengths | Quick alerts, windowed aggregates, Power BI output | Complex logic, big state, ML, **same code as batch**, Delta lakehouse |
| Typical role | **Hot path**: dashboards and alerts | **Lakehouse path**: bronze/silver/gold tables |

Many architectures use **both**, reading the same event hub through **separate consumer groups**.

---

## Part E — End-to-End Real-Time Architecture ➕ Added

```
 Web / mobile app ──(orders, clicks; partition key = order_id / session_id)──► Event Hubs  evhns-ecom-prod
                                                                               ├─ orders      (8 partitions)
                                                                               └─ clickstream (16 partitions)
                          ┌────────────────────────────┬───────────────────────────┼─────────────────────────────┐
                          ▼ cg-asa                     ▼ cg-databricks             ▼ Capture (Avro/Parquet)
                 Stream Analytics job           Databricks Structured              ADLS bronze/eventhubs/…
                 (SQL, windows, fraud rule)     Streaming / Declarative            (raw archive, replay,
                  ├─► Power BI  live sales       Pipelines                          history beyond retention)
                  ├─► Event Hubs fraud-alerts    ├─► bronze.orders_stream   (append)
                  │     └─► Function/Logic App   ├─► silver.order_events   (parse, validate, dedupe)
                  │           → Teams / block    ├─► silver.orders_current (foreachBatch MERGE)
                  └─► Azure SQL order metrics    └─► gold.revenue_5m, gold.funnel (windowed)
                                                         │
                                                         ▼
                                          Databricks SQL / Synapse serverless → Power BI (near-real-time)
                                          Nightly batch (Unit 11) still builds the star schema from silver
```

**Design checklist:**

- [ ] Choose **partition keys** that keep needed ordering (per order) without hot keys
- [ ] Use **one consumer group per consuming application**
- [ ] Use **event time** (`TIMESTAMP BY`, `withWatermark`) and decide how much lateness you accept
- [ ] Assume **at-least-once**: add dedup keys (`event_id`) and idempotent sinks (Delta, MERGE, upserts)
- [ ] Give every Spark query its own **checkpoint**, and never lose it
- [ ] Turn on **Capture** or keep a bronze copy so you can **replay**
- [ ] Use **managed identities / Entra RBAC**, private endpoints, and Key Vault for any remaining secrets
- [ ] Monitor **throttling, consumer lag, watermark delay, SU %, processed vs input rate**
- [ ] Ask whether **`availableNow` every N minutes** is fresh enough, since it costs much less than always-on

---

## Key Terms Cheat Sheet

- **Bounded vs unbounded data:** batch vs stream input
- **Event time / ingestion time / processing time:** when it happened / arrived / was processed
- **Out-of-order and late events:** the normal situation in streaming
- **Tumbling / hopping / sliding / session / snapshot windows**
- **Watermark:** "all events up to time T have probably arrived". It finalizes windows and bounds state.
- **State + checkpointing:** what the job remembers, saved so it can recover
- **At-most / at-least / exactly-once:** exactly-once = replayable source + checkpoint + idempotent sink
- **Backpressure / consumer lag**
- **Micro-batch vs continuous processing**
- ➕ **Lambda vs Kappa architecture**
- **Event Hubs:** managed, log-based event streaming with a Kafka-compatible endpoint (port 9093, SASL_SSL)
- **Namespace = cluster, event hub = topic**, partition, consumer group, offset, sequence number
- **Partition:** unit of order and parallelism. Order is only within a partition. Count is fixed in Standard.
- **Partition key:** same key → same partition → ordered. Watch for hot keys.
- **Retention + replay:** reading doesn't delete events
- **Consumer group:** independent view per application. **Checkpoint store:** the consumer's saved offsets (Blob).
- **Tiers:** Basic / Standard (TUs, auto-inflate) / Premium (PUs) / Dedicated (CUs). **1 TU ≈ 1 MB/s in, 2 MB/s out.**
- **Capture:** automatic archive to ADLS/Blob (Avro/Parquet)
- **Auth:** Event Hubs Data Sender / Receiver / Owner roles + managed identity. SAS keys are legacy.
- ➕ **Schema Registry; Event Hubs vs Event Grid vs Service Bus vs IoT Hub**
- **Stream Analytics:** serverless SQL over streams. Inputs (stream + **reference data**), query, outputs.
- **`TIMESTAMP BY`, `System.Timestamp()`, `TumblingWindow` / `HoppingWindow` / `SlidingWindow` / `SessionWindow`**
- **Stream–stream join needs `DATEDIFF`; reference-data join doesn't**
- **Late arrival / out-of-order tolerance, adjust vs drop**
- **Streaming Units (SUs), embarrassingly parallel jobs, `PARTITION BY`, watermark delay metric**
- ➕ **Fabric Real-Time Intelligence:** Eventstream, Eventhouse (KQL), Activator
- **Structured Streaming:** a stream as an unbounded table. `readStream` / `writeStream`.
- **Kafka source for Event Hubs:** `kafkashaded...PlainLoginModule`, `$ConnectionString`, `startingOffsets` (first run only), `maxOffsetsPerTrigger`
- **`withWatermark`, `window`, `session_window`, `dropDuplicatesWithinWatermark`**
- **Output modes:** append / update / complete
- **Triggers:** default, `processingTime`, `availableNow`, continuous
- **Checkpoint location:** offsets + commit log + state store (RocksDB). One per query.
- **`foreachBatch`:** batch logic (MERGE) per micro-batch. Must be idempotent.
- **Stream–static vs stream–stream joins** (the latter needs watermarks + a time bound)
- **Lakeflow Declarative Pipelines streaming tables, Lakeflow Jobs retries, small-file compaction**

---

## Practice Questions

1. **List three differences between batch and stream processing.**
   Bounded vs unbounded input. Scheduled runs that stop vs a job that's always running. Minutes-to-hours latency vs seconds. (Also: a streaming job must deal with late data, state, and failures while it runs.)

2. **Give one use case that needs streaming and one that doesn't.**
   Card fraud alerts need streaming. A nightly sales star schema doesn't.

3. **What's the difference between event time and processing time, and which should a "revenue per 5 minutes" metric use?**
   Event time is when it happened. Processing time is when your job handled it. Use event time, so delayed events still land in the right window.

4. **Compare tumbling, hopping, sliding, and session windows.**
   Tumbling: fixed size, no overlap. Hopping: fixed size with overlap (size + hop). Sliding: output whenever an event enters or leaves the window. Session: activity-based, closes after a gap of inactivity.

5. **What is a watermark, and what's the trade-off in choosing its delay?**
   An estimate that all events up to time T have arrived. It's used to finalize windows and drop state. A longer delay gives more complete results but more latency and state. A shorter delay gives faster results but drops more late events.

6. **Why is exactly-once described as "end-to-end"? What three things does it need?**
   Every link must cooperate. It needs a replayable source, checkpointed progress, and an idempotent or transactional sink.

7. **Event Hubs is at-least-once. What does that mean for your consumer?**
   The same event can be delivered twice (for example, after a crash before the checkpoint). Processing must be idempotent, or it must deduplicate on an event ID.

8. ➕ **Lambda vs Kappa?**
   Lambda has separate batch and speed layers (two codebases). Kappa has one streaming path and reprocesses by replaying the log.

9. **Map Kafka terms to Event Hubs: cluster, topic, partition, consumer group.**
   Namespace, event hub, partition, consumer group.

10. **Why do partitions matter, and what does order look like across them?**
    They're the unit of parallelism (about one reader per partition per consumer group). Order is guaranteed only within a partition, never across the whole event hub.

11. **All events for one order must be processed in order. How?**
    Send them with `partition_key = order_id`. The same key always goes to the same partition, which keeps them in order.

12. **Your event hub has 4 partitions and you start 8 consumers in one consumer group. What happens?**
    Only 4 do useful work (one per partition). The rest sit idle. To scale further you need more partitions, which only Premium/Dedicated can add after creation.

13. **What is a consumer group, and why give Stream Analytics and Databricks separate ones?**
    An independent view of the event hub with its own offsets. Separate groups let each application read at its own pace without affecting the other.

14. **Who tracks what a consumer has processed in Event Hubs?**
    The consumer, by checkpointing offsets to a store (usually Blob storage). Event Hubs only retains events.

15. **Reading events deletes them from Event Hubs: true or false?**
    False. Events stay until retention expires, which enables replay and multiple consumers.

16. **Your producer gets `ServerBusy` errors on a Standard namespace. Why, and what do you do?**
    It's being throttled for exceeding its throughput units. Add TUs or turn on auto-inflate (or move to Premium). Also batch your sends.

17. **What does Event Hubs Capture do, and why is it useful?**
    It writes events automatically to ADLS/Blob (Avro/Parquet), giving a raw bronze archive for replay and history beyond retention, with no code.

18. **How does an existing Kafka app connect to Event Hubs?**
    Change the configuration only: bootstrap server `<namespace>.servicebus.windows.net:9093`, SASL_SSL, and PLAIN with `$ConnectionString` (or OAUTHBEARER with Entra). Keep the code.

19. **How should producers and consumers authenticate?**
    With Entra ID and managed identities, using the Event Hubs Data Sender/Receiver roles. SAS keys are legacy. Keep them in Key Vault if you must use them.

20. ➕ **Event Hubs, Service Bus, or Event Grid for: (a) clickstream analytics, (b) "charge payment for order 5001" commands, (c) "a blob was created" notifications?**
    (a) Event Hubs, (b) Service Bus, (c) Event Grid.

21. **What are the three parts of a Stream Analytics job?**
    Inputs (stream and reference), a SQL query, and outputs.

22. **What does `TIMESTAMP BY` do, and what's used without it?**
    It sets event time from a field in the event. Without it, ASA uses arrival time (EventEnqueuedUtcTime).

23. **Write an ASA query for order count and revenue per 1-minute tumbling window.**
    `SELECT System.Timestamp() AS window_end, COUNT(*), SUM(total_amount) INTO [out] FROM [in] TIMESTAMP BY order_ts GROUP BY TumblingWindow(minute, 1)`

24. **"Alert if a card has 3+ orders within any 1-minute span." Which window?**
    A sliding window: `GROUP BY card_hash, SlidingWindow(minute, 1) HAVING COUNT(*) >= 3`.

25. **Why must an ASA stream–stream join include `DATEDIFF`, but a reference-data join doesn't?**
    Without a time bound, ASA would have to keep every event from both streams forever. Reference data is a finite lookup table held in memory.

26. **What is reference data in ASA? Give an example.**
    Static or slowly changing lookup data (from Blob or SQL) that's joined to the stream, such as a product catalog for adding the category.

27. **What do the late arrival and out-of-order policies control?**
    How late or out-of-order an event can be and still be included, and whether events outside those limits are adjusted or dropped.

28. **Your ASA job's watermark delay keeps growing and SU utilization is at 95%. What do you do?**
    The job is falling behind. Add SUs, make the query parallel (partition-aligned input, query, and output), and simplify heavy steps.

29. **ASA outputs are mostly at-least-once. How do you avoid duplicate rows in Azure SQL?**
    Write to a table with a key and upsert (or deduplicate downstream).

30. **When would you choose ASA over Databricks for streaming?**
    For a SQL team, fast setup, windowed aggregates or alerts, and Power BI/SQL outputs, with no cluster to run. Choose Databricks for complex logic, large state, ML, or lakehouse tables.

31. **Explain the "unbounded table" model of Structured Streaming.**
    The stream is a table that keeps growing. You write normal DataFrame queries, and Spark runs them incrementally on each new micro-batch.

32. **What code changes turn a batch job into a streaming one?**
    `read` → `readStream`, `write` → `writeStream` with a checkpoint location, and `.start()` / `.toTable()`. The transformations stay the same.

33. **Why does `startingOffsets = "earliest"` not reprocess everything after a restart?**
    It's used only when there's no checkpoint. Once one exists, Spark resumes from the checkpointed offsets.

34. **What is stored in a streaming checkpoint?**
    Source offsets per micro-batch, the commit log, and the state store (windows, dedup keys, join buffers).

35. **Two streaming queries write to different tables. Can they share a checkpoint?**
    No. Each query needs its own checkpoint location.

36. **A windowed aggregation in append mode writes nothing for 10 minutes. Why?**
    In append mode, a window is written only after the watermark passes its end, so results appear after roughly window size + watermark delay.

37. **Append, update, or complete: which one for (a) cleaned events into bronze/silver, (b) a small count per category shown in full each time, (c) changing aggregates upserted to a table?**
    (a) Append, (b) complete, (c) update (with `foreachBatch` + MERGE).

38. **What happens to state if you run `dropDuplicates` on a stream without a watermark?**
    It grows forever, because Spark must remember every key it has seen. Use `withWatermark` + `dropDuplicatesWithinWatermark`.

39. **How do you keep a "latest status per order" table from a stream of order events?**
    `foreachBatch`: deduplicate within the micro-batch, then MERGE into Delta, updating only when the incoming event is newer.

40. **Stream–static vs stream–stream join: what does each need?**
    Stream–static needs nothing special (the static Delta table is re-read each micro-batch). Stream–stream needs watermarks on both sides and a time-range join condition.

41. **When would you use `trigger(availableNow=True)` instead of an always-on stream?**
    When data a few minutes old is fresh enough. It processes everything new and stops, so it can run on a schedule at a fraction of the cost with the same code.

42. **How do you tell if a Structured Streaming query is falling behind?**
    Processed rows/sec stays below input rows/sec, batch duration keeps rising, and Event Hubs consumer lag grows.

43. **Why do streaming Delta tables often have a small-files problem, and how do you fix it?**
    Every micro-batch writes files. Use longer trigger intervals, optimized writes / auto compaction, and regular `OPTIMIZE` (or liquid clustering).

44. **Where do Lakeflow Declarative Pipelines fit in streaming?**
    Their streaming tables are declarative Structured Streaming: incremental processing, managed checkpoints, and expectations for data quality. They're a simple way to build bronze → silver streaming.

45. ➕ **Sketch a real-time architecture for live sales plus fresh lakehouse tables.**
    App → Event Hubs (partition key = order_id) → (a) ASA with consumer group `cg-asa` → Power BI and fraud alerts, (b) Databricks with `cg-databricks` → bronze/silver/gold Delta, (c) Capture → ADLS for replay. Nightly batch still builds the star schema.

46. ➕ **Your streaming job was down for 6 hours. Event Hubs retention is 7 days. What happens when you restart it?**
    It resumes from its checkpoint and catches up on the backlog (use `maxOffsetsPerTrigger` to control batch size). Nothing is lost because the events are still retained. With downtime longer than retention, you'd have to recover from the Capture archive.
