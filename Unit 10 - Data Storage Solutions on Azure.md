# Unit 10 — Data Storage Solutions on Azure: Study Guide

**Scope:** Azure Blob Storage and Azure Data Lake Storage (ADLS) Gen2 · Managed relational databases (Azure SQL Database, Azure Database for PostgreSQL) · Managed NoSQL databases (Azure Cosmos DB concepts)

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. Unit 9 built the network, the identities and the governance. This unit fills in the stores: the **lake** (ADLS Gen2), the **transactional database** (Azure SQL or PostgreSQL), and a **NoSQL store** for the product catalog and shopping carts (Cosmos DB).

---

## Part A — Azure Blob Storage

### 1. The storage account (quick recap from Unit 9)

A **storage account** is the top-level container for Azure Storage services (Blob, Files, Queues, Tables). It sets the **name/endpoint, redundancy (LRS/ZRS/GRS/GZRS), default access tier, networking, and encryption** for everything inside it.

| Account type | Use |
|---|---|
| **Standard general-purpose v2 (StorageV2)** | The default for almost everything, including data lakes |
| **Premium block blobs** | Low latency and high transaction rates on SSD (for example, many small files or streaming workloads) |
| Premium file shares / page blobs | Azure Files / VM disks. Not for data engineering. |

**Endpoints** (one per service):

```
https://stecomlakeprod.blob.core.windows.net   ← Blob API
https://stecomlakeprod.dfs.core.windows.net    ← ADLS Gen2 (Data Lake) API
https://stecomlakeprod.queue.core.windows.net
https://stecomlakeprod.file.core.windows.net
```

### 2. The Blob resource hierarchy

```
Storage account   stecomlakeprod
└── Container     bronze                         (like a top-level bucket)
    └── Blob      orders/2026/09/25/orders.json  (the object; the "/" is just part of its name)
```

**Blob URL:** `https://stecomlakeprod.blob.core.windows.net/bronze/orders/2026/09/25/orders.json`

- **Container names:** 3–63 characters, lowercase letters, numbers and hyphens.
- **Flat namespace:** in plain Blob storage, **folders don't exist**. `orders/2026/` is only a **name prefix**. Tools *show* folders, but renaming a "folder" means copying and deleting every blob under it. (ADLS Gen2 fixes this. See Part B.)

### 3. Blob types

| Type | Built for | Max size | DE use |
|---|---|---|---|
| **Block blob** | Files uploaded in blocks, then committed | ~190 TiB | **Almost everything:** CSV, JSON, Parquet, Delta, images, backups |
| **Append blob** | Append-only writes | ~195 GiB | Logs, audit trails |
| **Page blob** | Random read/write in 512-byte pages | 8 TiB | VM disks. Not for data. |

### 4. Access tiers (deeper than Unit 9)

| Tier | Online? | Storage cost | Access cost | **Minimum days** (early deletion fee) | Use |
|---|---|---|---|---|---|
| **Hot** | Yes | Highest | Lowest | — | Current data, Silver/Gold |
| **Cool** | Yes | Lower | Higher | **30** | Bronze older than a month |
| **Cold** | Yes | Lower still | Higher still | **90** | Rarely read, but must be readable right away |
| **Archive** | **No (offline)** | Lowest | Highest + **rehydration** | **180** | Compliance copies, raw history |

- **Default tier** is set on the account. Individual blobs can override it.
- **Early deletion fee:** delete or move a Cool blob after 10 days and you still pay for the remaining 20.
- **Archive rehydration:** to read an archived blob you first change its tier to Hot/Cool/Cold (or copy it). **Standard priority takes up to 15 hours. High priority is usually under 1 hour** for smaller objects and costs more.
- ⚠️ **Archive isn't supported on ZRS, GZRS or RA-GZRS accounts**, only LRS/GRS/RA-GRS. Our Unit 9 lake is ZRS, so Bronze ages to **Cold**, not Archive.

➕ **Added — lifecycle management policy** (a JSON rule set on the account, run by Azure about once a day):

```json
{
  "rules": [{
    "name": "bronze-age-out",
    "enabled": true,
    "type": "Lifecycle",
    "definition": {
      "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["bronze/"] },
      "actions": { "baseBlob": {
        "tierToCool": { "daysAfterModificationGreaterThan": 30 },
        "tierToCold": { "daysAfterModificationGreaterThan": 90 },
        "delete":     { "daysAfterModificationGreaterThan": 2555 }
      }}
    }
  }]
}
```

⚠️ The 2,555-day (about 7-year) delete fits Bronze folders **without** personal data. Folders that hold PII need a much shorter rule, and must stay deletable, so erasure requests can be honored (Unit 15).

### 5. Authorizing access to blobs

From **most to least recommended**:

| Method | How it works | Notes |
|---|---|---|
| **Microsoft Entra ID + RBAC** | Token from Entra, data-plane role (Storage Blob Data Reader/Contributor/Owner) | **Preferred.** Auditable, no secrets. Uses managed identities (Unit 9). |
| **User delegation SAS** | A **Shared Access Signature** (a signed URL with permissions and an expiry) signed with **Entra credentials** | Best SAS type. Revocable by revoking the user delegation key. |
| **Service SAS / Account SAS** | SAS signed with the **account key** | Hard to revoke (you must rotate the key). Keep expiry short. |
| **Shared Key (account keys)** | Two 512-bit keys that give **full access** to the whole account | Like a root password. **Disable shared key access** where you can. |
| **Anonymous public read** | No auth at all | **Disabled by default** on new accounts. Keep it off for data. |

**A SAS token looks like:** `...orders.json?sv=2024-11-04&sp=r&se=2026-09-26T00:00Z&sr=b&sig=...` (`sp=r` = read permission, `se` = expiry, `sig` = signature). **Anyone who has the URL has the access**, so treat it like a password.

### 6. Data protection features

| Feature | Protects against |
|---|---|
| **Soft delete** (blobs and containers) | Accidental deletes. Recover within the retention period (e.g. 7–14 days). |
| **Blob versioning** | Accidental overwrites. Every write keeps the previous version. |
| ➕ **Point-in-time restore** | Rolling block blobs back to an earlier state (needs versioning + change feed + soft delete) |
| ➕ **Snapshots** | Manual read-only copy of a blob at a moment |
| ➕ **Immutable storage (WORM)** | "Write once, read many". **Time-based retention** or **legal hold**. Nobody can delete or change the data, not even an Owner. Used for compliance. |
| ➕ **Change feed** | Ordered log of every create/update/delete in the account. Useful for auditing and incremental processing. |
| **Encryption at rest** | Always on (AES-256). **Microsoft-managed keys** by default, or **customer-managed keys** in Key Vault. |
| **Encryption in transit** | HTTPS/TLS. Set **minimum TLS 1.2** and "secure transfer required". |

---

## Part B — Azure Data Lake Storage (ADLS) Gen2

### 1. What ADLS Gen2 is

**ADLS Gen2 = Blob Storage + a hierarchical namespace (HNS).** It isn't a separate service. It's a **storage account with `isHnsEnabled = true`**. You get Blob's pricing, tiers, redundancy and lifecycle features, plus:

| Feature | Flat namespace (plain Blob) | **Hierarchical namespace (ADLS Gen2)** |
|---|---|---|
| Directories | Fake (name prefixes) | **Real directory objects** |
| Rename/move a folder | Copy + delete **every blob**. Slow and **not atomic**. | **One atomic metadata operation** |
| Delete a folder | Delete every blob | One operation |
| Security | Container/account level (RBAC, SAS) | RBAC **+ POSIX-style ACLs on each folder and file** |
| Hadoop/Spark driver | `wasbs://` (legacy) | **ABFS driver:** `abfss://` |

**Why this matters for data engineering:** Spark, Hive and Delta Lake write output to a temp folder and then **rename** it into place when the job commits. On a flat namespace that rename is slow and can leave half-written results. With HNS it's fast and atomic.

- ➕ **Gen1** was a separate, older service. It was **retired in 2024**. "ADLS" today means Gen2.
- HNS is chosen **at account creation**. An existing account **can be upgraded** to HNS, but it's **one-way** (you can't turn it off).

### 2. Addressing data: the ABFS URI

```
abfss://<container>@<account>.dfs.core.windows.net/<path>
abfss://bronze@stecomlakeprod.dfs.core.windows.net/orders/2026/09/25/
```

- `abfss` = ABFS **over TLS** (always use the `s`).
- In ADLS terms a container is also called a **file system**.

```python
# PySpark (Unit 7) on Databricks, which authenticates with its managed identity / Unity Catalog
df = spark.read.json("abfss://bronze@stecomlakeprod.dfs.core.windows.net/orders/2026/09/")
df.write.format("delta").mode("overwrite") \
  .save("abfss://silver@stecomlakeprod.dfs.core.windows.net/orders/")
```

```python
# pandas via adlfs (Unit 6)
import pandas as pd
from azure.identity import DefaultAzureCredential
df = pd.read_parquet(
    "abfss://gold@stecomlakeprod.dfs.core.windows.net/sales/daily_revenue/",
    storage_options={"account_name": "stecomlakeprod", "credential": DefaultAzureCredential()},
)
```

### 3. Access control in ADLS Gen2: RBAC + ACLs

There are **two layers**:

| Layer | Granularity | Example |
|---|---|---|
| **Azure RBAC** (data-plane roles) | Account or **container** | Storage Blob Data Reader on the `gold` container |
| **ACLs (access control lists)** | **Individual directory or file** | Group `grp-finance` can read `gold/finance/` only |

**Evaluation order:**

```
Request arrives → Does an RBAC role assignment grant it?
                   ├── Yes → ALLOWED (ACLs are never checked)
                   └── No  → Check the ACLs on the path → allow or deny
```

So **RBAC is broad and wins first**. If you want someone limited to one folder, **don't** give them a data role on the whole container. Give them only ACLs (plus Reader on the account if they need to see it in the portal).

- **Storage Blob Data Owner** = **superuser**: full access to all data and can set ACLs.

**ACL permissions (POSIX-style):**

| Permission | On a **file** | On a **directory** |
|---|---|---|
| **r** (read, 4) | Read contents | **List** its children (needs **x** too) |
| **w** (write, 2) | Write/append | **Create/delete** children (needs **x** too) |
| **x** (execute, 1) | Not used in ADLS | **Traverse** (pass through it to reach children) |

**The rule that trips everyone up:** to read `gold/sales/2026/file.parquet`, a principal needs **x on every parent directory** (the container root, `sales`, `2026`) **and r on the file**.

**ACL entries** on each item:

```
user::rwx                  ← owning user
group::r-x                 ← owning group
group:<grp-finance-id>:r-x ← named group
mask::r-x                  ← max permission for named users/groups and the owning group
other::---                 ← everyone else
```

**Access ACL vs default ACL:**

| | **Access ACL** | **Default ACL** |
|---|---|---|
| Applies to | Controls access **to this item** | A **template** copied to **new** children |
| Exists on | Files and directories | **Directories only** |

⚠️ Default ACLs only affect **items created later**. Existing children keep their old ACLs. To fix existing data, apply the ACL **recursively** (portal, `az storage fs access update-recursive`, or `set_access_control_recursive` in the SDK).

**Best practices:**

- Assign ACLs to **Entra security groups**, not individual users. Each item holds **at most 32 ACL entries**, and group membership is easier to change than thousands of ACLs.
- Set **default ACLs on top-level folders early**, before data lands.
- Use **containers as the main security boundary** (bronze / silver / gold) and ACLs for sub-areas.

### 4. Organizing the lake

**Zones / layers** (the medallion pattern from Units 7–8):

```
stecomlakeprod
├── bronze/   raw, as-received (JSON/CSV/Parquet), append-only, immutable
│   └── <source>/<entity>/yyyy/mm/dd/          e.g. shopify/orders/2026/09/25/
├── silver/   cleaned, deduplicated, typed (Delta/Parquet)
│   └── <domain>/<entity>/                     e.g. sales/orders/  (partitioned by order_date)
└── gold/     business-ready aggregates, star schemas (Unit 5)
    └── <data product>/                        e.g. sales/daily_revenue/
```

➕ **Added — other common zones:** `landing/` (temporary drop zone before validation), `sandbox/` or `workspace/` (analysts' experiments), `quarantine/` (rows that failed quality checks).

**Performance guidance:**

- **Avoid the small-file problem.** Thousands of tiny files mean slow listing, slow Spark jobs and higher transaction costs. Aim for files around **hundreds of MB up to ~1 GB** (compact with Delta `OPTIMIZE`).
- **Columnar formats** (Parquet/Delta) for Silver/Gold. Compress (Snappy/ZSTD).
- **Partition by columns you filter on** (usually a date), but not so fine that each partition holds only tiny files.
- **Put time last in Bronze paths** (`source/entity/yyyy/mm/dd`) so each dataset's security and lifecycle rules apply to one folder tree. Inside that tree, the date is the first partition level. Unit 13 writes it Hive-style (`ingest_date=2026-09-24/`), which Spark reads as a column.

### 5. Networking the lake (links to Unit 9)

- An ADLS account has **two endpoints, blob and dfs**. With private endpoints you normally need **one for each** (`privatelink.blob.core.windows.net` **and** `privatelink.dfs.core.windows.net`). Forgetting the `dfs` one is a classic reason Spark can't reach the lake.
- Storage firewall: **disable public network access**, or allow only selected VNets/IPs. Use **trusted Microsoft services** exceptions carefully.

### 6. Blob Storage vs ADLS Gen2: when to use which

| Use case | Choose |
|---|---|
| Analytics data lake (Spark, Databricks, Synapse, Fabric, dbt external tables) | **ADLS Gen2 (HNS on)** |
| App assets: images, PDFs, backups, static website | Blob (flat) |
| Folder-level security needed | ADLS Gen2 |
| Features that don't yet support HNS | Blob (check the feature support list; most features now support HNS) |

➕ **Added — OneLake** is Microsoft Fabric's single tenant-wide lake. It's built on ADLS Gen2 and speaks the same ABFS API. You'll see **shortcuts** from OneLake to existing ADLS accounts.

---

## Part C — Managed Relational Databases

"**Managed**" = **PaaS** (Unit 9). Microsoft handles hardware, OS, patching, backups, high availability and minor version upgrades. You handle schema, queries, indexes, security settings and data.

### 1. The Azure SQL family

**What T-SQL is.** **T-SQL (Transact-SQL)** is Microsoft's version (**dialect**) of SQL. It's the language you use to talk to **every product built on the SQL Server engine**: SQL Server, Azure SQL Database, Azure SQL Managed Instance, and the warehouses in Azure Synapse and Microsoft Fabric.

**Why it exists.** Standard (ANSI) SQL is **declarative**: you say *what* data you want (`SELECT`, `INSERT`, `JOIN`), not *how* to get it step by step. That's not enough for logic that lives inside the database. Each vendor adds its own procedural extensions, and T-SQL is Microsoft's. It adds:

| T-SQL adds | What it's for |
|---|---|
| **Variables and control flow** (`DECLARE`, `IF...ELSE`, `WHILE`) | Step-by-step logic inside the database |
| **Error handling** (`TRY...CATCH`, `THROW`) | Catch failures and roll back cleanly |
| **Transaction control** (`BEGIN TRAN`, `COMMIT`, `ROLLBACK`) | Group statements into one all-or-nothing unit (ACID, Unit 3) |
| **Stored procedures, functions, triggers** | Reusable server-side logic, e.g. a `usp_place_order` procedure the app calls |
| **Its own syntax and built-in functions** | `TOP`, `MERGE`, `IDENTITY`, `GETDATE()`, `NVARCHAR` |

**Why it matters to you:** if a source system runs on Azure SQL or SQL Server, your extract queries, incremental-load logic and any stored procedures you call are written in T-SQL. Your SQL knowledge carries over; only the dialect-specific details change.

**Every major database has its own dialect:**

| Database | Dialect / procedural language |
|---|---|
| SQL Server / Azure SQL | **T-SQL** |
| PostgreSQL | Standard SQL + **PL/pgSQL** |
| Oracle | **PL/SQL** |
| Snowflake (Unit 8) | Snowflake SQL + Snowflake Scripting |

➕ **Added — T-SQL vs PostgreSQL cheat sheet** (Unit 3 used Postgres):

| Task | PostgreSQL | T-SQL (Azure SQL) |
|---|---|---|
| First N rows | `LIMIT 10` | `SELECT TOP 10 ...` or `OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY` |
| Auto-increment | `GENERATED ALWAYS AS IDENTITY` / `SERIAL` | `IDENTITY(1,1)` |
| Unicode string | `TEXT` / `VARCHAR` | `NVARCHAR(n)` / `NVARCHAR(MAX)` |
| Current time | `now()` | `SYSUTCDATETIME()` / `GETDATE()` |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` | `MERGE` |
| String concat | `a \|\| b` | `a + b` or `CONCAT(a, b)` |
| Boolean | `BOOLEAN` | `BIT` |

**The three Azure SQL options.** All three run the SQL Server engine and use T-SQL:

| Option | Model | Compatibility with SQL Server | Use |
|---|---|---|---|
| **Azure SQL Database** | PaaS, **one database** (or a pool) | High, but no instance-level features (SQL Agent, cross-DB queries, CLR) | **New cloud apps**, OLTP sources |
| **Azure SQL Managed Instance** | PaaS, **whole instance** in your VNet | **Near 100%** (SQL Agent, cross-database queries, Service Broker) | **Lift-and-shift** of on-prem SQL Server |
| **SQL Server on Azure VMs** | **IaaS** | 100%, full OS control | Needs OS access or unsupported features |

### 2. Azure SQL Database in detail

**Logical server:** a container for databases that holds logins, firewall rules and auditing settings. It is **not** a VM.
Endpoint: `sql-ecom-prod.database.windows.net`, **port 1433** (TDS protocol).

**Deployment options:**

| | **Single database** | **Elastic pool** |
|---|---|---|
| Resources | Dedicated to one DB | **Shared** by many DBs |
| Best for | One app, predictable load | **Many DBs with different peak times**, e.g. one DB per customer in a SaaS app |

**Purchasing models:**

| | **DTU model** | **vCore model** (recommended) |
|---|---|---|
| Unit | **DTU** (Database Transaction Unit): a bundled mix of CPU, memory, I/O | Choose **vCores, memory and storage separately** |
| Tiers | Basic, Standard, Premium | **General Purpose, Business Critical, Hyperscale** |
| Flexibility | Simple, less control | More control. Can use **Azure Hybrid Benefit** (reuse existing SQL Server licenses) and reservations. |

**vCore service tiers:**

| Tier | Architecture | Max size | Use |
|---|---|---|---|
| **General Purpose** | Compute separate from **remote** premium storage | Up to ~4 TB | Most workloads, budget-friendly |
| **Business Critical** | **Local SSD**, Always On-style cluster with replicas. **One free readable secondary.** | Up to ~4 TB | Low latency, high I/O, fastest failover |
| **Hyperscale** | Distributed: compute + page servers + log service | **Up to 128 TB** | Large or fast-growing DBs, fast scaling, near-instant backups |

**Compute tiers (vCore):**

- **Provisioned:** fixed compute, billed per hour.
- **Serverless:** **autoscales** between a min and max vCore setting, billed **per second** of use. On General Purpose it can **auto-pause** after an idle period (only storage is billed while paused). The first connection after a pause has a **cold-start delay**. Good for dev/test and intermittent workloads.

➕ **Added:** Azure SQL Database has a **free offer** (a monthly allowance of vCore-seconds and storage per database) that's handy for class projects.

**High availability and business continuity:**

| Feature | What it gives |
|---|---|
| **Built-in HA** | Automatic failover inside the region. **Zone redundancy** option across availability zones. |
| **Automated backups** | Full weekly, differential every 12–24 h, **transaction log every ~5–10 min** |
| **Point-in-time restore (PITR)** | Restore to any second within **1–35 days** (default 7) as a **new** database |
| **Long-term retention (LTR)** | Keep weekly/monthly/yearly full backups for **up to 10 years** |
| **Geo-restore** | Restore from geo-replicated backups into another region |
| **Active geo-replication** | Up to **4 readable secondaries** in other regions |
| **Failover groups** | Group of DBs that fail over together, with **stable listener endpoints** so apps don't change connection strings |
| ➕ **Read scale-out** | Send reporting queries to a read-only replica with `ApplicationIntent=ReadOnly` (Premium/Business Critical/Hyperscale) |

**Security:**

| Layer | Feature |
|---|---|
| Network | **Server firewall rules** (IP ranges), "Allow Azure services" (broad, avoid in prod), **private endpoint** (recommended), disable public access |
| Authentication | **SQL authentication** (username/password) and **Microsoft Entra authentication**. **Entra-only authentication** can disable SQL logins entirely. |
| Authorization | Database roles: `db_datareader`, `db_datawriter`, `db_owner`, custom roles, `GRANT/DENY` |
| Data | **TDE** (Transparent Data Encryption), **on by default**. **Always Encrypted** (column encryption where even DBAs can't see plaintext). **Dynamic data masking** (`xxxx@xxxx.com`). **Row-level security.** |
| Monitoring | **Auditing** to storage or Log Analytics. **Microsoft Defender for SQL** (vulnerability assessment, threat detection such as SQL injection). |

➕ **Added — incremental extraction from Azure SQL.** For ingesting only changed rows into Bronze, use **Change Tracking** (which rows changed), **Change Data Capture (CDC)** (full before/after change history), or a `modified_at` watermark column (Unit 3/8 incremental pattern).

### 3. Azure Database for PostgreSQL (Flexible Server)

**Flexible Server is the only deployment option.** The older **Single Server was retired in March 2025**. It runs **community PostgreSQL**, so your Unit 3 SQL works unchanged.

Endpoint: `psql-ecom-prod.postgres.database.azure.com`, **port 5432**, **TLS required** by default.

**Compute tiers:**

| Tier | VM series | Use |
|---|---|---|
| **Burstable** | B-series (earns CPU credits when idle) | Dev/test, low-traffic apps. **No HA support.** |
| **General Purpose** | D-series | Most production workloads |
| **Memory Optimized** | E-series (more RAM per vCore) | Heavy caching, large working sets, analytics-style queries |

**Key features:**

| Feature | Detail |
|---|---|
| **High availability** | **Zone-redundant** (standby in another zone) or **same-zone** standby, with **synchronous** replication and automatic failover |
| **Backups & PITR** | Automatic, **7–35 days** retention. Optional **geo-redundant backup**. Restore creates a **new server**. |
| **Read replicas** | Asynchronous replicas (same or **other regions**) for reporting and read scaling |
| **Stop/start** | Stop a server to save compute cost (auto-restarts after 7 days). Great for dev. |
| **Storage** | Can grow online (with an **auto-grow** option), up to tens of TiB |
| **Built-in PgBouncer** | Connection pooler. Useful for FastAPI apps opening many short connections. |
| **Extensions** | Allow-list them in the `azure.extensions` server parameter, then `CREATE EXTENSION`. Popular: `pg_stat_statements`, `PostGIS`, **`pgvector`** (embeddings), `pg_cron` (scheduled jobs), **`azure_storage`** (import/export with Blob) |
| **Major version upgrade** | In place, from the portal/CLI |
| **Server parameters** | Tune `work_mem`, `max_connections`, `wal_level`, etc. (most, not all) |

**Networking (chosen at creation):**

| Mode | How |
|---|---|
| **Public access** | Public endpoint + **firewall rules**. You can **add private endpoints** too. |
| **Private access (VNet integration)** | Server deployed into a **delegated subnet** of your VNet. No public endpoint. Needs a Private DNS zone. |

**Authentication:** PostgreSQL auth (passwords), **Microsoft Entra auth**, or both.

```sql
-- As the Entra admin, connected to the "postgres" database:
SELECT * FROM pgaadauth_create_principal('mi-ecom-api', false, false);
-- then in the ecom database:
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA sales TO "mi-ecom-api";
```

```python
# psycopg (Unit 4) with a token as the password (tokens expire, so fetch a fresh one per connection or pool)
import psycopg
from azure.identity import DefaultAzureCredential

token = DefaultAzureCredential().get_token("https://ossrdbms-aad.database.windows.net/.default").token
conn = psycopg.connect(host="psql-ecom-prod.postgres.database.azure.com", dbname="ecom",
                       user="mi-ecom-api", password=token, sslmode="require")
```

➕ **Added — CDC from Postgres.** Set `wal_level = logical` to enable **logical replication**, which tools like **Debezium** or Azure Data Factory/Fabric use to stream changes into the lake.

➕ **Added — related services:**

- **Azure Database for MySQL (Flexible Server):** the same managed model for MySQL.
- **Elastic clusters** on Flexible Server: horizontal sharding (Citus) for very large Postgres workloads. Replaces **Azure Cosmos DB for PostgreSQL** for new projects.

### 4. Choosing a relational option

| Situation | Choose |
|---|---|
| New app, team knows Postgres, wants open-source extensions (pgvector, PostGIS) | **PostgreSQL Flexible Server** |
| New app, Microsoft/.NET shop, wants serverless auto-pause or up to 128 TB (Hyperscale) | **Azure SQL Database** |
| Many small tenant DBs with uneven load | **Azure SQL elastic pool** |
| Migrating on-prem SQL Server that uses SQL Agent / cross-DB queries | **Azure SQL Managed Instance** |
| Need OS access or an unsupported feature | **SQL Server / Postgres on a VM** (IaaS) |

⚠️ **Remember Unit 5:** these are **OLTP** databases, the **sources** of your pipelines. Don't run heavy analytics on the production primary. Use a **read replica**, or extract into the lake/warehouse.

---

## Part D — Managed NoSQL: Azure Cosmos DB Concepts

### 1. What Cosmos DB is

A **fully managed, globally distributed NoSQL database** with:

- **Single-digit-millisecond** reads and writes at any scale
- **Turnkey global distribution:** add regions with a click, optionally **write in every region**
- **Elastic scale** of throughput and storage via **partitioning**
- **SLAs** on availability (**99.99%** single region, **99.999%** with multiple regions), latency, throughput and consistency
- **Schema-agnostic:** stores JSON items, and **indexes every property automatically** by default

**When to use it:** high-volume, low-latency, flexible-schema workloads with **known access patterns**: product catalogs, shopping carts, user profiles and sessions, IoT telemetry, event stores.
**When not to:** complex ad-hoc joins, multi-table transactions, BI/reporting (that's the warehouse, Unit 5).

### 2. APIs (chosen at account creation, can't be changed)

| API | Data model | Pick it when |
|---|---|---|
| **API for NoSQL** (native) | JSON documents, **SQL-like query language** | **New apps. Default choice.** Newest features appear here first. |
| **API for MongoDB** | BSON documents, MongoDB wire protocol | Migrating a MongoDB app. ➕ The vCore-based version has been rebranded **Azure DocumentDB**. |
| **API for Apache Cassandra** | Wide-column, CQL | Migrating Cassandra |
| **API for Apache Gremlin** | **Graph** (vertices, edges) | Relationship-heavy data: recommendations, fraud rings |
| **API for Table** | Key-value | Upgrading from Azure **Table Storage** for global distribution and better SLAs |

### 3. Resource model

```
Cosmos DB account   cosmos-ecom-prod            (regions, consistency default, API)
└── Database        ecom                         (optional shared throughput)
    └── Container   carts   (partition key /userId)   ← unit of scale and throughput
        └── Item    { "id": "cart-123", "userId": "u-42", "items": [...], "_ts": 1790000000 }
```

The terms change by API: **container** = collection (MongoDB) = table (Cassandra/Table) = graph (Gremlin). **Item** = document / row / vertex or edge.

**System properties** on each item: `id` (unique **within a logical partition**), `_ts` (last-modified Unix time), `_etag` (for **optimistic concurrency**), `_rid`, `_self`.

### 4. Partitioning (the most important design decision)

| Term | Meaning |
|---|---|
| **Partition key** | A property path chosen **when the container is created** (e.g. `/userId`). **Effectively immutable**: changing it means copying data into a new container. |
| **Logical partition** | All items with the **same partition key value**. **Max 20 GB.** |
| **Physical partition** | Internal machines that hold many logical partitions. Cosmos creates and splits them automatically. Each serves up to **10,000 RU/s** and **50 GB**. |

**A good partition key:**

- **High cardinality:** many distinct values (`userId`, `productId`, `deviceId`)
- **Spreads storage and requests evenly** so there are no **hot partitions**
- **Appears in the filter of most queries**, so a query hits **one** partition instead of fanning out as a **cross-partition query** (slower, costs more RUs)

| Container | Good key | Bad key | Why bad |
|---|---|---|---|
| carts | `/userId` | `/status` | Only a few values → hot partitions |
| telemetry | `/deviceId` (or hierarchical `/deviceId`, `/date`) | `/date` | All of today's writes hit one partition |
| orders | `/customerId` | `/country` | Skewed: one big country gets most of the traffic |

➕ **Added:**

- **Hierarchical partition keys:** up to **3 levels** (e.g. `/tenantId`, `/userId`, `/sessionId`) to get past the 20 GB per-value limit while still routing queries efficiently.
- **Synthetic keys:** concatenate properties (`"u-42_2026-09"`) to spread load.

### 5. Request Units (RUs) and throughput

**A Request Unit (RU)** is Cosmos DB's **currency for throughput**. It combines CPU, memory and IOPS into one number.

- **Baseline: a point read of a 1 KB item by `id` + partition key = 1 RU.**
- Writes cost more (a 1 KB insert is roughly 5+ RUs). Bigger items, more indexed properties, and complex or cross-partition queries cost more.
- Every response reports its charge in the header `x-ms-request-charge`. Measure it.

**Throughput options:**

| Mode | How you pay | Use |
|---|---|---|
| **Provisioned (manual)** | Fixed RU/s per hour (min **400 RU/s** per container) | Steady, predictable traffic |
| **Autoscale** | Set a **max** RU/s. Scales between **10% and 100%** of it. You pay for the peak each hour. | Variable traffic (e.g. flash sales) |
| **Serverless** | **Per RU consumed**. No provisioning. | Dev/test, spiky low traffic |

- Throughput can be set on a **container (dedicated)** or a **database (shared)** among its containers.
- **Exceed your RU/s → HTTP 429 "Request rate too large"** with a `retry-after` header. The SDKs retry automatically. Persistent 429s mean you should raise RU/s or fix a hot partition.
- ➕ **Free tier:** the first **1,000 RU/s and 25 GB** are free for one account per subscription.

### 6. Consistency levels

Distributed databases trade **consistency** against **latency, availability and throughput**. Cosmos DB offers **five levels**, from strongest to weakest:

| Level | Guarantee | Analogy |
|---|---|---|
| **Strong** | Reads always return the **latest committed write** (linearizable) | Everyone sees the same thing instantly |
| **Bounded staleness** | Reads lag writes by at most **K versions or T seconds** | "At most 5 minutes behind" |
| **Session** *(default)* | **Within a client session**: read your own writes, monotonic reads/writes. Others may see older data. | You always see your own cart changes |
| **Consistent prefix** | Never see writes **out of order**, but may be behind | You see A, then A+B. Never B without A. |
| **Eventual** | No ordering guarantee. **Replicas converge eventually.** | Like-counts on a post |

- **Stronger = higher latency and cost.** Strong and bounded-staleness reads cost about **2× the RUs** of weaker levels.
- **Strong isn't available with multi-region writes.**
- The default is set **on the account**. A client can **relax** it for a request, but not strengthen it.
- **Session** fits most apps, including carts and user profiles.

➕ **Added — CAP / PACELC.** **CAP:** during a network **P**artition, choose **C**onsistency or **A**vailability. **PACELC** adds: **E**lse (normal operation), choose **L**atency or **C**onsistency. Cosmos's five levels are points along that latency-vs-consistency trade-off.

### 7. Global distribution

- **Add or remove regions** at any time. Data replicates automatically.
- **Single-region writes** (one write region, many read regions) or **multi-region writes** (write anywhere, lowest write latency, 99.999% SLA).
- **Service-managed failover** promotes another region if the write region goes down. Set **failover priorities**.
- **Conflict resolution** for multi-region writes: **last writer wins** (on `_ts` by default, or a custom property) or a **custom** stored procedure.

### 8. Indexing, TTL and change feed

| Feature | Detail |
|---|---|
| **Automatic indexing** | Every property is indexed by default. The **indexing policy** lets you **exclude** paths (saves write RUs and storage), add **composite indexes** (for `ORDER BY a, b` or multi-filter queries), spatial and **vector** indexes. |
| **TTL (time to live)** | Items auto-delete after N seconds (container default or per item). Great for **carts and sessions**. |
| **Change feed** | A **persistent, ordered (per partition key) log of inserts and updates** in a container. Read it with **Azure Functions triggers** or the change feed processor to build event-driven pipelines, update materialized views, or stream to the lake. The default (latest-version) mode **doesn't capture deletes**, so use **soft deletes + TTL** or the **all versions and deletes** mode. |
| **Transactions** | ACID within **one logical partition** (stored procedures, **transactional batch**). **No multi-partition transactions.** |
| **Optimistic concurrency** | Send the `_etag` with `If-Match` on an update. It fails with 412 if someone else changed the item. |

### 9. Data modeling: think in access patterns, not normal forms

Unit 5 taught **normalization** for OLTP. Cosmos DB flips this: **model around how the app reads**, and **denormalize** to avoid joins (joins only work *within one item*).

| **Embed** (nest inside the item) when... | **Reference** (separate items, store IDs) when... |
|---|---|
| One-to-few relationships | One-to-many **unbounded** (e.g. all reviews of a product) |
| Data is read together | Data changes often and independently |
| Child data doesn't grow without limit | Would push the item past the **2 MB item size limit** |

```json
// carts container, partition key /userId, TTL = 7 days
{
  "id": "cart-u-42",
  "userId": "u-42",
  "items": [
    { "productId": "p-100", "name": "Running Shoes", "price": 89.0, "qty": 1 },
    { "productId": "p-205", "name": "Socks (3-pack)", "price": 12.5, "qty": 2 }
  ],
  "updatedAt": "2026-09-25T09:15:00Z"
}
```

`name` and `price` are **copied** from the product catalog (**denormalized**). If a price changes, the **change feed** on `products` can update open carts.

**Access control:** Cosmos DB uses **data-plane RBAC** (e.g. *Cosmos DB Built-in Data Contributor*) for Entra access, just like storage. Disable **key-based auth** where possible.

➕ **Added — analytics on Cosmos data.** Don't run big analytical scans against your transactional RUs. Options: **mirroring into Microsoft Fabric/OneLake** (a near-real-time replica for analytics), or the **change feed → lake** (Bronze) pipeline. The older **Synapse Link / analytical store** path isn't recommended for new projects.

---

## Part E — Putting It Together: Choosing the Right Store ➕ Added

**Polyglot persistence** means using **different stores for different jobs**. The e-commerce platform on Azure:

| Data | Access pattern | Store | Why |
|---|---|---|---|
| Orders and inventory; payments | ACID transactions, joins, constraints | **PostgreSQL Flexible Server** for orders; **Azure SQL** for the payments system (as in Unit 11) | Relational integrity (Unit 3) |
| Product catalog (varied attributes per category) | High-read, flexible schema, by `productId` | **Cosmos DB** (NoSQL API) | Schema flexibility, low latency |
| Shopping carts, sessions | Very high write rate, by `userId`, short-lived | **Cosmos DB** with TTL | Scale + automatic expiry |
| Raw extracts, clickstream, files | Cheap bulk storage, append-only | **ADLS Gen2 → bronze** | Cheap, any format, tiers |
| Cleaned and modeled data | Spark/dbt processing | **ADLS Gen2 → silver/gold** (Delta) | Open formats, atomic renames |
| Product images, invoice PDFs | Serve files via URL | **Blob Storage** (flat) + CDN | Simple object storage |

```
            FastAPI (App Service, managed identity)
             │                     │
   orders ───▼──────┐      carts ──▼────────────┐
   PostgreSQL Flex  │      Cosmos DB (NoSQL API) │
   (private access) │      (change feed)         │
             │ CDC  │             │              │
             ▼      │             ▼              │
   ADF ──► bronze/  ◄──── Function (change feed) │
             │                                   │
      Databricks (Spark, MI) ──► silver/ ──► gold/ ──► dbt / Power BI
             ADLS Gen2: ZRS, HNS, private endpoints (blob + dfs), RBAC + ACLs
```

**Quick decision guide:**

| If you need... | Use |
|---|---|
| Cheap storage for files of any format | Blob Storage |
| A data lake for Spark/Delta with folder security | **ADLS Gen2** |
| Transactions, joins, constraints, SQL | Azure SQL Database / PostgreSQL |
| Global low latency, flexible schema, massive scale, known key lookups | **Cosmos DB** |
| Analytics / BI over large history | Warehouse or lakehouse (Units 5, 7, 8), **not** the OLTP or NoSQL store |

---

## Key Terms Cheat Sheet

- **Storage account:** top-level container for Blob/Files/Queues/Tables. Sets redundancy, tier, networking.
- **Container / blob:** bucket / object. Flat namespace = "folders" are just name prefixes.
- **Block / append / page blob:** general files / append-only logs / VM disks
- **Hot / Cool / Cold / Archive:** minimum days 0 / 30 / 90 / 180. Archive is offline and needs **rehydration** (up to 15 h standard, <1 h high priority).
- **Archive not supported on ZRS/GZRS**
- ➕ **Lifecycle management:** rules to tier or delete by age
- **Auth options:** Entra + RBAC (best) → user delegation SAS → service/account SAS → shared key → anonymous (off)
- **SAS:** signed URL with permissions and expiry. Bearer credential.
- **Soft delete, versioning, immutable (WORM) storage:** undelete / undo overwrites / nobody can change it
- **ADLS Gen2:** Blob + **hierarchical namespace** (real directories, atomic rename, ACLs)
- **ABFS URI:** `abfss://container@account.dfs.core.windows.net/path`
- **RBAC then ACLs:** if RBAC grants access, ACLs aren't checked
- **ACL r/w/x:** read / write / traverse. Need **x on every parent** + r on the file.
- **Access ACL vs default ACL:** this item / template for **new** children only
- **Storage Blob Data Owner:** superuser in ADLS
- **Small-file problem:** too many tiny files. Compact to ~hundreds of MB–1 GB.
- **Two private endpoints for ADLS:** blob + dfs
- **T-SQL (Transact-SQL):** Microsoft's SQL dialect for the SQL Server engine. Adds variables, control flow, error handling, transactions, stored procedures.
- **Azure SQL Database / Managed Instance / SQL on VM:** single DB PaaS / full-instance PaaS / IaaS
- **Logical server:** holds logins and firewall rules. `*.database.windows.net:1433`
- **Elastic pool:** many DBs sharing resources
- **DTU vs vCore:** bundled units / independent CPU-memory-storage
- **General Purpose / Business Critical / Hyperscale:** remote storage / local SSD + replicas / up to 128 TB
- **Serverless (Azure SQL):** autoscale, per-second billing, auto-pause
- **PITR 1–35 days, LTR up to 10 years, active geo-replication, failover groups**
- **TDE (default on), Always Encrypted, dynamic data masking, RLS, Entra-only auth**
- **PostgreSQL Flexible Server:** Burstable / General Purpose / Memory Optimized. Zone-redundant HA, read replicas, stop/start, PgBouncer, extensions. Single Server retired.
- **Public access vs private access (VNet integration):** firewall rules / delegated subnet
- **Cosmos DB:** globally distributed NoSQL, single-digit ms, 99.999% multi-region SLA
- **APIs:** NoSQL (default), MongoDB, Cassandra, Gremlin, Table. Fixed at creation.
- **Account → database → container → item**
- **Partition key:** chosen at creation. High cardinality, even distribution, in most filters.
- **Logical partition ≤ 20 GB; physical partition ≤ 10,000 RU/s, 50 GB**
- ➕ **Hierarchical partition keys:** up to 3 levels
- **RU:** 1 KB point read = 1 RU. **429** = rate limited.
- **Provisioned (min 400) / autoscale (10–100% of max) / serverless**
- **Consistency:** Strong → Bounded staleness → **Session (default)** → Consistent prefix → Eventual
- **Change feed:** ordered log of inserts/updates (not deletes by default)
- **TTL:** auto-expire items
- **Embed vs reference:** read together and bounded / unbounded or independently changing. Items ≤ 2 MB.
- ➕ **Polyglot persistence:** the right store for each workload

---

## Practice Questions

1. **What's the difference between a storage account, a container and a blob?**

    The account is the top-level resource with the endpoint, redundancy and settings. A container groups blobs. A blob is the object (file) itself.

2. **In a flat-namespace Blob account, what is `orders/2026/09/`?**

    Just a name prefix. Folders don't really exist, so renaming one means copying and deleting every blob under it.

3. **Which blob type would you use for Parquet files? For an application log?**

    Block blob. Append blob.

4. **A Cool-tier blob is deleted after 10 days. What happens to cost?**

    An early deletion fee is charged for the remaining 20 days of the 30-day minimum.

5. **An analyst needs a file from Archive in 30 minutes. Is that possible?**

    Only with high-priority rehydration, which is usually under an hour for smaller objects. Standard priority can take up to 15 hours. Plan tiers so urgent data isn't archived.

6. **Your lake is ZRS. Can a lifecycle rule move Bronze to Archive?**

    No. Archive isn't supported on ZRS/GZRS/RA-GZRS. Use Cold, or keep an archive copy in an LRS/GRS account.

7. **Rank these by preference: account key, user delegation SAS, Entra ID + RBAC, service SAS.**

    Entra ID + RBAC, then user delegation SAS, then service SAS, then account key.

8. **Why is a user delegation SAS safer than a service SAS?**

    It's signed with Entra credentials instead of the account key, respects the signer's permissions, and can be revoked without rotating the account keys.

9. **Name three features that protect lake data from accidental deletes or overwrites.**

    Soft delete (blobs and containers), blob versioning, point-in-time restore, and immutable storage for compliance.

10. **What does ADLS Gen2 add to Blob Storage, and how do you get it?**

    A hierarchical namespace: real directories, atomic renames, and POSIX ACLs. You enable HNS on a StorageV2 account at creation, or upgrade an existing account (one-way).

11. **Why do Spark and Delta Lake benefit from HNS?**

    Their job commits rename output folders. With HNS a rename is one atomic metadata operation. On a flat namespace it's many copies and deletes: slow, and not atomic.

12. **Write the ABFS path for the `orders` folder in the `silver` container of `stecomlakeprod`.**

    `abfss://silver@stecomlakeprod.dfs.core.windows.net/orders/`

13. **A user has Storage Blob Data Reader on the `gold` container, and the ACL on `gold/finance/` denies them. Can they read `gold/finance/`?**

    Yes. RBAC is evaluated first, and because it grants access, ACLs aren't checked. To limit them to certain folders, remove the container-level role and use ACLs only.

14. **What ACL permissions are needed to read `gold/sales/2026/part-0001.parquet`?**

    Execute (x) on the container root, `sales` and `2026`, plus read (r) on the file.

15. **You set a default ACL on `gold/finance/` but existing files are still inaccessible to the new group. Why?**

    Default ACLs only apply to items created afterwards. Apply the ACL recursively to existing items.

16. **Why assign ACLs to groups rather than users?**

    Each item has at most 32 ACL entries. Group membership changes don't require touching millions of ACLs.

17. **What is the small-file problem, and how do you fix it?**

    Many tiny files slow listing and Spark tasks and increase transaction costs. Compact them into larger files (for example with Delta `OPTIMIZE`) and avoid over-partitioning.

18. **Spark reaches the lake through the blob private endpoint but fails on `abfss://`. Likely cause?**

    There's no private endpoint (and private DNS zone) for the `dfs` endpoint. ADLS needs both blob and dfs.

19. **Compare Azure SQL Database, SQL Managed Instance, and SQL Server on a VM.**

    Single-database PaaS for new apps / full-instance PaaS with near-100% compatibility for lift-and-shift / IaaS with full OS control.

20. **When would you use an elastic pool?**

    For many databases with varying, non-overlapping peaks (like one DB per SaaS tenant), so they share resources more cheaply.

21. **DTU vs vCore?**

    DTU bundles CPU, memory and I/O into one measure with simple tiers. vCore lets you choose compute and storage independently, and supports Hybrid Benefit and reservations.

22. **Which Azure SQL tier for a 40 TB database? For the lowest-latency OLTP?**

    Hyperscale (up to 128 TB). Business Critical (local SSD, replicas).

23. **What does serverless compute give you, and what's the downside?**

    Autoscaling, per-second billing, and auto-pause when idle. The downside is a cold-start delay on the first connection after a pause.

24. **Someone dropped a table at 14:05. How do you recover in Azure SQL Database?**

    Point-in-time restore to 14:04 as a new database (within the 1–35 day retention), then copy the table back.

25. **Active geo-replication vs failover groups?**

    Geo-replication creates readable secondaries per database. Failover groups fail over a set of databases together and provide stable listener endpoints so connection strings don't change.

26. **Which PostgreSQL Flexible Server tier can't use high availability?**

    Burstable.

27. **Public access vs private access networking in PostgreSQL Flexible Server?**

    Public access uses a public endpoint with firewall rules (private endpoints can be added). Private access deploys the server into a delegated VNet subnet with no public endpoint.

28. **How do you enable `pgvector` on Flexible Server?**

    Add `vector` to the `azure.extensions` server parameter allow-list, then run `CREATE EXTENSION vector;`.

29. **Why shouldn't the BI team query the production orders database directly?**

    It's an OLTP system, and heavy analytical queries would slow transactions. Use a read replica or extract the data into the lake/warehouse.

30. **What's a partition key, and what makes a good one?**

    The property Cosmos uses to distribute items. A good one has high cardinality, spreads storage and requests evenly, and appears in most query filters.

31. **Why is `/orderDate` a poor partition key for an orders container?**

    All of today's writes land in one logical partition, creating a hot partition that throttles.

32. **Logical vs physical partition limits?**

    Logical: all items with one key value, max 20 GB. Physical: managed by Cosmos, up to 10,000 RU/s and 50 GB each.

33. **What is a Request Unit? What's the cheapest operation?**

    A normalized measure of throughput cost. A point read of a 1 KB item by id + partition key costs 1 RU.

34. **Your app gets HTTP 429 from Cosmos DB. What does it mean, and what can you do?**

    The request rate exceeded the provisioned RU/s. The SDK retries after `retry-after`. Longer term: raise RU/s or use autoscale, fix hot partitions, trim the indexing policy, and use point reads instead of queries.

35. **Manual vs autoscale vs serverless throughput?**

    Fixed RU/s for steady load / scales from 10% to 100% of a max for variable load / pay per RU for dev or spiky low traffic.

36. **List the five consistency levels from strongest to weakest. Which is the default?**

    Strong, Bounded staleness, Session, Consistent prefix, Eventual. Session is the default.

37. **Which consistency level fits a shopping cart, and why?**

    Session. The user always sees their own changes, with lower latency and cost than Strong.

38. **Can you use Strong consistency with multi-region writes?**

    No.

39. **What is the change feed used for? What doesn't it capture by default?**

    Event-driven processing: syncing to the lake, updating materialized views, triggering Functions. In latest-version mode it doesn't capture deletes, so use soft deletes + TTL or the all-versions-and-deletes mode.

40. **Embed or reference: a product's 3 images? A product's reviews?**

    Embed the images (few, read together). Reference the reviews (unbounded, could exceed the 2 MB item limit).

41. **Can a Cosmos DB transaction span two partition key values?**

    No. Transactions (stored procedures, transactional batch) are limited to one logical partition.

42. ➕ **Choose a store for each: order payments, product catalog, clickstream files, invoice PDFs, cleaned sales tables.**

    PostgreSQL/Azure SQL · Cosmos DB · ADLS Gen2 bronze · Blob Storage · ADLS Gen2 silver/gold (Delta).

43. ➕ **Design secure access for the new stores in the e-commerce platform.**

    Give each workload a managed identity. FastAPI's identity gets an Entra database user in PostgreSQL (read/write on the `sales` schema) and *Cosmos DB Built-in Data Contributor* on the `carts` and `products` containers. ADF gets Blob Data Contributor on `bronze`. The finance group gets ACL read on `gold/finance/` only. Disable password/key auth where possible, use private endpoints (blob + dfs for the lake) and private access for Postgres, and keep any remaining secrets in Key Vault.
