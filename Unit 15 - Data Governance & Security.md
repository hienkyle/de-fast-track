# Unit 15 — Data Governance & Security: Study Guide

**Scope:** Handling sensitive and PII data (masking, anonymization) · Azure Purview (data cataloging, lineage, and governance) · Data security best practices (encryption, access control)

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. Units 11–14 built pipelines that are reliable and monitored. This unit covers making sure the **right people** see the **right data**, that sensitive data is **protected**, and that everyone can find out **what data exists and where it came from**.

---

## Part A — Handling Sensitive and PII Data

### 1. What counts as sensitive

| Category | Meaning | E-commerce examples |
|---|---|---|
| **Direct identifiers (PII)** | Identify a person on their own | Full name, email, phone, national ID, card number, precise address |
| **Quasi-identifiers** | Harmless alone, identifying **in combination** | Birth date, ZIP code, gender, city, job title |
| **Sensitive / special category** | Extra harm if leaked | Health data, religion, biometrics, precise location |
| **Payment data (PCI)** | Card numbers, CVV | Covered by PCI DSS. Ideally never stored: use the payment provider's tokens. |
| **Confidential business data** | Not personal, still restricted | Margins, supplier prices, unreleased sales figures |

⚠️ **Quasi-identifiers matter.** A classic study found that **ZIP + birth date + gender** alone uniquely identifies about **87% of Americans**. Removing names and emails does not make data anonymous.

### 2. ➕ Regulations you'll hear about

| Regulation | Where | Key points for data engineers |
|---|---|---|
| **GDPR** | EU (applies to EU residents' data anywhere) | Lawful basis, data minimization, **right to erasure**, breach notification within 72 h. **Pseudonymized data is still personal data. Truly anonymized data is not.** |
| **CCPA / CPRA** | California | Right to know, delete, and opt out of the sale of data |
| **HIPAA** | US health data | Protected health information (PHI), Safe Harbor de-identification |
| **PCI DSS** | Card payments worldwide | Protect cardholder data. Tokenize, and keep card data out of the lake. |
| **Vietnam Personal Data Protection Law** | Vietnam (effective 2026, building on Decree 13/2023) | Consent, data subject rights, rules for sensitive data and cross-border transfer |

### 3. Protection techniques

| Technique | What it does | Example | Reversible? | Still personal data? |
|---|---|---|---|---|
| **Static masking** | Permanently replaces values in a **copy** | Dev/test copy with fake emails | No | Depends on what's left |
| **Dynamic masking** | Hides values **at query time** based on who asks. The stored data is unchanged. | Support agents see `a***@gmail.com` | Yes, for authorized users | Yes |
| **Redaction / suppression** | Removes the value or column entirely | Drop `phone` from silver | No | Less so |
| **Tokenization** | Replaces a value with a random token. The mapping lives in a secure **token vault**. | `4111…1111` → `tok_8f3a` | Yes, via the vault | Yes |
| **Pseudonymization (keyed hashing)** | Deterministic hash with a **secret key** | `customer_hash = SHA-256(email + secret)` | Not directly, but linkable | **Yes** (GDPR) |
| **Encryption** | Reversible with the key | Column encryption, Always Encrypted | Yes, with the key | Yes |
| **Generalization** | Makes values less precise | Age 34 → `30–39`; ZIP `70000` → `700**` | No | Reduces risk |
| **Perturbation / noise** | Adds random noise to values | Salary ± 5% | No | Reduces risk |
| **Anonymization** | Combines the above so **no one can be re-identified** by reasonable means | Aggregated, generalized, k-anonymous dataset | No | **No** |
| ➕ **k-anonymity** | Every combination of quasi-identifiers appears in **at least k rows** | k = 5: each (age band, ZIP3, gender) group has ≥ 5 people | — | Measures anonymity |
| ➕ **Differential privacy** | Adds calibrated noise to **query results**, with a mathematical privacy guarantee | Published counts per city | — | Strong protection |
| ➕ **Synthetic data** | Generates fake records with the same statistics | Realistic test data | — | Usually no |

**Key distinctions:**

- **Masking vs anonymization:** masking *hides* values from some viewers, but the real data is still there. Anonymization *removes* the ability to identify anyone, for everyone, permanently.
- **Pseudonymization ≠ anonymization.** A consistent hash still lets you join and track one person across tables. That's why it's useful, and also why it's still personal data.
- ⚠️ **An unsalted hash of an email or phone number is not protection.** An attacker hashes a list of known emails and matches them (a dictionary attack). Always use a **secret key** (ideally HMAC), stored in Key Vault.

### 4. Where it happens in the medallion architecture

```
 Sources ──► BRONZE (raw, contains PII)       ──► SILVER (pseudonymized)               ──► GOLD (aggregated / de-identified)
             • Access: pipeline identities       • customer_hash = keyed hash             • Mostly no direct identifiers
               + few data engineers only          • PII columns dropped, or masked        • Broad analyst access
             • Classified by Purview                by policy (UC column masks)
             • Short retention                    • Token vault / lookup table in a
                                                    separate restricted schema
```

- **Classify early.** Know which columns are PII as soon as data lands (Purview scans, Part B).
- **Minimize.** Don't ingest columns nobody needs. Data you don't have can't leak.
- **Keep the re-identification key separate.** The `customer_hash → email` lookup lives in its own restricted schema.
- ⚠️ **Don't mix up the three kinds of key or hash:**
  - `customer_hash`: a keyed hash **for privacy**. Deterministic, so it can be joined across tables.
  - `customer_key`: Unit 5's **integer surrogate key**. It identifies a dimension *version*, not a person.
  - `row_hash`: Unit 5's **change-detection hash**. Unkeyed, so it gives no privacy protection.
- **Silver is where access control matters most** (see Unit 13, Part D §4). That's where cleaned data becomes widely used.
- ➕ **Right to erasure vs "immutable bronze":** Units 11 and 13 say to keep bronze raw and immutable, but a legal deletion request **overrides** that. Plan for it:
  - Keep PII-bearing bronze on **short retention**.
  - Never put personal data in **WORM** containers (Unit 10). They can't be deleted, not even to comply with erasure.
  - Or use **crypto-shredding**: delete the person's key, and every copy becomes unreadable, including bronze and backups.
  - On Delta, `DELETE` + `VACUUM`. Until you vacuum, time travel still holds the old files.
- ⚠️ **Don't leak PII into logs.** Structured logs (Unit 13, Part E) should carry `customer_hash`, never an email. The same goes for error messages and quarantine tables.
- **Non-production:** dev and test use **statically masked or synthetic** data, never a raw prod copy.

### 5. Implementation on Azure

**Pseudonymizing in PySpark (bronze → silver).** Replace the email with a **keyed hash**, where the key is a secret from Key Vault. Mask, generalize (birth date → age band, ZIP → first 3 digits) or drop the other PII columns.

```python
key = dbutils.secrets.get("kv-scope", "pii-hash-key")
df = df.withColumn("customer_hash", F.sha2(F.concat(F.lower("email"), F.lit(key)), 256)).drop("email")
```

Spark has no built-in HMAC function, so this appends the secret key before hashing (the `SHA-256(email + secret)` form from the table). Where a true HMAC is required, compute it in a pandas UDF with Python's `hmac` module.

**Dynamic Data Masking (Azure SQL / Synapse dedicated pool)**

- A masking rule is set **per column**. Functions:
  - `default()`: full mask
  - `email()`: shows `aXXX@XXXX.com`
  - `partial(prefix, padding, suffix)`: e.g. show only the last 4 digits
  - `random(low, high)`: for numbers
- Only users with **`UNMASK`** see real values.

```sql
ALTER TABLE dbo.customers ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
GRANT UNMASK TO support_leads;
```

⚠️ **DDM is not a security boundary.** Users can still filter on masked columns (`WHERE salary > 100000`) and infer the values. Combine it with column permissions or de-identification for really sensitive data.

**Unity Catalog column masks and row filters (Databricks)**

- **Column mask:** a SQL function attached to a column. It returns the real value or a masked one depending on who is querying (`is_account_group_member()`).
- **Row filter:** a function that returns TRUE/FALSE per row, e.g. regional analysts see only their region.
- The policy lives **on the table**, so every query path that goes through Unity Catalog (SQL warehouse, notebook, a BI tool connected to a SQL warehouse) gets the same result. Tools that read the lake files directly bypass it (Part C §4).
- ➕ **Attribute-based access control (ABAC)** with governed tags lets one policy cover every column tagged `pii=email`.

```sql
CREATE FUNCTION ecom_prod.gold.mask_name(n STRING) RETURNS STRING
  RETURN IF(is_account_group_member('pii_readers'), n, '***');
ALTER TABLE ecom_prod.gold.dim_customer ALTER COLUMN full_name SET MASK ecom_prod.gold.mask_name;
```

**Row-level security (Azure SQL / Synapse):** an **inline table-valued predicate function** decides whether a row is visible (e.g. `region` matches the user's session context). A **security policy** attaches it to the table as a **filter predicate** (hides rows on read) or a **block predicate** (prevents writing rows the user couldn't see).

**Column-level security:** `GRANT SELECT` on specific columns only.

---

## Part B — Microsoft Purview (formerly Azure Purview)

### 1. Naming and current state

- **Azure Purview** was renamed **Microsoft Purview** in 2022 and merged with Microsoft's compliance tools (Information Protection, DLP, and others) under one brand.
- The data governance side now runs in the **new Microsoft Purview portal**, built around the **Data Map** and the **Unified Catalog**.
- The **classic** Data Catalog, Data Health Insights and Workflow (the old "Azure Purview" experience) **no longer accept new customers** and are in support-only mode. Courses and older docs still show the classic screens and terms (collections, glossary, insights), so learn both sets of names.
- In the **new experience**, Purview is a single **tenant-level** account for the whole organization. The classic experience allowed several accounts per tenant.

### 2. Architecture

```
 Sources                       Microsoft Purview Data Map                   Microsoft Purview Unified Catalog
 ───────                       ──────────────────────────                   ─────────────────────────────────
 ADLS Gen2, Blob          ┐    Register source → SCAN (integration runtime)   Governance domains
 Azure SQL, Synapse       │    ─ technical metadata (schemas, columns)        Data products (curated, shareable)
 Databricks Unity Catalog ├──► ─ CLASSIFICATIONS (email, card no., …)   ───►  Glossary terms, critical data elements, OKRs
 Power BI, Fabric         │    ─ sensitivity labels                           Data quality rules and scores
 ADF / Synapse pipelines ─┤    ─ LINEAGE graph                                Health management, access requests
 On-prem SQL, S3, Snowflake┘   (metadata only: the data never leaves the source)   Search and discovery for consumers
```

⚠️ **Purview stores metadata only.** It reads schemas and samples values to classify them, but it doesn't copy your data. Access in Purview **doesn't grant access to the data itself**.

### 3. Data Map: registering and scanning

| Step | Details |
|---|---|
| **Register** a source | ADLS, Azure SQL, Synapse, Databricks, Power BI, on-prem SQL Server, S3, Snowflake, and many more |
| **Authentication** | Prefer Purview's **managed identity** (e.g. give it **Storage Blob Data Reader** on the lake, and `db_datareader` on SQL). Otherwise use credentials stored in **Key Vault**. |
| **Integration runtime** | **Azure IR** for public cloud sources. **Self-hosted IR** for on-prem or private networks. **Managed VNet IR** for private endpoints. (Same idea as ADF, Unit 11.) |
| **Scan rule set** | Which file types to read and which classification rules to apply |
| **Schedule** | Full or incremental scans, e.g. weekly |
| **Result** | Assets with schema, **classifications**, owners, and lineage, organized into **collections** (classic) / **domains** (new) that also control who can see what |

**Data Map roles** (on a collection or domain): Collection admin, Data source admin, Data curator (edit metadata), Data reader (browse).

### 4. Classification and labeling

- **System classifications:** 200+ built-in patterns, such as email address, phone number, credit card number, IBAN, and national ID numbers for many countries. Applied automatically during scans by **sampling** values and matching regex or dictionary patterns.
- **Custom classifications:** your own regex or dictionary rules, e.g. an internal `ECOM-CUST-\d{8}` customer ID.
- ➕ **Sensitivity labels** (Microsoft Purview Information Protection): *Public / General / Confidential / Highly Confidential*, applied to assets based on their classifications. The same labels are used in Office and Power BI.
- **Why it matters:** you can't protect PII you don't know about. The scan results drive the masking and access rules from Part A.

### 5. Lineage

**Lineage** shows how data moves: source → transformations → targets, at dataset level and sometimes at column level.

| How lineage gets into Purview | Notes |
|---|---|
| **ADF / Synapse pipelines** | Connect the data factory to Purview. **Copy**, **Data Flow**, and **Execute SSIS package** activities report lineage automatically when they run. |
| **Databricks Unity Catalog** | Scanning Unity Catalog brings in its tables and the lineage Unity Catalog captured |
| **Power BI / Fabric** | Datasets → reports → dashboards |
| ➕ **Custom / manual lineage** | Through the **Apache Atlas API** (e.g. the `pyapacheatlas` library) for tools Purview can't see, such as a custom Python job |

**What lineage is used for:**

- **Impact analysis:** "If I rename `silver.orders.amount`, which gold tables and Power BI reports break?"
- **Root cause:** "The revenue dashboard is wrong. Trace it upstream to the failing source." This links to incident triage in Unit 13.
- **Auditability:** "Where did this data come from, when, and through which pipeline?" Purview lineage answers this across the whole estate. Audit columns and control tables (Unit 13, Part D §2) answer it row by row.
- **Compliance:** showing regulators where PII flows.

### 6. Unified Catalog: governance

| Concept | Meaning |
|---|---|
| **Governance domain** | A business area (Sales, Customer, Finance) that owns its data products and terms |
| **Data product** | A curated, documented group of assets built for a use case, e.g. "Customer 360", which consumers can find and **request access** to |
| **Glossary term** | A shared business definition, e.g. **"Active customer" = at least one order in the last 90 days**. This fixes the metric drift problem from Unit 13 Part D §5. |
| **Critical data element (CDE)** | A field that matters most for the business (e.g. `customer_id`, `order_amount`), with extra quality rules |
| **OKRs** | Business objectives linked to the data products that support them |
| **Data quality** | Rules (completeness, uniqueness, validity, and so on) and scores on assets. These are the same quality dimensions as Unit 13 Part D §3. |
| **Health management** | Tracks governance health: missing owners, unclassified assets, low quality scores |

**Roles in a federated governance model:**

| Role | Responsibility |
|---|---|
| **Central data office** | Sets policies and standards, oversees compliance |
| **Data owner** | Accountable for a dataset's meaning, quality, and who gets access (the same role as in Unit 13, Part E) |
| **Data steward** | Maintains metadata, definitions, and quality day to day |
| **Data consumer** | Finds data in the catalog and requests access |

### 7. ➕ Purview vs Unity Catalog

| | **Microsoft Purview** | **Databricks Unity Catalog** |
|---|---|---|
| Scope | Whole data estate (Azure, on-prem, other clouds, SaaS, Power BI) | Databricks workspaces (tables, volumes, models) |
| Main job | Discover, classify, lineage, business glossary, governance workflows | **Enforce** access (GRANTs, masks, row filters), lineage inside Databricks, auditing |
| Enforces data access? | Mostly **no**: it's a catalog and governance layer | **Yes** |

**Use both:** Unity Catalog enforces access inside Databricks, and Purview scans Unity Catalog so Databricks assets appear alongside everything else in one estate-wide catalog.

---

## Part C — Data Security Best Practices

### 1. Defense in depth

| Layer | Controls |
|---|---|
| **Identity** | Microsoft Entra ID, MFA, ➕ Conditional Access, managed identities, ➕ PIM (just-in-time admin) |
| **Network** | Private endpoints, disabled public access, managed VNets, firewalls, NSGs |
| **Data** | Encryption at rest, in transit, and in use; masking; row and column security |
| **Access** | Least-privilege RBAC, ACLs, Unity Catalog grants, SQL permissions |
| **Monitoring** | Audit logs, ➕ Defender for Cloud, ➕ Sentinel (links to Unit 13) |

Each layer assumes the others might fail.

### 2. Encryption

**At rest:**

| Service | Default | Stronger options |
|---|---|---|
| **Storage / ADLS** | **Always on**, AES-256, Microsoft-managed keys | **Customer-managed keys (CMK)** in Key Vault or Managed HSM. ➕ Infrastructure (double) encryption. |
| **Azure SQL / Synapse** | **TDE (Transparent Data Encryption) on by default** for new databases | TDE with CMK (**BYOK**) |
| **Databricks** | Managed disks and control-plane data encrypted | CMK for managed services, managed disks, and workspace storage |
| Delta / Parquet on ADLS | Inherits storage encryption | — |

- **Microsoft-managed vs customer-managed keys:** the data is encrypted either way. CMK gives **you** control over rotation, auditing of key use, and the ability to **revoke** the key, which makes the data unreadable.
- ➕ **Envelope encryption:** data is encrypted with a **data encryption key (DEK)**. The DEK is itself encrypted ("wrapped") by a **key encryption key (KEK)** in Key Vault. Rotating the KEK doesn't require re-encrypting all the data.

**In transit:** TLS everywhere. On storage accounts, turn on **secure transfer required** and set a **minimum TLS version of 1.2**. Database connections should use `Encrypt=True`.

**In use:**

- **Always Encrypted** (Azure SQL): columns are encrypted **in the client driver**, so even DBAs and the database engine never see plaintext. **Secure enclaves** allow some operations (comparisons, pattern matching) on encrypted data.
- ➕ **Confidential computing** (confidential VMs) protects data in memory with hardware.

**Application-level / column encryption:** encrypt specific fields in the pipeline before writing them. This is used for crypto-shredding (Part A §4).

### 3. Secrets and key management

- **Azure Key Vault** stores **secrets** (passwords, connection strings), **keys** (CMK, signing), and **certificates**.
  - Use the **Azure RBAC permission model** (e.g. *Key Vault Secrets User* for a pipeline identity).
  - Turn on **soft delete + purge protection**. Purge protection is **required** when a vault holds CMKs, because losing the key means losing the data.
  - Set **rotation** policies and expiry notifications.
- **Managed identities** remove credentials altogether. **System-assigned** identities are tied to one resource's lifecycle. **User-assigned** identities are standalone and can be shared across resources. They also prevent the **expired-credential** ingestion failure (Unit 13, Part D §1): there's no secret left to expire or leak.
- **Databricks:** use **Key Vault–backed secret scopes**. `dbutils.secrets.get()` values are redacted in notebook output.
- **Never** put secrets in code, notebooks, ADF JSON, or Git. ➕ Turn on **secret scanning** in the repo, so a committed secret is caught right away (Unit 1 explains why it must then be rotated).

### 4. Access control

**Principles:**

- **Least privilege:** give only the access the job needs.
- **Assign to groups, not individuals:** easier to review and to revoke when people leave.
- **Separation of duties:** the person who writes a pipeline doesn't approve its production access.
- ➕ **Just-in-time** admin with PIM, plus regular **access reviews**.

**Azure RBAC: control plane vs data plane**

| Role type | Examples | Lets you |
|---|---|---|
| **Control plane** | Owner, Contributor, Reader | Manage the **resource** (create, configure, delete) |
| **Data plane** | **Storage Blob Data Reader / Contributor / Owner**, Key Vault Secrets User | Read or write the **data** inside it |

⚠️ *Reader* on a storage account **doesn't** let you read blobs. Use Storage Blob Data roles for data. Also **disable shared key access** where possible, because an account key bypasses both RBAC and ACLs. Prefer **user delegation SAS** (signed with Entra credentials) over account-key SAS.

**ADLS Gen2 ACLs (POSIX-style)**

- Permissions: **r** (read), **w** (write), **x** (execute = traverse a folder), set per user or group on folders and files.
- You need **x on every parent folder** to reach a file.
- **Access ACLs** control the object itself. **Default ACLs** (folders only) are **copied to new children** created afterwards. ⚠️ Changing a default ACL **doesn't update existing children**, so apply ACLs recursively for those.
- **Evaluation order:** Azure RBAC is checked first. If a data role grants access, **ACLs aren't checked**. Use RBAC for broad access (whole container) and ACLs for fine-grained access (specific folders).

```
stecomlakeprod
 ├─ bronze   (container)  RBAC: pipeline MIs → Storage Blob Data Contributor; ACLs: grp-de-engineers rwx
 ├─ silver   (container)  RBAC: Databricks Access Connector (Unit 11); ACLs: grp-de-engineers rwx
 └─ gold     (container)  RBAC: Access Connector; ACLs: grp-bi-direct r-x on gold/sales/ only (no masked columns)
```

- **Containers are the main security boundary** (Unit 10). RBAC applies per container, and ACLs cover sub-folders.
- Analysts working in **Databricks** get access through **Unity Catalog grants**, not ACLs. Databricks reads the lake as its Access Connector's managed identity. Lake ACLs are only for tools that read files directly (Synapse serverless, Power BI). ⚠️ Those tools bypass Unity Catalog, so column masks and row filters don't apply to them. Give direct ACLs only on folders with no masked data (e.g. aggregated `gold/sales/`), and have Power BI read masked tables such as `dim_customer` through a Databricks SQL warehouse.

**Databricks Unity Catalog**

```sql
GRANT USE CATALOG ON CATALOG ecom_prod TO `grp-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ecom_prod.gold TO `grp-analysts`;
```

- Hierarchy: **metastore → catalog → schema → table/view/volume**. You need `USE CATALOG` + `USE SCHEMA` + the object privilege.
- Granting `SELECT` on a schema covers its current **and future** tables.
- Row filters and column masks are covered in Part A §5. Unity Catalog also records **audit logs** and lineage automatically.

**SQL (Azure SQL / Synapse):** database roles and `GRANT`, **column-level security** (`GRANT SELECT` on specific columns), row-level security (Part A §5), DDM, and Entra authentication instead of SQL logins.

### 5. Network security

These were covered in Units 9–11; here's the summary:

- **Private endpoints** for storage (**both blob and dfs**), SQL, Key Vault, and Purview. Then **disable public network access**.
- **ADF / Synapse managed VNet** with managed private endpoints.
- **Databricks VNet injection** + **secure cluster connectivity** (no public IPs on cluster nodes).
- Storage firewalls, NSGs, and ➕ service tags.

### 6. Auditing and threat detection

| Source | What it tells you |
|---|---|
| **Storage logs** (`StorageBlobLogs`) | Who read or wrote which file (needs a diagnostic setting, Unit 13 Part A) |
| **Azure SQL auditing** | Queries and logins, sent to Log Analytics or storage |
| **Databricks audit logs** / ➕ `system.access.audit` | Workspace, cluster, and Unity Catalog access events |
| **Entra sign-in and audit logs** | Logins, role assignments |
| **Key Vault logs** | Every secret and key access |
| ➕ **Microsoft Defender for Cloud** (Defender for Storage, for SQL) | Alerts on unusual access, malware uploads, SQL injection |
| ➕ **Microsoft Sentinel** | SIEM: correlates all of these and raises security incidents |

### 7. Best-practices checklist

- [ ] **Classify** data (Purview scans) and tag PII columns
- [ ] **Minimize** data: don't ingest what you don't need, and set retention limits
- [ ] **Pseudonymize** PII in silver with a keyed hash. Keep the lookup separate and restricted.
- [ ] Use **masking / row filters** for shared tables. Don't rely on DDM alone for highly sensitive data.
- [ ] **Encryption at rest** (CMK where required), **TLS 1.2+**, Always Encrypted for the most sensitive columns
- [ ] **Managed identities + Key Vault**, with no secrets in code. Purge protection on vaults.
- [ ] **Least privilege**, group-based: data-plane roles + ACLs + Unity Catalog grants. Disable shared keys.
- [ ] **Private endpoints**, public access disabled
- [ ] **Audit logs** on, sent to Log Analytics, with alerts on unusual access
- [ ] Dev/test uses **masked or synthetic** data
- [ ] Every dataset has an **owner** and a **steward** in the catalog
- [ ] Deploy policies and permissions as **code** (Bicep/Terraform, Unit 9). Review access regularly.

---

## Part D — ➕ Added: Putting It Together (E-commerce Platform)

| Layer | Contains | Who can access | Protection |
|---|---|---|---|
| **Sources** | CRM, orders DB, payment provider | Source teams | Card numbers stay with the payment provider (tokens only) |
| **Bronze** | Raw data with PII | Pipeline managed identities + `grp-de-engineers` | Private endpoints, CMK, short retention, Purview scan + classification |
| **Silver** | Cleaned; `customer_hash` instead of email and phone; `full_name` kept for `dim_customer` | Engineers, some analysts (Unity Catalog grants) | Column mask on `full_name`, row filter by region |
| **PII vault** | `customer_hash → email, phone` | `grp-pii-readers` only | Separate schema, audited access |
| **Gold** | Star schema (Unit 5). `dim_customer.full_name` is the only direct identifier. | Analysts, Power BI service | Column mask on `full_name`, RLS for regional managers |
| **Catalog** | Purview: scans ADLS, Unity Catalog, SQL, Power BI; ADF lineage connected | Everyone can **search**; access through requests | Glossary terms, owners, data quality scores |

---

## Key Terms Cheat Sheet

- **PII:** direct identifiers vs **quasi-identifiers** (ZIP + birth date + gender)
- **Static vs dynamic masking; redaction; tokenization (vault); pseudonymization (keyed hash); generalization; perturbation**
- **Anonymized** = can't be re-identified, so not personal data. **Pseudonymized** = still personal data (GDPR).
- ➕ **k-anonymity, differential privacy, synthetic data, crypto-shredding**
- **`customer_hash`** (keyed hash, privacy) vs **`customer_key`** (surrogate key, Unit 5) vs **`row_hash`** (change detection, no privacy)
- **DDM** functions: `default()`, `email()`, `partial()`, `random()`; `GRANT UNMASK`. Not a security boundary.
- **Unity Catalog column masks + row filters;** `is_account_group_member()`; ➕ ABAC with governed tags
- **SQL RLS:** predicate function + security policy (filter / block predicates). **CLS:** column-level GRANT.
- **Microsoft Purview** (formerly Azure Purview): **Data Map** (register, scan, classify, lineage) + **Unified Catalog** (domains, data products, glossary, CDEs, OKRs, data quality, health). Metadata only. Classic experience is support-only.
- **Scanning:** managed identity + Blob Data Reader; Azure / self-hosted / managed VNet IR; scan rule sets; collections / domains
- **Classifications:** 200+ system + custom (regex/dictionary); ➕ sensitivity labels
- **Lineage:** ADF/Synapse (Copy, Data Flow, SSIS), Unity Catalog, Power BI, ➕ Atlas API. Used for impact analysis, root cause, audit.
- **Roles:** data owner, data steward, data consumer, central data office
- **Encryption:** at rest (storage SSE, **TDE** by default, CMK/BYOK), in transit (TLS 1.2+), in use (**Always Encrypted**, enclaves); ➕ envelope encryption DEK/KEK
- **Key Vault:** secrets, keys, certificates; RBAC; soft delete + **purge protection**; rotation. **Managed identities** (system vs user-assigned).
- **RBAC control plane vs data plane;** disable shared keys; user delegation SAS
- **ADLS ACLs:** r/w/x, execute on parents, access vs default ACLs, RBAC evaluated first. Containers are the main boundary.
- **Unity Catalog hierarchy:** metastore → catalog → schema → table; USE CATALOG + USE SCHEMA + SELECT
- **Network:** private endpoints (blob + dfs), managed VNet, VNet injection, secure cluster connectivity
- **Audit:** StorageBlobLogs, SQL auditing, Databricks audit logs, Key Vault logs, ➕ Defender for Cloud, Sentinel

---

## Practice Questions

1. **What's the difference between a direct identifier and a quasi-identifier? Give two of each.**
   A direct identifier identifies a person on its own (email, national ID). A quasi-identifier only identifies them in combination with others (birth date, ZIP code).

2. **You removed names and emails from a dataset. Is it anonymous?**
   Not necessarily. Quasi-identifiers like ZIP + birth date + gender can still re-identify most people. Generalize them or check k-anonymity.

3. **Masking vs anonymization?**
   Masking hides values from some users, but the real data still exists. Anonymization permanently removes the ability to identify anyone.

4. **Why is pseudonymized data still personal data under GDPR?**
   It can be linked back to the person with extra information (the key or lookup table), and it still tracks one individual across records.

5. **Why is `sha2(email, 256)` without a secret a weak pseudonymization?**
   Anyone can hash a list of known emails and match them (a dictionary attack). Use a keyed hash (HMAC) with a secret from Key Vault.

6. **Static vs dynamic masking: when do you use each?**
   Static for dev/test copies, where data is permanently replaced. Dynamic for production tables where some users need real values and others don't.

7. **Tokenization vs encryption?**
   Tokenization replaces the value with a random token and stores the mapping in a vault, with no mathematical link. Encryption transforms the value and can be reversed by anyone with the key.

8. **Name four Dynamic Data Masking functions and what each shows.**
   `default()` masks the value fully, `email()` shows the first letter plus `XXX@XXXX.com`, `partial()` shows a chosen prefix and suffix with custom padding in between, and `random()` returns a random number in a range.

9. **Why isn't Dynamic Data Masking a security boundary?**
   Users can still filter on masked columns (`WHERE salary > 100000`) and infer the hidden values.

10. **How do you make only the `pii_readers` group see real customer names in `gold.dim_customer`?**
    Create a SQL function that returns the name when `is_account_group_member('pii_readers')` and a masked value otherwise, then `ALTER TABLE ... ALTER COLUMN full_name SET MASK`.

11. **Where in the medallion architecture should PII be pseudonymized, and why?**
    Bronze → silver. Bronze keeps raw data under tight access for replay. Silver is widely used, so it should carry keys instead of identifiers.

12. **A customer asks to be deleted. What must you do on a Delta table?**
    `DELETE` their rows, then `VACUUM`. Erasure also applies to "immutable" bronze and backups, so plan for it with short retention, no WORM storage for PII, or crypto-shredding.

13. **What is Microsoft Purview, and what was it called before?**
    Microsoft's unified data governance service (formerly Azure Purview). It catalogs, classifies, and traces lineage across the data estate.

14. **Name the two main parts of Purview's data governance and what each does.**
    Data Map: registers and scans sources, capturing metadata, classifications, and lineage. Unified Catalog: governance domains, data products, glossary, data quality, and discovery for consumers.

15. **Does Purview copy your data?**
    No. It stores metadata only, sampling values to classify them. Access in Purview doesn't grant access to the data.

16. **Purview's scan of ADLS fails with a permission error. What's missing?**
    Purview's managed identity needs **Storage Blob Data Reader** on the storage account or container (and network access if it's behind private endpoints).

17. **Which integration runtime do you use to scan an on-prem SQL Server?**
    A self-hosted integration runtime.

18. **System vs custom classifications?**
    System: 200+ built-in patterns (email, credit card, national IDs). Custom: your own regex or dictionary rules for organization-specific data.

19. **How does ADF lineage get into Purview?**
    Connect the data factory to Purview. Copy, Data Flow, and Execute SSIS activities then report lineage automatically when they run.

20. **Give three uses of lineage.**
    Impact analysis before changes, root-cause analysis when data is wrong, and audit/compliance (showing where PII flows).

21. **What's a data product and a glossary term in the Unified Catalog?**
    A data product is a curated group of assets for a use case that consumers can request access to. A glossary term is a shared business definition (e.g. "active customer").

22. **Data owner vs data steward?**
    The owner is accountable for the data and approves access. The steward maintains its metadata, definitions, and quality day to day.

23. **Purview vs Unity Catalog?**
    Purview catalogs and governs metadata across the whole estate but mostly doesn't enforce access. Unity Catalog enforces access inside Databricks. Use both, with Purview scanning Unity Catalog.

24. **Is data in ADLS encrypted if you do nothing?**
    Yes. Storage encryption (AES-256) is always on, with Microsoft-managed keys by default.

25. **Why choose customer-managed keys?**
    Control over rotation, auditing key use, and the ability to revoke access to the data, often required by compliance.

26. **What is TDE, and what does Always Encrypted add?**
    TDE encrypts the database files at rest and is on by default. Always Encrypted encrypts columns in the client, so the database engine and DBAs never see plaintext.

27. **Why is purge protection required for a Key Vault holding CMKs?**
    If the key were permanently deleted, all data encrypted with it would be unrecoverable.

28. **How do managed identities improve security and reliability?**
    There are no secrets to store, leak, rotate, or expire. That removes the "expired credential" pipeline failure from Unit 13.

29. **A user has Reader on a storage account but can't read files. Why?**
    Reader is a control-plane role. Reading data needs a data-plane role (Storage Blob Data Reader) or ACLs.

30. **A user has read ACL on a file but still gets "access denied". Why?**
    They need execute (x) permission on every parent folder to traverse the path.

31. **You set a default ACL on `silver/`, but analysts still can't read existing files. Why?**
    Default ACLs apply only to children created afterwards. Apply the ACL recursively to existing items.

32. **Which Unity Catalog privileges does an analyst need to query `ecom_prod.gold.fact_sales`?**
    `USE CATALOG` on `ecom_prod`, `USE SCHEMA` on `ecom_prod.gold`, and `SELECT` on the table (or the schema).

33. **Why disable shared key access on a storage account?**
    Account keys give full access and bypass RBAC and ACLs. Without them, all access goes through Entra identities that can be audited and revoked.

34. **List five audit or detection sources for a data platform.**
    StorageBlobLogs, Azure SQL auditing, Databricks audit logs, Key Vault logs, Entra sign-in logs (plus Defender for Cloud and Sentinel).

35. ➕ **Design security for the e-commerce lakehouse in one paragraph.**
    Purview scans and classifies all sources. Bronze is restricted to pipeline identities and engineers, with CMK, private endpoints and short retention. Silver replaces PII with keyed hashes (`customer_hash`), with Unity Catalog masks and row filters. A separate PII vault is restricted to one group. Gold holds the star schema for analysts, with `full_name` masked. Access uses managed identities, Key Vault, and group-based least privilege. Audit logs go to Log Analytics with alerts.
