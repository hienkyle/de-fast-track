# Unit 09 — Azure Fundamentals & Core Infrastructure: Study Guide

**Scope:** Intro to cloud computing and Azure global infrastructure · Core Azure services (resource groups, subscriptions) · Identity and access management (Entra ID) · Virtual networking (VNet, subnets, NSGs)

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform from earlier units. Up to now, the data sat in Postgres, Snowflake and local Spark. Here it moves to **Azure**: a data lake for Bronze/Silver/Gold, compute for Spark and dbt, and a private network with locked-down access around it all.

---

## Part A — Intro to Cloud Computing

### 1. What cloud computing is

**Cloud computing** means renting computing resources (servers, storage, databases, networking, software) over the internet from a provider, and **paying for what you use** instead of buying and running your own hardware.

➕ **Added — the five NIST characteristics** (the textbook definition):

| Characteristic | Meaning |
|---|---|
| **On-demand self-service** | Create a VM or database yourself in minutes, with no ticket to IT |
| **Broad network access** | Reach it over the network from anywhere |
| **Resource pooling** | The provider shares physical hardware across many customers (multi-tenancy) |
| **Rapid elasticity** | Scale up or down quickly, sometimes automatically |
| **Measured service** | Usage is metered and billed (per second, per GB, per request) |

### 2. CapEx vs OpEx and the consumption model

| | **CapEx (capital expenditure)** | **OpEx (operational expenditure)** |
|---|---|---|
| What | Big **up-front** spend on physical assets | **Ongoing** spend on services as you use them |
| Example | Buying servers and a data center | Paying the Azure bill each month |
| Risk | Guess capacity years ahead: too much wastes money, too little means outages | Pay for actual usage, adjust anytime |
| Model | On-premises | **Cloud (consumption-based / pay-as-you-go)** |

Cloud moves spending from **CapEx to OpEx**. For data engineering this matters a lot: a Spark cluster that runs 2 hours a night costs 2 hours of compute, not a server that idles 22 hours a day.

### 3. Cloud benefits

| Benefit | Meaning |
|---|---|
| **High availability** | The service stays up. Measured by an **SLA (Service Level Agreement)**: the provider's written uptime promise, e.g. 99.9%, with service credits if they miss it |
| **Scalability** | Handle more load. **Vertical** = bigger machine (scale up). **Horizontal** = more machines (scale out). |
| **Elasticity** | Scale **automatically** with demand, then back down |
| **Reliability** | Recover from failures. Resources spread across zones and regions. |
| **Predictability** | Of **performance** (autoscale, load balancing) and **cost** (pricing calculator, budgets) |
| **Security & governance** | Built-in policies, compliance certifications, identity controls |
| **Manageability** | Manage **of** the cloud (portal, CLI, templates, monitoring) and **in** the cloud (autoscale, alerts) |

➕ **Added — composite SLA:** when services depend on each other, multiply their SLAs. A VM at 99.9% calling a database at 99.99% gives 0.999 × 0.9999 ≈ **99.89%**. The chain is less available than any one part.

### 4. Service models: IaaS, PaaS, SaaS

The "aaS" means **"as a Service"**: you rent it instead of owning it. The first letter says **how much of the stack you rent**:

- **IaaS = Infrastructure as a Service:** you rent raw building blocks (virtual machines, disks, networks) and set up everything on top yourself.
- **PaaS = Platform as a Service:** you rent a ready-made platform (runtime, database engine, managed Spark) and bring only your code and data.
- **SaaS = Software as a Service:** you rent finished software and just use it.

| Model | You manage | Provider manages | Azure examples | DE example |
|---|---|---|---|---|
| **IaaS** | OS, runtime, apps, data | Hardware, network, virtualization | Virtual Machines, VNets, disks | Running your own Postgres or Airflow on a VM |
| **PaaS** | Apps and data | Everything up to the runtime | App Service, Azure SQL Database, Azure Functions, Databricks | Azure SQL instead of Postgres on a VM |
| **SaaS** | Just your data and settings | Everything | Microsoft 365, Power BI service | Business users reading dashboards |

➕ **Added — serverless:** a PaaS style where you don't pick or manage servers at all, and pay per execution (Azure Functions, serverless SQL pools). Good for small, event-driven jobs, like processing a file when it lands in storage.

### 5. The shared responsibility model

Security is split between you and Microsoft, and **the split moves with the service model**.

| Responsibility | On-prem | IaaS | PaaS | SaaS |
|---|---|---|---|---|
| Physical data center, hosts, network | You | Microsoft | Microsoft | Microsoft |
| Operating system, patching | You | **You** | Microsoft | Microsoft |
| Network controls (NSGs, firewalls) | You | **You** | Shared | Microsoft |
| Applications | You | You | **You** | Microsoft |
| Identity and access | You | You | You | **You** |
| **Data, accounts, devices** | **You** | **You** | **You** | **You** |

**Always yours, in every model:** your **data**, your **accounts/identities**, and the **devices** that access them. Microsoft never takes responsibility for who you give access to.

### 6. Deployment models

| Model | Meaning | Example |
|---|---|---|
| **Public cloud** | Shared provider infrastructure, available to anyone | Azure, AWS, GCP |
| **Private cloud** | Cloud-style infrastructure used by one organization (on-prem or hosted) | A bank's own data center running a cloud platform |
| **Hybrid cloud** | Public + private connected together | On-prem SQL Server replicating to Azure |
| ➕ **Multi-cloud** | Using more than one public provider | Snowflake on AWS + Azure Databricks |

➕ **Added — Azure Arc:** manages servers, Kubernetes clusters and databases outside Azure (on-prem, other clouds) as if they were Azure resources. **Azure Local** (formerly Azure Stack HCI) runs Azure services on your own hardware.

---

## Part B — Azure Global Infrastructure

### 1. The building blocks, from smallest to largest

```
Datacenter  →  Availability Zone  →  Region  →  Region pair  →  Geography
(a building)   (1+ datacenters with   (a set of zones   (two regions in    (e.g. Asia Pacific,
               own power/cooling/     close together,    the same geography  Europe; data
               network)               low latency)       for disaster        residency and
                                                          recovery)          compliance)
```

| Concept | What it is | Protects against |
|---|---|---|
| **Datacenter** | A physical facility with servers. You never pick one directly. | — |
| **Availability Zone (AZ)** | A physically separate location inside a region, with **independent power, cooling and networking**. Zone-enabled regions have **at least three zones**. | A single **datacenter failure** (fire, power loss) |
| **Region** | A set of datacenters in one area, connected by a low-latency network. You **choose a region** for almost every resource. | — |
| **Region pair** | Two regions in the same geography, usually hundreds of km apart, paired for disaster recovery. Updates roll out to one at a time, and one is prioritized for recovery in a wide outage. | A whole **region failing** (natural disaster) |
| **Geography** | A market containing one or more regions, with shared data residency and compliance rules | — |

Example pair: **Southeast Asia (Singapore) ↔ East Asia (Hong Kong)**. Some newer regions have no pair and rely on availability zones instead.

**Where a data team in Vietnam would usually deploy:** Southeast Asia (Singapore) for lowest latency, unless compliance or pricing points elsewhere.

### 2. Zonal vs zone-redundant vs non-zonal

| Deployment | Meaning | Example |
|---|---|---|
| **Zonal** | Pinned to **one** zone you choose | A VM in Zone 1 |
| **Zone-redundant** | Replicated **across zones** automatically | ZRS storage, zone-redundant Azure SQL |
| **Non-zonal / regional** | Azure places it anywhere in the region, no zone guarantee | Many basic services |

For high availability, put **two VMs in different zones** behind a load balancer.

### 3. Choosing a region

- **Latency:** close to users and to the data sources
- **Compliance / data residency:** some data must stay in a country or geography
- **Service availability:** not every service or VM size exists in every region
- **Price:** the same service costs different amounts in different regions
- **Zones and pairs:** does the region support availability zones? Does it have a pair?

➕ **Added — sovereign clouds:** physically and logically separate instances of Azure: **Azure Government** (US government) and **Azure operated by 21Vianet** (China). They have their own portals and aren't reachable from a normal subscription.

---

## Part C — Core Azure Services and Resource Organization

### 1. The management hierarchy

```
Microsoft Entra tenant  (your organization's identity directory)
└── Root management group
    └── Management groups   (e.g. "Data Platform", "Corp")      ← policies/RBAC for many subscriptions
        └── Subscriptions   (e.g. "ecom-dev", "ecom-prod")      ← billing + access boundary
            └── Resource groups  (e.g. rg-ecom-data-prod-sea)   ← lifecycle container
                └── Resources   (storage account, VM, VNet, Databricks workspace...)
```

**Settings flow downward:** an RBAC role or Azure Policy assigned at a management group is **inherited** by every subscription, resource group and resource below it.

### 2. Resources

A **resource** is one manageable item: a VM, a storage account, a VNet, a database, a public IP. Every resource:

- lives in **exactly one resource group**
- has a **resource type** (`Microsoft.Storage/storageAccounts`) and usually a **region**
- has a unique **resource ID**:

```
/subscriptions/<sub-id>/resourceGroups/rg-ecom-data-prod-sea/providers/Microsoft.Storage/storageAccounts/stecomlakeprod
```

You'll use resource IDs as the **scope** in RBAC assignments (Part D).

### 3. Resource groups

A **resource group (RG)** is a logical container for resources that share a **lifecycle**: created, managed and deleted together.

| Rule | Detail |
|---|---|
| One resource → one RG | A resource can't be in two RGs |
| No nesting | RGs can't contain other RGs |
| **Deleting an RG deletes everything in it** | Handy for tearing down a dev environment. Dangerous in prod (use **locks**). |
| RG has a location | That's only where its **metadata** is stored. Resources inside can be in **other regions**. |
| Resources can move | Between RGs and subscriptions (most resource types) |
| Scope for access and policy | Grant a team Contributor on one RG instead of the whole subscription |

**How to group:** by **lifecycle and environment**, e.g. `rg-ecom-data-dev-sea` and `rg-ecom-data-prod-sea`, or by workload component (`rg-ecom-net-prod-sea`, `rg-ecom-lake-prod-sea`).

### 4. Subscriptions

A **subscription** is an agreement with Microsoft to use Azure, linked to an account for **billing**. It's two boundaries:

| Boundary | Meaning |
|---|---|
| **Billing boundary** | Each subscription gets its own invoice and cost reports. Separate subscriptions = separate costs (dev vs prod, team A vs team B). |
| **Access control boundary** | RBAC and policies can be applied per subscription, e.g. only the platform team has access to prod |

- A subscription **trusts exactly one Entra tenant**. A tenant can have **many** subscriptions.
- Subscriptions have **limits and quotas** (e.g. vCPUs per region). Big workloads sometimes hit them and request an increase.
- Common types: Free account / Azure for Students, Pay-As-You-Go, Enterprise Agreement, Microsoft Customer Agreement.

**Typical split:** one subscription per environment (`ecom-dev`, `ecom-test`, `ecom-prod`) so prod costs and access are isolated.

### 5. Management groups

Containers **above subscriptions** to apply governance to many subscriptions at once.

- Up to **6 levels deep** (not counting the root)
- Each subscription or management group has **one parent**
- Example: assign an Azure Policy "only Southeast Asia and East Asia regions allowed" at the "Data Platform" management group, and every subscription below inherits it

### 6. Azure Resource Manager (ARM) and ways to manage Azure

**Azure Resource Manager** is the deployment and management layer. **Every request** (portal click, CLI command, Terraform, SDK) goes through ARM, which authenticates it, checks RBAC and policy, then sends it to the resource provider. That's why access control works the same everywhere.

| Tool | Use |
|---|---|
| **Azure portal** | Web UI. Good for learning and exploring. |
| **Azure CLI** (`az ...`) | Cross-platform command line. Good for scripts. |
| **Azure PowerShell** (`New-AzResourceGroup ...`) | PowerShell cmdlets |
| **Azure Cloud Shell** | Browser-based shell with CLI + PowerShell preinstalled |
| **ARM templates** (JSON) / **Bicep** | Microsoft's **Infrastructure as Code (IaC)**: declare resources in a file, deploy repeatably |
| ➕ **Terraform** | Popular multi-cloud IaC tool, widely used by data teams |
| **SDKs** | Python (`azure-identity`, `azure-storage-blob`), etc. |

➕ **Added — Bicep example** (IaC ties back to Unit 1: infrastructure in Git, reviewed in PRs):

```bicep
param location string = resourceGroup().location

resource lake 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'stecomlakedev'
  location: location
  sku: { name: 'Standard_ZRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true              // hierarchical namespace = ADLS Gen2
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
  }
}
```

### 7. Governance tools

| Tool | Does | Example |
|---|---|---|
| **Tags** | Name/value labels on resources, RGs and subscriptions. Used for cost reports, automation, ownership. **Not inherited by default.** | `env=prod`, `owner=data-team`, `costcenter=1234` |
| **Resource locks** | Prevent accidental changes. **Override RBAC**: even an Owner must remove the lock first. | `CanNotDelete` (can modify, can't delete) · `ReadOnly` (can't modify or delete) |
| **Azure Policy** | Enforce or audit rules on resource **properties** | Allowed regions, require an `env` tag, deny public IPs, storage must disable public blob access |
| **Azure RBAC** | Controls **who can do what** (Part D) | Data engineers get Contributor on the dev RG |
| ➕ **Cost Management + budgets** | Track spend, set budgets and alerts | Alert at 80% of the monthly dev budget |
| ➕ **Pricing & TCO calculators** | Estimate costs before deploying / compare with on-prem | |
| ➕ **Azure Advisor** | Recommendations on cost, security, reliability, performance | "This VM is underused, resize it" |

**Policy vs RBAC:** RBAC controls **what a user can do** (can Hien create VMs?). Policy controls **what resources can look like** (whoever creates a VM, it must be in Southeast Asia). You usually need both.

➕ **Added — naming convention:** a consistent pattern like `<type>-<workload>-<env>-<region>`, e.g. `rg-ecom-data-prod-sea`, `vnet-ecom-prod-sea`, `snet-databricks-public`, `nsg-app-prod`. Storage account names are the exception: **3–24 lowercase letters and numbers only, globally unique** (`stecomlakeprod`).

### 8. Core services overview

**Compute:**

| Service | Model | Use |
|---|---|---|
| **Virtual Machines** | IaaS | Full control: self-hosted Airflow, legacy tools |
| **VM Scale Sets** | IaaS | Many identical VMs that autoscale |
| **App Service** | PaaS | Web apps and APIs (your Unit 4 FastAPI app) |
| **Azure Functions** | Serverless | Event-driven code (run when a file lands) |
| **Container Instances / Container Apps** | PaaS / serverless | Run containers without managing servers |
| **AKS (Kubernetes Service)** | Managed Kubernetes | Container orchestration at scale |

**Storage — the storage account (plus managed disks):**

| Service | Stores | DE use |
|---|---|---|
| **Blob Storage** | Unstructured objects (files) | Raw files, Parquet, backups |
| **Data Lake Storage Gen2 (ADLS Gen2)** | Blob storage + **hierarchical namespace** (real folders, POSIX-style ACLs) | **The data lake:** `bronze/`, `silver/`, `gold/` containers |
| **Azure Files** | SMB/NFS file shares | Shared config, lift-and-shift apps |
| **Queue Storage** | Simple messages | Lightweight job queues |
| **Table Storage** | NoSQL key-value | Simple metadata |
| **Managed Disks** | Block storage for VMs (a separate resource, not part of a storage account) | VM OS/data disks |

**Storage redundancy:**

| Option | Copies | Survives |
|---|---|---|
| **LRS** (locally redundant) | 3 in **one datacenter** | A disk/server failure |
| **ZRS** (zone-redundant) | 3 across **3 zones** | A zone (datacenter) failure |
| **GRS** (geo-redundant) | LRS + 3 async copies in the **paired region** | A region failure |
| **GZRS** | ZRS + LRS in the paired region | Zone **and** region failures |
| **RA-GRS / RA-GZRS** | As above, plus **read access** to the secondary | Read during a primary outage |

**Blob access tiers:** **Hot** (frequent access) → **Cool** (≥30 days) → **Cold** (≥90 days) → **Archive** (≥180 days, offline, hours to rehydrate). Cheaper storage, more expensive reads as you go down. ➕ **Added — lifecycle management** rules move old Bronze files to Cool/Cold automatically. (Not Archive: it isn't supported on ZRS accounts like our lake, Unit 10.)

**Databases:** Azure SQL Database, Azure Database for PostgreSQL / MySQL (flexible server), Cosmos DB (globally distributed NoSQL).

---

## Part D — Identity and Access Management (Microsoft Entra ID)

### 1. Authentication vs authorization

| | **Authentication (AuthN)** | **Authorization (AuthZ)** |
|---|---|---|
| Question | **Who are you?** | **What are you allowed to do?** |
| Handled by | **Microsoft Entra ID** (passwords, MFA, tokens) | **Azure RBAC** (for Azure resources), Entra roles (for the directory) |
| Order | First | Second |

### 2. Microsoft Entra ID (formerly Azure Active Directory)

**Azure AD was renamed Microsoft Entra ID** in 2023. Same service, new name. You'll still see "Azure AD" and `aad` in older docs, CLI flags and code.

Entra ID is Microsoft's **cloud identity and access management service**. It stores identities and issues **tokens** that prove who someone is to Azure, Microsoft 365 and thousands of other apps.

| Term | Meaning |
|---|---|
| **Tenant** | A dedicated instance of Entra ID for one organization (`contoso.onmicrosoft.com`). The top-level identity boundary. |
| **User** | A person's identity. **Member** (internal) or **guest** (external, B2B). |
| **Group** | A set of users (or other principals). **Security groups** for access. Assign roles to groups, not individuals. Membership can be **assigned** or **dynamic** (rule-based, e.g. `department = Data`). |
| **Service principal** | The identity of an **application or automation** in a tenant |
| **App registration** | The app's definition (client ID, credentials, permissions). Creating one creates a service principal. |
| **Managed identity** | A service principal that **Azure manages for you**: no secrets to store or rotate (below) |
| **Device** | Registered/joined laptops and phones, used in access policies |

**Entra ID vs on-prem Active Directory (AD DS):**

| | **On-prem Active Directory** | **Microsoft Entra ID** |
|---|---|---|
| Protocols | Kerberos, NTLM, LDAP | HTTPS: **OAuth 2.0, OpenID Connect, SAML** |
| Structure | Domains, forests, OUs, Group Policy | Flat tenant, groups, admin units |
| Built for | Servers and PCs on a corporate network | Cloud apps and the internet |

➕ **Added — hybrid identity:** **Microsoft Entra Connect** (Connect Sync / Cloud Sync) syncs on-prem AD users to Entra ID so people use one account for both.

### 3. Security principals for automation: service principal vs managed identity

Pipelines (ingestion jobs, Spark jobs, Functions, a dbt runner) need identities too, to read the data lake or Key Vault.

| | **Service principal + secret/certificate** | **Managed identity** |
|---|---|---|
| Credentials | You create a client secret or certificate and must **store and rotate** it | **None to handle**: Azure issues tokens automatically |
| Where it works | Anywhere (GitHub Actions, on-prem, other clouds) | Only on **Azure resources** that support it |
| Risk | Secrets leak (committed to Git, pasted in notebooks) | Much lower |
| **Prefer** | Only when a managed identity isn't possible | **Always, when running inside Azure** |

**Two kinds of managed identity:**

| | **System-assigned** | **User-assigned** |
|---|---|---|
| Lifecycle | Tied to **one resource**: deleted when the resource is deleted | **Standalone** resource with its own lifecycle |
| Sharing | One resource only | Can be attached to **many resources** |
| Use | A single ingestion service reading the lake | Several Functions or VMs sharing the same permissions |

**Unit 8: a secret, even though it's kept out of Git.** The password lives in an environment variable, so someone has to create it, store it safely and rotate it:

```yaml
# ~/.dbt/profiles.yml (Unit 8)
password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
```

**Azure with a managed identity: no secret at all.** Azure issues the token:

```python
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

cred = DefaultAzureCredential()   # tries env vars → managed identity → Azure CLI login → ...
lake = DataLakeServiceClient("https://stecomlakeprod.dfs.core.windows.net", credential=cred)
for p in lake.get_file_system_client("bronze").get_paths(path="orders/2026/09/"):
    print(p.name)
```

The same code runs on your laptop (using your `az login`) and in Azure (using the resource's managed identity). There's nothing to leak.

➕ **Added — workload identity federation:** lets GitHub Actions (or another cloud) get Entra tokens **without any secret**, by trusting the external system's tokens. The best choice for CI/CD deploying to Azure.

### 4. Authentication features

| Feature | Meaning |
|---|---|
| **Single sign-on (SSO)** | One sign-in for many apps |
| **Multifactor authentication (MFA)** | Something you **know** (password) + something you **have** (phone, key) or **are** (fingerprint). Microsoft enforces MFA for signing in to the Azure portal, CLI and PowerShell. |
| **Passwordless** | Microsoft Authenticator, Windows Hello, **FIDO2 passkeys** |
| **Conditional Access** | **If-then policies** evaluated at sign-in: *if* user is in group X, from an unknown location or risky device, *then* require MFA / block / require a compliant device. Needs Entra ID **P1**. |
| ➕ **Self-service password reset (SSPR)** | Users reset their own passwords |
| ➕ **Identity Protection** | Detects risky sign-ins (leaked credentials, impossible travel). Needs **P2**. |

➕ **Added — Entra ID editions:** **Free** (users, groups, basic SSO), **P1** (Conditional Access, dynamic groups, hybrid features), **P2** (Identity Protection, **Privileged Identity Management**, access reviews).

➕ **Added — external identities:** **B2B collaboration** invites partners as guest users in your tenant. **Microsoft Entra External ID** handles sign-in for customer-facing apps.

### 5. Azure RBAC (role-based access control)

**RBAC decides what an authenticated principal can do to Azure resources.** A **role assignment** has three parts:

```
WHO             +     WHAT               +     WHERE
Security principal    Role definition          Scope
(user, group,         (set of allowed          (management group, subscription,
 service principal,    actions)                 resource group, or a single resource)
 managed identity)
```

Example: *group `grp-data-engineers`* + *Contributor* + *resource group `rg-ecom-data-dev-sea`*.

**Scope and inheritance:** an assignment applies to its scope **and everything below it**. Contributor on a subscription = Contributor on every RG and resource in it. **RBAC is additive:** your effective permissions are the **union** of all your assignments.

**Fundamental built-in roles:**

| Role | Can do | Can't do |
|---|---|---|
| **Owner** | Everything, **including granting access** to others | — |
| **Contributor** | Create and manage all resources | **Grant access** (assign roles) |
| **Reader** | View resources | Change anything |
| **User Access Administrator** | Manage access (role assignments) | Manage resources |
| ➕ **Role Based Access Control Administrator** | Assign roles, can be limited to specific roles/principals | Manage resources |

**Control plane vs data plane (important for data engineers):**

| Plane | Controls | Example roles |
|---|---|---|
| **Control plane (management)** | Managing the **resource itself**: create, configure, delete the storage account | Owner, Contributor, Reader, Storage Account Contributor |
| **Data plane** | Accessing the **data inside**: reading/writing blobs | **Storage Blob Data Reader / Contributor / Owner**, Key Vault Secrets User |

A classic surprise: **Contributor on a storage account doesn't give Entra-based access to read the blobs.** You need a data role like **Storage Blob Data Contributor**. (Contributor can list the account keys, which is one reason to disable key access, see below.)

**Custom roles:** when no built-in role fits, define one with `Actions`, `NotActions`, `DataActions`, `NotDataActions` and `AssignableScopes`.

➕ **Added — deny assignments:** block specific actions even if a role allows them. You can't create them directly; Azure creates them (e.g. for managed apps, Deployment Stacks).

### 6. Entra roles vs Azure RBAC roles

Two separate role systems that are easy to confuse:

| | **Microsoft Entra roles** | **Azure RBAC roles** |
|---|---|---|
| Control | The **directory**: users, groups, app registrations, licenses | **Azure resources**: VMs, storage, VNets |
| Examples | Global Administrator, User Administrator, Application Administrator | Owner, Contributor, Reader, Storage Blob Data Reader |
| Scope | Tenant (or administrative unit) | Management group → subscription → RG → resource |

A **Global Administrator** doesn't automatically have access to Azure subscriptions (they can **elevate** themselves to User Access Administrator at root, which is logged).

### 7. Security principles and practices

- **Least privilege:** grant the **minimum** role at the **narrowest** scope needed. Reader before Contributor, one RG before a whole subscription.
- **Assign to groups, not individuals:** people join and leave, and group membership is easier to review.
- **Zero Trust:** "never trust, always verify": verify explicitly (every request, using identity, location, device), use least-privilege access, **assume breach**.
- **Defense in depth:** layers of protection so one failure isn't fatal: physical → identity & access → perimeter → **network** → compute → application → **data**.
- **Prefer Entra auth over keys:** disable storage **shared key** access and SAS where possible, so all access goes through RBAC and is auditable.
- ➕ **Privileged Identity Management (PIM):** **just-in-time** admin access: become Owner for 2 hours with approval, instead of permanently. (P2)
- ➕ **Access reviews:** periodically confirm people still need their access. (P2)
- ➕ **Key Vault:** store remaining secrets (API keys for external sources, database passwords) and read them with a managed identity + **Key Vault Secrets User** role, instead of putting them in code or pipeline configs.

---

## Part E — Virtual Networking (VNet, Subnets, NSGs)

### 1. Virtual Network (VNet)

A **VNet** is your **private network in Azure**. Resources in it (VMs, private endpoints, Databricks clusters) talk to each other privately and are isolated from other customers.

| Property | Detail |
|---|---|
| **Scope** | One **region**, one **subscription** (it spans all zones in that region) |
| **Address space** | One or more private IP ranges in **CIDR** notation, usually RFC 1918: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` |
| **Isolation** | Inbound from the internet is blocked unless you add a public IP / load balancer and allow it |
| **Communication** | Resources in the same VNet can reach each other by default, across subnets |
| **Name resolution** | Azure-provided DNS by default, or your own / Azure Private DNS zones |

**CIDR refresher:**

| CIDR | Addresses | Typical use |
|---|---|---|
| `/16` | 65,536 | A whole VNet (`10.10.0.0/16`) |
| `/24` | 256 | A subnet (`10.10.1.0/24`) |
| `/26` | 64 | A small subnet (Bastion needs /26 or larger) |
| `/29` | 8 | Smallest allowed IPv4 subnet |

Number of addresses = 2^(32 − prefix). **Plan address spaces so they don't overlap** with on-prem networks or other VNets, or you can't peer/connect them later.

### 2. Subnets

A **subnet** is a range inside the VNet's address space. You split a VNet into subnets to **group resources by role** and apply different security rules to each.

```
vnet-ecom-prod-sea   10.10.0.0/16
├── snet-app             10.10.1.0/24   ← FastAPI on App Service (VNet integration) / VMs
├── snet-data            10.10.2.0/24   ← private endpoints: storage, SQL, Key Vault
├── snet-dbx-host        10.10.4.0/22   ← Databricks (needs two delegated subnets)
├── snet-dbx-container   10.10.8.0/22
└── AzureBastionSubnet   10.10.255.0/26 ← name is required exactly like this
```

**Azure reserves 5 IP addresses in every subnet:**

| Address (for `10.10.1.0/24`) | Reserved for |
|---|---|
| `10.10.1.0` | Network address |
| `10.10.1.1` | Default gateway |
| `10.10.1.2`, `10.10.1.3` | Azure DNS |
| `10.10.1.255` | Broadcast |

So a `/24` gives **251** usable addresses, and a `/29` gives only **3**.

- Subnet ranges can't overlap each other and must fit in the VNet's address space.
- Some services need a **dedicated or delegated subnet** (Databricks, App Service VNet integration, Azure Firewall, VPN gateway `GatewaySubnet`).
- ➕ **Added — private subnets and outbound access:** VMs used to get **default outbound internet access** automatically. Microsoft is retiring this: new VNets now default to **private subnets** without it, so give outbound traffic an explicit path, like a **NAT gateway**, a load balancer outbound rule, or a firewall.

### 3. Network Security Groups (NSGs)

An **NSG** is a list of **allow/deny rules** that filter traffic to and from resources. It's a basic, **stateful** firewall at layers 3–4 (IP, port, protocol).

**Each rule has:**

| Field | Example |
|---|---|
| **Priority** | **100–4096. Lower number = evaluated first.** Processing **stops at the first match.** |
| Name | `Allow-HTTPS-Inbound` |
| **Direction** | Inbound or Outbound |
| **Source / destination** | IP/CIDR, **service tag**, or **application security group** |
| Ports | `443`, `1433`, `8000-8100`, `*` |
| Protocol | TCP, UDP, ICMP, Any |
| **Action** | Allow or Deny |

**Stateful** means that if an inbound request is allowed, **its response is automatically allowed back out**. You don't write a matching outbound rule.

**Default rules** (can't be deleted, but your rules with lower numbers override them):

| Direction | Priority | Name | Effect |
|---|---|---|---|
| Inbound | 65000 | AllowVNetInBound | Allow traffic from within the VNet (and peered/connected networks) |
| Inbound | 65001 | AllowAzureLoadBalancerInBound | Allow Azure Load Balancer health probes |
| Inbound | 65500 | **DenyAllInBound** | **Deny everything else** |
| Outbound | 65000 | AllowVnetOutBound | Allow traffic to the VNet |
| Outbound | 65001 | **AllowInternetOutBound** | Allow outbound to the internet |
| Outbound | 65500 | DenyAllOutBound | Deny everything else |

So by default: **nothing from the internet gets in, and everything can go out.**

**Where to attach an NSG:**

| Attached to | Applies to |
|---|---|
| **Subnet** | Every resource in the subnet (recommended, simpler) |
| **Network interface (NIC)** | One VM |
| Both | Traffic must be allowed by **both**. Inbound: subnet NSG first, then NIC NSG. Outbound: NIC first, then subnet. |

One NSG can be attached to many subnets/NICs. Each subnet or NIC can have **at most one** NSG.

**Service tags** are Microsoft-managed names for groups of IP ranges, so you don't hard-code IPs: `Internet`, `VirtualNetwork`, `AzureLoadBalancer`, `Storage`, `Storage.SoutheastAsia`, `Sql`, `AzureCloud`, `AzureDatabricks`.

**Application security groups (ASGs)** let you group VMs by role and write rules like "`asg-web` can reach `asg-db` on 5432" instead of listing IP addresses.

**Worked example — the app subnet:**

| Priority | Name | Dir | Source | Dest | Port | Action |
|---|---|---|---|---|---|---|
| 100 | Allow-HTTPS-Internet | In | Internet | snet-app | 443 | Allow |
| 110 | Allow-SSH-Bastion | In | 10.10.255.0/26 | snet-app | 22 | Allow |
| 4000 | Deny-All-Other-In | In | Any | Any | * | Deny |

**Common mistakes:** giving an allow rule a *higher* number than a deny rule that matches the same traffic (the deny wins because it's checked first). Opening SSH/RDP (22/3389) to `Internet`, which bots scan constantly. Use **Azure Bastion** or just-in-time access instead.

➕ **Added — troubleshooting:** **Network Watcher** → *IP flow verify* tells you which NSG rule allowed or denied a specific packet. *Effective security rules* shows the merged subnet + NIC rules for a VM.

### 4. Connecting networks ➕ Added

| Option | Connects | Notes |
|---|---|---|
| **VNet peering** | VNet ↔ VNet (same or different regions: **global peering**) | Private, low latency over Microsoft's backbone. **Not transitive**: if A↔B and B↔C, A can't reach C. |
| **VPN Gateway** (site-to-site) | On-prem network ↔ VNet | Encrypted tunnel **over the public internet** |
| **VPN Gateway** (point-to-site) | A single laptop ↔ VNet | For individual developers |
| **ExpressRoute** | On-prem ↔ Azure | **Private dedicated connection** through a provider, not over the internet. Higher bandwidth, more consistent, more expensive. |

**Hub-and-spoke:** a central **hub** VNet holds shared services (firewall, VPN gateway, Bastion), and workload **spoke** VNets peer to it. Common in enterprises.

### 5. Reaching PaaS services privately ➕ Added

Storage accounts, Azure SQL and Key Vault have **public endpoints** by default. To keep data traffic off the internet:

| | **Service endpoint** | **Private endpoint (Private Link)** |
|---|---|---|
| How | Traffic from your subnet reaches the service's **public** endpoint over the Azure backbone. The service firewall allows your subnet. | The service gets a **private IP inside your VNet** (a NIC in `snet-data`) |
| Service's public endpoint | Still public (restricted by firewall rules) | Can be **fully disabled** |
| Reachable from on-prem / peered VNets | No | **Yes** |
| DNS | No change | Needs **Private DNS zone** (`privatelink.dfs.core.windows.net`) |
| Cost | Free | Per-hour + per-GB |
| Best for | Simple setups | **Production data platforms** (the recommended default) |

### 6. Other networking services ➕ Added

| Service | Does |
|---|---|
| **Public IP address** | Gives a resource an internet-facing IP |
| **NAT gateway** | Gives a subnet a stable **outbound** IP (useful when a source API allow-lists your IP) |
| **Azure Load Balancer** | Layer 4 (TCP/UDP) load balancing |
| **Application Gateway** | Layer 7 (HTTP) load balancing + **WAF** (web application firewall) |
| **Azure Firewall** | Managed, **stateful layer 3–7 firewall** for a whole network (FQDN filtering, threat intel). NSG = per-subnet filter. Firewall = central network-wide control. |
| **Azure Bastion** | Browser-based RDP/SSH to VMs **without public IPs** |
| **Azure DNS / Private DNS** | Public and private name resolution |
| **Route tables (UDRs)** | Override default routing, e.g. send all outbound through the firewall |
| **DDoS Protection** | Protects public IPs from DDoS attacks |

---

## Part F — Putting It Together: A Secure E-commerce Data Platform ➕ Added

The layout below uses two services that get their own unit later. For now, all you need to know is what job each one does:

- **Azure Data Factory (ADF):** the **ingestion** service. It copies raw data from the source systems into the Bronze container.
- **Azure Databricks:** managed **Spark**, the same PySpark from Unit 7 running on Azure. It turns Bronze into Silver and Gold.

This part is about the **identities and permissions** each one needs, not how they work inside.

```
Entra tenant: hienco.onmicrosoft.com
└── Management group: Data Platform   (Policy: allowed regions = SEA/EA, require tag env)
    ├── Subscription: ecom-dev
    │   └── rg-ecom-data-dev-sea         (grp-data-engineers = Contributor)
    └── Subscription: ecom-prod
        ├── rg-ecom-net-prod-sea         (CanNotDelete lock)
        │   └── vnet-ecom-prod-sea 10.10.0.0/16
        │       ├── snet-app   + nsg-app-prod   (443 from Internet)
        │       ├── snet-data  + nsg-data-prod  (private endpoints only)
        │       └── snet-dbx-* (Databricks, VNet-injected)
        └── rg-ecom-data-prod-sea        (grp-data-engineers = Reader; deploys via CI only)
            ├── stecomlakeprod  ADLS Gen2, ZRS, public access disabled, private endpoint
            │     containers: bronze / silver / gold
            ├── adf-ecom-prod   (ingestion; system-assigned MI → Storage Blob Data Contributor on bronze)
            ├── dbx-ecom-prod   (Spark; MI → Blob Data Contributor on bronze + silver + gold: it also writes Bronze tables, Units 11–12)
            ├── kv-ecom-prod    (source API keys; MIs have Key Vault Secrets User)
            └── grp-analysts → ACL read on the gold folders they need (no container-wide data role, Units 10 & 15)
```

The whole unit in one picture: **governance** comes down the hierarchy, **identity** decides who (and which pipeline) can touch which data, and the **network** keeps that data off the public internet.

---

## Key Terms Cheat Sheet

- **Cloud computing:** renting IT resources over the internet, paying for what you use
- **CapEx vs OpEx:** up-front hardware purchase vs ongoing pay-as-you-go spending
- **Scalability vs elasticity:** can grow (vertical = bigger, horizontal = more) / grows and shrinks automatically
- **SLA (Service Level Agreement):** the provider's guaranteed uptime percentage. ➕ Composite SLA = multiply the dependent SLAs
- **IaaS / PaaS / SaaS:** Infrastructure / Platform / Software as a Service. You manage OS and up / only app and data / only data and settings
- **Shared responsibility:** data, identities and devices are **always the customer's**
- **Public / private / hybrid cloud:** shared provider / single org / connected mix
- **Region:** a set of datacenters in one area. You pick one per resource.
- **Availability Zone:** separate datacenter(s) in a region with independent power/cooling/network. ≥3 per zone-enabled region.
- **Region pair:** two regions in the same geography for disaster recovery
- **Zonal vs zone-redundant:** pinned to one zone / replicated across zones
- **Hierarchy:** tenant → management groups → subscriptions → resource groups → resources
- **Resource group:** lifecycle container. One RG per resource, no nesting, deleting it deletes everything inside.
- **Subscription:** billing boundary + access control boundary. Trusts one tenant.
- **Management group:** governs many subscriptions. Up to 6 levels.
- **Azure Resource Manager (ARM):** the management layer every request goes through
- **ARM templates / Bicep / Terraform:** Infrastructure as Code
- **Tags:** labels for cost/ownership. Not inherited by default.
- **Resource locks:** `CanNotDelete`, `ReadOnly`. Override RBAC.
- **Azure Policy:** rules about what resources may look like (regions, tags, no public IPs)
- **Storage account services:** Blob, ADLS Gen2 (hierarchical namespace), Files, Queues, Tables
- **Redundancy:** LRS, ZRS, GRS, GZRS, RA-GRS/RA-GZRS
- **Access tiers:** Hot, Cool, Cold, Archive
- **Microsoft Entra ID:** new name for Azure AD. Cloud identity service.
- **Tenant:** an organization's Entra ID instance
- **AuthN vs AuthZ:** who you are (Entra ID) / what you can do (RBAC)
- **Service principal:** identity for an app or automation
- **Managed identity:** Azure-managed service principal with no secrets. System-assigned (one resource) vs user-assigned (shareable).
- **MFA, SSO, Conditional Access:** second factor / one sign-in / if-then sign-in policies (P1)
- **RBAC role assignment:** principal + role definition + scope. Inherited downward. Additive.
- **Owner / Contributor / Reader / User Access Administrator:** everything / manage but not grant / view / grant only
- **Control plane vs data plane:** manage the resource / access the data in it (Storage Blob Data roles)
- **Entra roles vs Azure roles:** directory admin (Global Admin) / resource access (Owner)
- **Least privilege, Zero Trust, defense in depth:** minimum access / verify everything, assume breach / layered security
- ➕ **PIM:** just-in-time privileged access
- ➕ **Key Vault:** secrets, keys, certificates
- **VNet:** private network in one region and subscription, with a CIDR address space
- **Subnet:** range inside a VNet. **5 IPs reserved** per subnet. Smallest /29.
- **NSG:** stateful allow/deny rules. Priority 100–4096, **lower first**, first match wins.
- **NSG default rules:** allow VNet + load balancer inbound, deny all other inbound, allow internet outbound
- **NSG association:** subnet (recommended) and/or NIC. Traffic must pass both.
- **Service tag / ASG:** Microsoft-managed IP groups / your own groups of VMs
- ➕ **VNet peering:** connect VNets. Not transitive.
- ➕ **VPN Gateway vs ExpressRoute:** encrypted over the internet / private dedicated line
- ➕ **Service endpoint vs private endpoint:** backbone route to a public endpoint / private IP in your VNet
- ➕ **NAT gateway, Bastion, Azure Firewall:** outbound IP / VM access without public IP / central L3–L7 firewall

---

## Practice Questions

1. **Explain CapEx vs OpEx, and why cloud suits a nightly Spark job.**
   CapEx is up-front hardware spending; OpEx is paying for usage over time. A nightly job only pays for the hours it runs instead of for a server that sits idle most of the day.

2. **What's the difference between scalability and elasticity? Vertical vs horizontal scaling?**
   Scalability is the ability to handle more load. Elasticity is scaling up and down automatically with demand. Vertical = a bigger machine; horizontal = more machines.

3. **A VM (99.9%) depends on Azure SQL (99.99%). What's the composite SLA?**
   0.999 × 0.9999 ≈ 99.89%.

4. **Classify: a VM running Airflow, Azure SQL Database, Power BI service, Azure Functions.**
   IaaS, PaaS, SaaS, PaaS (serverless).

5. **In the shared responsibility model, what is always the customer's responsibility?**
   Their data, accounts/identities, and the devices that access them.

6. **Who patches the OS for a VM? For Azure SQL Database?**
   VM (IaaS): the customer. Azure SQL (PaaS): Microsoft.

7. **What's an availability zone, and what failure does it protect against? What about a region pair?**
   A physically separate location in a region with independent power, cooling and networking. It protects against a datacenter failure. A region pair protects against an entire region going down.

8. **Name four factors when choosing a region.**
   Latency to users and data, compliance/data residency, whether the service is available there, price, zone and pair support.

9. **Draw the Azure management hierarchy from tenant to resource.**
   Entra tenant → root management group → management groups → subscriptions → resource groups → resources.

10. **Give four rules about resource groups.**
    A resource is in exactly one RG. RGs can't be nested. Deleting an RG deletes everything in it. The RG's location only stores metadata, so its resources can be in other regions.

11. **What two boundaries does a subscription provide? Why use separate dev and prod subscriptions?**
    Billing and access control. It isolates costs and restricts who can touch production.

12. **What's the difference between Azure Policy and RBAC?**
    RBAC controls what a principal can do. Policy controls what properties resources are allowed to have, no matter who creates them.

13. **An Owner tries to delete a resource group with a `CanNotDelete` lock. What happens?**
    The deletion fails. Locks override RBAC, so the lock has to be removed first.

14. **Are tags inherited from a resource group by its resources?**
    Not by default. You can use Azure Policy to inherit or require them.

15. **What does ADLS Gen2 add to Blob Storage, and why does it matter for a data lake?**
    A hierarchical namespace: real directories (fast renames/moves, which Spark and Delta rely on) and POSIX-style ACLs for folder-level security.

16. **Compare LRS, ZRS, GRS and GZRS.**
    LRS: 3 copies in one datacenter. ZRS: 3 copies across zones. GRS: LRS plus an async copy in the paired region. GZRS: ZRS plus a copy in the paired region. RA- versions allow reading from the secondary.

17. **What's the difference between authentication and authorization, and which Azure service handles each?**
    Authentication proves who you are (Entra ID). Authorization decides what you can do (Azure RBAC for resources).

18. **What is Microsoft Entra ID, and what was it called before?**
    Microsoft's cloud identity and access management service; formerly Azure Active Directory.

19. **Why prefer a managed identity over a service principal with a client secret?**
    There are no secrets to store, leak or rotate. Azure issues the tokens automatically.

20. **System-assigned vs user-assigned managed identity?**
    System-assigned is tied to one resource and is deleted with it. User-assigned is a standalone resource that can be shared by many resources.

21. **What does Conditional Access do? Give an example.**
    It applies if-then policies at sign-in, e.g. if a user signs in from outside Vietnam on an unmanaged device, require MFA or block access.

22. **What are the three parts of an RBAC role assignment?**
    Security principal (who), role definition (what), scope (where).

23. **Hien has Reader on the subscription and Contributor on `rg-ecom-data-dev-sea`. What can she do in that RG?**
    Contributor actions. RBAC is additive, so effective permissions are the union of her assignments.

24. **What's the difference between Owner and Contributor?**
    Both manage all resources, but only Owner can grant access to others.

25. **A pipeline's identity has Contributor on the storage account but gets "403 AuthorizationPermissionMismatch" reading blobs with Entra auth. Why, and how do you fix it?**
    Contributor is a control-plane role. Reading data needs a data-plane role, e.g. Storage Blob Data Reader/Contributor at the account or container scope.

26. **Global Administrator vs Owner?**
    Global Administrator is an Entra role that manages the directory. Owner is an Azure RBAC role that manages resources. One doesn't automatically include the other.

27. **Explain least privilege and give a data-platform example.**
    Grant the minimum role at the narrowest scope. Analysts get read access only to the `gold` data they need (e.g. ACL read on `gold/sales/`), not Contributor on the subscription.

28. **What is a VNet's scope?**
    One region and one subscription. It spans all availability zones in that region.

29. **How many usable IPs are in a `/24` subnet and a `/28` subnet in Azure? Why?**
    251 and 11. Azure reserves 5 addresses per subnet: network, gateway, two for DNS, broadcast.

30. **Why must VNet address spaces not overlap with on-prem or other VNets?**
    Overlapping ranges can't be peered or connected by VPN/ExpressRoute, because routing would be ambiguous.

31. **How are NSG rules evaluated?**
    By priority, lowest number first. Processing stops at the first matching rule.

32. **Rule 200 allows TCP 443 from Internet; rule 150 denies all inbound from Internet. Is HTTPS allowed?**
    No. Rule 150 is checked first and matches, so the traffic is denied.

33. **What does "NSGs are stateful" mean?**
    Response traffic for an allowed connection is automatically allowed back, so you don't need a matching rule in the other direction.

34. **List the default inbound NSG rules and the resulting behavior.**
    AllowVNetInBound (65000), AllowAzureLoadBalancerInBound (65001), DenyAllInBound (65500). Traffic inside the VNet is allowed, and everything from the internet is blocked.

35. **An NSG on the subnet allows port 22, and an NSG on the VM's NIC doesn't. Can you SSH in?**
    No. When both exist, inbound traffic must be allowed by both the subnet NSG and the NIC NSG.

36. **What's a service tag? Give two examples.**
    A Microsoft-managed name for a set of IP ranges, used in rules instead of IPs: `Internet`, `VirtualNetwork`, `Storage.SoutheastAsia`, `AzureLoadBalancer`.

37. ➕ **VNets A↔B and B↔C are peered. Can A reach C?**
    No. Peering isn't transitive. Peer A and C directly or route through a hub firewall.

38. ➕ **Service endpoint vs private endpoint: which lets you fully disable the storage account's public endpoint and reach it from on-prem?**
    A private endpoint. It gives the service a private IP in your VNet.

39. ➕ **How would you let admins reach a VM without a public IP or opening port 22 to the internet?**
    Azure Bastion (browser-based SSH/RDP), with the NSG allowing SSH only from the Bastion subnet.

40. ➕ **Design the access model for the lake: ADF (the ingestion service) writes Bronze, Databricks (managed Spark) writes Silver/Gold (and its own Bronze tables), analysts read Gold.**
    Give each service a managed identity and the narrowest data-plane role. ADF's identity: Storage Blob Data Contributor on the `bronze` container. Databricks' identity: Blob Data Contributor on `bronze`, `silver` and `gold` (it reads raw data, and its Auto Loader and streaming jobs in Units 11–12 also write Bronze tables). Analysts' group: ACL read on the `gold` folders they need, not a container-wide data role, which would override the folder ACLs (Unit 10). Then disable shared-key access so every request goes through RBAC, reach the lake through a private endpoint, and keep external API keys in Key Vault.
