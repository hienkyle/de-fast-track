# Unit 16 — DataOps & CI/CD for Data Pipelines: Study Guide

**Scope:** DataOps principles · CI/CD for data pipelines with Azure DevOps or GitHub Actions · Automating deployment of Azure Data Factory and Databricks assets

> ➕ **Added** marks closely related topics that aren't in this unit's syllabus. Everything else comes from the syllabus.

The examples continue the e-commerce platform. Units 11–15 built pipelines that are reliable, monitored and secure. This unit covers how changes to those pipelines get from a developer's branch to production **safely, repeatably and without manual clicking**.

---

## Part A — DataOps Principles

### 1. What DataOps is

**DataOps** applies **DevOps, Agile and Lean manufacturing** ideas to data work. The goal is to deliver trustworthy data quickly and repeatably.

| Source idea | What DataOps borrows |
|---|---|
| **Agile** | Small, frequent increments. Fast feedback from data consumers. |
| **DevOps** | Version control, automated testing, CI/CD, infrastructure as code, collaboration between dev and ops |
| **Lean / statistical process control** | Treat the pipeline like a factory line. Monitor quality at every step, and stop the line when output drifts out of bounds. |

⚠️ **DataOps ≠ DevOps for data.** DevOps manages **code**. DataOps has to manage **code and data**, and data changes even when the code doesn't (new source values, schema drift, late files). So DataOps adds **data tests that run on every pipeline run**, not only at deploy time.

### 2. Core principles

| Principle | Meaning | E-commerce example |
|---|---|---|
| **Everything as code** | Pipelines, SQL, infrastructure, config, permissions and tests all live in Git | ADF JSON, Databricks bundles, dbt models, Bicep, Unity Catalog grants |
| **Automate everything repeatable** | Build, test, deploy and monitor without manual steps | A merge to `main` deploys to test automatically |
| **Test code and data** | Unit tests on logic, data quality tests on output | pytest on the dedup function; dbt `unique` test on `order_id` |
| **Environment isolation** | Separate dev, test and prod, with the same code promoted between them | `ecom_dev` / `ecom_test` / `ecom_prod` catalogs |
| **Small, frequent releases** | Many small changes are safer than one big one | Weekly deploys instead of quarterly |
| **Observability** | Measure pipeline health and data health (Unit 13) | Freshness, volume and failure alerts |
| **Reproducibility** | Any version can be rebuilt from Git + config | Rebuild last Tuesday's deployment exactly |
| **Collaboration & ownership** | Engineers, analysts and business share one workflow | PR reviews (Unit 1), data owners (Unit 15) |
| **Self-service** | Consumers find and use data without filing tickets | Catalog + data products (Unit 15) |

➕ **The DataOps Manifesto** lists 18 principles (e.g. "continually satisfy your customer", "analytics is code", "reduce heroism", "quality is paramount", "monitor quality and performance", "reuse", "improve cycle times"). You don't need to memorize all 18. Know that the manifesto exists and the main themes above.

### 3. The two DataOps loops

```
  INNOVATION LOOP (changing the pipeline)             PRODUCTION LOOP (running the pipeline)
  ─────────────────────────────────────              ──────────────────────────────────────
  Idea → branch → code → test → PR review             Data arrives → pipeline runs → data tests
       → CI → deploy test → approval → prod           → monitor / alert → consumers
       (CI/CD: Parts B–E)                             (Units 11–13)
```

- **Innovation loop:** how fast and safely you can **change** the pipeline. Measured by deployment frequency and lead time.
- **Production loop:** how reliably the pipeline **delivers good data** every day. Measured by freshness, quality and data downtime.
- A mature team is good at both. Fast deployments are worthless if production breaks every time.

### 4. ➕ Measuring DataOps

| Metric | What it measures |
|---|---|
| **Deployment frequency** | How often you release to prod |
| **Lead time for changes** | Commit → running in prod |
| **Change failure rate** | % of deployments that cause an incident or rollback |
| **Time to restore** | How long to recover from a failed change |
| **Data downtime** | Time data is missing, late or wrong (Unit 13) |
| **Test coverage** | % of models/tables with tests |

The first four are the **DORA metrics** from DevOps.

---

## Part B — CI/CD Fundamentals for Data Pipelines

### 1. Terms

| Term | Meaning |
|---|---|
| **Continuous Integration (CI)** | Every change is merged often and **automatically validated**: build, lint, unit tests, config validation |
| **Continuous Delivery (CD)** | Every validated change is **ready to deploy**. Prod deployment needs a **manual approval**. |
| **Continuous Deployment** | Every validated change goes to prod **automatically**, no human gate |
| **Artifact** | The packaged, versioned output of the build (ARM template, Python wheel, bundle) that gets deployed |
| **Environment** | A deployment target (dev, test, prod) with its own resources, config and approvals |
| **Promotion** | Deploying the **same artifact** to the next environment |

⚠️ Most data teams use **Continuous Delivery**: automatic to test, manual approval to prod. Data changes can be expensive to undo (backfills, reprocessed history).

⚠️ **Build once, deploy many.** The artifact built in CI is the one promoted to test and prod. Only **configuration** changes between environments. Rebuilding per environment means prod may run something you never tested.

### 2. A typical data pipeline flow

```
 DEV (developer workspace, not deployed by the pipeline)
 • ADF: Git-connected dev factory, feature branches + Debug
 • Databricks: personal bundle copies (development mode)
      │
 feature branch ──PR──► main ─────────────────────────────────────────────────────────►
      │                  │
      ▼                  ▼
  CI on PR:           BUILD: package artifact ──► DEPLOY TEST ──► [approval] ──► DEPLOY PROD
  • lint / format     (ARM template, bundle,       • integration                 • smoke test
  • unit tests          wheel, dbt project)          tests on realistic data
  • validate config                                 • data tests
  • dbt build (slim)

  ADF deploys to test AND prod follow: stop triggers → deploy → clean up + restart triggers (Part D §4)
```

### 3. What gets tested

| Test type | What it checks | When | Example |
|---|---|---|---|
| **Static checks** | Style, syntax, config validity | CI on every PR | Linting Python/SQL, validating ADF or bundle config |
| **Unit tests** | One transformation's logic on tiny fake data | CI | Dedup keeps the latest row per `order_id` |
| **Integration tests** | Components together on real services | After deploy to test | Full bronze → gold run on a sample |
| **Data tests / expectations** | Properties of the **output data** | Every pipeline run, in all environments | `order_id` unique, `amount ≥ 0`, freshness within the SLA (e.g. < 26 h for the nightly load, Unit 13) |
| **Smoke tests** | "Is it alive?" after deploy | Right after prod deploy | Trigger a small run, check it succeeds |
| ➕ **Contract / schema tests** | Output schema matches what consumers expect | CI + runtime | dbt model contracts (Unit 8) |

- **Unit tests on Spark:** keep transformations as **pure functions** (DataFrame in, DataFrame out) so they can be tested without the whole pipeline. This links to the Unit 6 advice on modular code.
- **dbt Slim CI (Unit 8 recap):** in CI, build and test **only models changed in the PR and their downstream models** (`state:modified+`), comparing against prod's `manifest.json` (`--state`), and **defer** unchanged upstream models to prod (`--defer`).

### 4. Environment strategy on Azure

| | Dev | Test / UAT | Prod |
|---|---|---|---|
| **Who changes it** | Engineers, directly: the Git-connected ADF factory, personal bundle deploys. **Not** deployed by the CD pipeline. | Only the CD pipeline | Only the CD pipeline, after approval |
| **ADF** | `adf-ecom-dev` (**Git-connected**) | `adf-ecom-test` (not Git-connected) | `adf-ecom-prod` (not Git-connected) |
| **Databricks** | Dev workspace or `ecom_dev` catalog | `ecom_test` catalog | `ecom_prod` catalog |
| **Storage** | `stecomlakedev` | `stecomlaketest` | `stecomlakeprod` |
| **Data** | Masked / synthetic (Unit 15) | Masked copy or sample | Real |
| **Secrets** | `kv-ecom-dev` | `kv-ecom-test` | `kv-ecom-prod` |

- Ideally separate **resource groups or subscriptions** per environment, all created with **infrastructure as code** (Bicep/Terraform, Unit 9).
- **Same names, different values:** code refers to logical names (e.g. a `catalog` variable). Each environment supplies its own values.
- ⚠️ **No manual changes in test or prod.** A hand edit in prod is **drift**: the next deployment overwrites it, or it silently differs from Git.

### 5. ➕ Branching strategies

| Strategy | How it works | Fit for data teams |
|---|---|---|
| **Trunk-based / GitHub Flow** | Short-lived feature branches → PR → `main`. `main` is always deployable. | **Recommended.** Simple and matches ADF's collaboration branch model. |
| **GitFlow** | `develop`, `release/*`, `hotfix/*`, `main` | Heavier. Useful only with strict scheduled releases. |
| **Environment branches** (`dev`, `test`, `prod` branches) | Merge between branches to promote | ⚠️ Anti-pattern: breaks "build once, deploy many" and branches drift apart |

Review rules from Unit 1 still apply: branch protection on `main`, required reviewers, required CI checks.

### 6. ➕ Safe release patterns for data

- **Write-Audit-Publish (WAP):** write new data to a staging location, **audit** it with data tests, and only then **publish** (swap it in). Bad data never reaches consumers.
- **Blue/green tables or views:** build the new version beside the old, then switch a view to point at it. Rollback = switch back.
- **Backward-compatible schema changes:** add columns first, remove old ones in a later release once consumers have moved.
- **Rollback:** redeploy the previous artifact version. For data, use **Delta time travel** (Unit 6) to find the last good version, and **`RESTORE TABLE … TO VERSION AS OF`** (Unit 13) to roll the table back to it.

---

## Part C — CI/CD Tools: Azure DevOps and GitHub Actions

### 1. Side-by-side

| Concept | **Azure DevOps Pipelines** | **GitHub Actions** |
|---|---|---|
| Pipeline definition | `azure-pipelines.yml` (YAML). ➕ Classic UI release pipelines also exist. | `.github/workflows/*.yml` |
| What starts it | `trigger` (push), `pr` (pull request), schedules, manual | `on:` `push`, `pull_request`, `schedule`, `workflow_dispatch` (manual) |
| Structure | **Stages → Jobs → Steps** (steps are **tasks** or scripts) | **Workflow → Jobs → Steps** (steps are **actions** or scripts) |
| Reusable building blocks | Tasks from the Marketplace, **templates** | Actions from the Marketplace, **reusable workflows** |
| Where it runs | **Agents**: Microsoft-hosted or self-hosted | **Runners**: GitHub-hosted or self-hosted |
| Connecting to Azure | **Service connection** (Azure Resource Manager) | `azure/login` action with credentials or **OIDC** |
| Secrets & variables | **Variable groups** (can link to **Key Vault**), secret variables | **Secrets** and **variables** at repo, org or environment level |
| Approvals | **Environments** with **approvals and checks** | **Environments** with **protection rules** (required reviewers, wait timer, branch rules) |
| Build outputs | **Pipeline artifacts** (publish / download) | **Artifacts** (upload / download) |
| Other parts of the suite | Boards, Repos, Test Plans, Artifacts feeds | Issues, Projects, Packages |

**Which one?** Same concepts, same result. Choose based on where the code already lives and what the organization uses. ADF's Git integration supports **both** Azure Repos and GitHub.

### 2. Anatomy of a multi-stage pipeline

```
 Pipeline / Workflow
 ├─ Stage/Job: CI          → checkout, lint, unit tests, validate, build artifact, publish artifact
 ├─ Stage/Job: Deploy_Test (environment: test)  → download artifact, deploy with test config, integration + data tests
 └─ Stage/Job: Deploy_Prod (environment: prod)  → ⏸ approval required → deploy, smoke test
```

- Dev isn't a pipeline stage. It's the developers' own workspace (Part B §4).
- **Stages/jobs depend on each other** (`dependsOn` / `needs`), so prod only runs if test succeeded.
- **The approval is attached to the environment,** not the pipeline. Every pipeline deploying to `prod` hits the same gate.
- Deploy jobs in Azure DevOps are **deployment jobs** that target an environment, which gives a **deployment history** per environment.

A short GitHub Actions skeleton, just to see the shape:

```yaml
on: { push: { branches: [main] } }
jobs:
  build:       { runs-on: ubuntu-latest, steps: [ ... ] }
  deploy-prod:
    needs: build
    environment: prod          # protection rule → required reviewer
    runs-on: ubuntu-latest
    steps: [ ... ]
```

### 3. Authenticating the pipeline to Azure

| Option | How it works | Verdict |
|---|---|---|
| **Service principal + client secret** | Secret stored in the CI tool | Works, but the secret can **leak or expire** (the Unit 13 expired-credential failure again) |
| **Workload identity federation / OIDC** | The CI tool gets a **short-lived token**. Azure trusts it through a **federated credential** on the app registration or managed identity. | ✅ **Recommended.** No stored secret. |
| **Managed identity** | Only on **self-hosted** agents/runners running on Azure VMs | Good for private networks |

- Give the deployment identity **least privilege** (Unit 15): e.g. *Data Factory Contributor* on the prod factory's resource group, not *Owner* on the subscription.
- **One identity per environment**, so the test pipeline can't touch prod.
- Pipeline secrets come from **Key Vault** (variable group link in Azure DevOps, or read at runtime). Never in YAML.

---

## Part D — Automating ADF Deployment

### 1. The ADF CI/CD model

```
 Developer                     DEV factory (Git-connected)                        TEST / PROD factories (NOT Git-connected)
 ─────────                     ──────────────────────────                        ─────────────────────────────────────────
 feature branch in ADF Studio → "Debug" runs on the branch
          │ PR
          ▼
 collaboration branch (main) ──► BUILD: validate + export ARM template ──► DEPLOY: ARM template + environment parameters
                                  (npm package, automated publish)         + stop triggers before / start triggers after
```

**Key facts:**

- **Only the dev factory is connected to Git.** Test and prod are updated **only** by the CD pipeline.
- In ADF Studio you work in a **feature branch**, test with **Debug**, then raise a PR to the **collaboration branch** (usually `main`).
- Git stores each pipeline, dataset, linked service, trigger and data flow as a **JSON file**.
- What gets deployed is an **ARM template** of the whole factory:
    - `ARMTemplateForFactory.json`: all the resources
    - `ARMTemplateParametersForFactory.json`: values that differ per environment

### 2. Two ways to produce the ARM template

Unit 11 covered the basic flow: collaboration branch → **Publish** → ARM templates. Automated publish replaces the manual Publish click with a build step.

| | **Manual publish (legacy)** | **Automated publish (recommended)** |
|---|---|---|
| How | Someone clicks **Publish** in ADF Studio on the collaboration branch | The build pipeline uses the **`@microsoft/azure-data-factory-utilities`** npm package to **validate** the factory and **export** the ARM template from `main` |
| Where the template lands | The **`adf_publish`** branch | A **build artifact** |
| Problems | Depends on a person remembering to click. It also updates the dev factory's live version at the same moment. The build isn't validated or repeatable in CI. | None: every merge to `main` produces a validated, versioned artifact automatically |

⚠️ With automated publish, you usually **disable the Publish button** in the dev factory so nobody bypasses the pipeline.

### 3. Parameterization: one template, many environments

What must change between environments:

- Linked service connection details: storage URL, SQL server, Databricks workspace URL
- Key Vault base URL
- **Global parameters** (e.g. `env = dev/test/prod`)
- Trigger settings, e.g. only prod runs on a schedule

How ADF handles it:

- By default the ARM template exposes **some** properties as parameters (connection strings, URLs).
- A **custom parameterization template** (`arm-template-parameters-definition.json` in the repo) controls exactly **which properties** become parameters.
- The deploy stage **overrides** those parameters per environment.
- ✅ Best practice: linked services read secrets from **Key Vault**, and use the factory's **managed identity**. Then the only thing that changes per environment is the **vault URL / resource URL**, not any secret.

### 4. The deploy step

This runs for **every** target environment (test and prod):

1. **Pre-deployment:** **stop active triggers** in the target factory. ARM deployment fails, or behaves unpredictably, if it updates a running trigger.
2. **Deploy the ARM template** with that environment's parameter values (incremental mode).
3. **Post-deployment:** **delete resources** that were removed from Git, and **restart the triggers**.

Microsoft provides a **pre/post-deployment script** for steps 1 and 3. Deployment runs through the ARM template deployment task (Azure DevOps) or an Azure deploy action (GitHub).

### 5. Gotchas and best practices

- ⚠️ **ARM limits:** a single template allows **256 parameters** and a size limit. Large factories use **linked templates**, which the export also produces.
- ⚠️ **Integration runtimes:** a self-hosted IR name must be the **same** in every environment. Each environment has its own IR machine registered under that name. ➕ Or share one IR across factories with a **linked (shared) IR**.
- **Hotfixes:** branch from the commit that's in prod, fix, deploy through the pipeline, then **merge back** to `main`. Never edit the prod factory directly.
- ➕ **Deploy only changed pipelines?** ADF deploys the **whole factory**. Tools exist for selective deployment, but Microsoft's standard model is full-factory deployment.
- ➕ **Synapse pipelines** follow the same model, using the **Synapse workspace deployment** task/action instead of plain ARM.

---

## Part E — Automating Databricks Deployment

### 1. What counts as a "Databricks asset"

Notebooks, Python/SQL files and **wheels** · **Lakeflow Jobs** (Unit 11) · **Lakeflow Declarative Pipelines** (formerly DLT, Unit 11) · Clusters and cluster policies · SQL queries and dashboards · ML models · **Unity Catalog objects** (catalogs, schemas, grants)

### 2. Deployment options

| Option | What it is | Status |
|---|---|---|
| **Databricks Asset Bundles (DABs)** | Define code **plus** jobs/pipelines/config as a project in Git, deploy per target with the Databricks CLI | ✅ **Recommended** today |
| **Git folders** (formerly **Repos**) | A Git clone inside the workspace | ✅ For **development**. In prod, a job can point to a Git reference, but bundles are preferred. |
| **Terraform (Databricks provider)** | IaC for workspaces, clusters, Unity Catalog, permissions | ✅ For **platform/infrastructure**. Often used alongside bundles. |
| REST API / custom scripts | Call the Jobs and Workspace APIs directly | Legacy, a lot of glue code |
| ➕ `dbx` | Older Databricks Labs deployment tool | ❌ Deprecated, replaced by bundles |

**Rule of thumb:** **Terraform** for the platform (workspace, metastore, catalogs, grants). **Bundles** for the data product (jobs, pipelines, code).

### 3. Databricks Asset Bundles

A bundle is a folder with a **`databricks.yml`** file at the root:

| Section | Purpose |
|---|---|
| `bundle` | The bundle's name |
| `resources` | The **jobs, pipelines, dashboards**, etc. to create, defined as YAML |
| `variables` | Values that change per environment (e.g. `catalog`) |
| `targets` | **dev / test / prod**: the workspace host, variable values, mode and permissions for each |

```yaml
bundle: { name: ecom_pipelines }
variables: { catalog: { default: ecom_dev } }
workspace: { host: https://adb-dev.azuredatabricks.net }   # default (dev) workspace
targets:
  dev:  { mode: development, default: true }
  test:
    workspace: { host: https://adb-test.azuredatabricks.net }   # its own workspace, not dev's
    variables: { catalog: ecom_test }
  prod:
    mode: production
    workspace: { host: https://adb-prod.azuredatabricks.net }
    variables: { catalog: ecom_prod }
    run_as: { service_principal_name: sp-ecom-deploy }
```

**Lifecycle:** **validate** → **deploy** (uploads files and creates/updates the jobs) → **run**. In CI/CD, the pipeline calls these through the Databricks CLI, authenticated as a **service principal**.

**`mode: development` vs `mode: production`:**

| `development` | `production` |
|---|---|
| Resource names get a **`[dev <username>]` prefix**, so each developer gets an isolated copy | Real names |
| **Schedules and triggers paused** | Schedules active |
| No branch or `run_as` checks | Checks that you deploy from the right **Git branch**. Should run as a **service principal** (`run_as`), not a person. |

- **Same bundle, different target** = "build once, deploy many" for Databricks.
- Code reads the catalog from a **variable/parameter**, never a hard-coded `ecom_prod`. The Part B §4 environment table depends on this.
- ➕ **Bundle templates** create a standard project layout, so every team starts the same way.

### 4. A Databricks CI/CD flow

| Stage | What happens |
|---|---|
| **Development** | Each developer deploys to the **dev** target (development mode, prefixed names, paused schedules) |
| **PR (CI)** | Lint, **unit tests** on transformation functions (locally or with ➕ **Databricks Connect**), **validate** the bundle |
| **Merge → test** | Deploy the bundle to the **test** target, **run** the job on sample data, check data tests |
| **Approval → prod** | Deploy to the **prod** target as the service principal. Schedules turn on. |
| **After deploy** | Smoke run; monitoring and alerts from Unit 13 take over |

⚠️ **Humans shouldn't own prod jobs.** If a job runs as a person, it breaks when that person leaves or loses access. Use a **service principal** with Unity Catalog grants (Unit 15).

### 5. ➕ How ADF and Databricks deploy together

In the e-commerce platform, **ADF orchestrates** and **Databricks transforms** (Unit 11):

- **One repo or two?** Either works. Many teams use one repo with folders for `adf/`, `databricks/` (bundle), `dbt/` and `infra/` (Bicep/Terraform), so one PR can change everything related to a feature.
- **Order matters:** deploy **infrastructure → Databricks bundle → ADF**. ADF's Databricks activity must point at a job or notebook that already exists in that environment.
- ADF's Databricks linked service uses the **workspace URL parameter** and **managed identity** per environment (Part D §3).
- ➕ An ADF pipeline can trigger a **Databricks job** by ID or name instead of a notebook path, so bundles control the job definition.

---

## Part F — ➕ Added: Putting It Together (E-commerce Platform)

| Change | CI checks | CD path | Gate before prod |
|---|---|---|---|
| New column in `silver.orders` (PySpark) | Unit test, bundle validate | Bundle → test → run → data tests | Approval |
| New ADF copy pipeline for a vendor feed | ADF validate + ARM export | ARM deploy to test (triggers stopped/restarted) → trigger run | Approval, triggers stopped/restarted |
| New dbt gold model (Unit 8; dbt on Databricks, Unit 11) | Slim CI: `dbt build` on modified models | dbt job in the `ecom_test` catalog | Approval |
| New private endpoint (Unit 10) | IaC validate / what-if plan | IaC apply to test | Approval |
| Grant for a new analyst group (Unit 15) | IaC plan review | IaC apply | Approval + data owner sign-off |

---

## Key Terms Cheat Sheet

- **DataOps:** Agile + DevOps + Lean applied to data. Tests **code and data**. ➕ DataOps Manifesto (18 principles).
- **Innovation loop vs production loop**
- ➕ **DORA metrics:** deployment frequency, lead time, change failure rate, time to restore; + data downtime
- **CI vs Continuous Delivery (manual prod gate) vs Continuous Deployment (no gate)**
- **Artifact; build once, deploy many; promotion; environment; drift**
- **Dev is the developer workspace, not a pipeline stage.** The pipeline promotes test → prod.
- **Test types:** static, unit, integration, data tests, smoke; ➕ contract tests; **dbt Slim CI** (Unit 8: `state:modified+`, `--state`, `--defer`)
- ➕ **Trunk-based** (recommended) vs GitFlow vs environment branches (anti-pattern)
- ➕ **Write-Audit-Publish, blue/green, rollback** (redeploy previous artifact; Delta time travel + `RESTORE`, Unit 13)
- **Azure DevOps:** stages → jobs → steps/tasks, agents, service connections, variable groups (Key Vault), environments + approvals and checks
- **GitHub Actions:** workflows → jobs → steps/actions, runners, `on:` events, secrets, environments + protection rules, `needs`
- **Workload identity federation / OIDC:** no stored secret. One least-privilege identity per environment.
- **ADF CI/CD:** only **dev** is Git-connected; feature branch → collaboration branch; **ARM template** + parameters file; `adf_publish` (manual Publish click, legacy) vs **npm utilities package (automated)**; custom parameterization template; global parameters; **stop triggers → deploy → cleanup + restart triggers in every target environment**; 256-parameter limit → linked templates; same IR name across environments; hotfix flow
- **Databricks assets:** notebooks, code, **Lakeflow Jobs**, **Lakeflow Declarative Pipelines** (formerly DLT), clusters, dashboards, Unity Catalog objects
- **Databricks Asset Bundles:** `databricks.yml` (bundle, resources, variables, **targets** dev/test/prod); validate → deploy → run; **development mode** (`[dev <username>]` prefix, paused schedules) vs **production mode** (branch check, `run_as` service principal)
- **Git folders** (formerly Repos) for development; **Terraform** for platform; ➕ `dbx` deprecated
- **Deploy order:** infrastructure → Databricks → ADF

---

## Practice Questions

1. **What is DataOps, and how is it different from DevOps?**

    DataOps applies Agile, DevOps and Lean ideas to data work. Unlike DevOps, it has to manage data as well as code, and data changes without code changes, so it adds data tests on every pipeline run.

2. **Name five DataOps principles.**

    Everything as code, automate everything repeatable, test code and data, environment isolation, small frequent releases (also observability and reproducibility).

3. **What are the innovation loop and the production loop?**

    Innovation: how fast and safely you change the pipeline (CI/CD). Production: how reliably the running pipeline delivers good data (monitoring, data tests).

4. **Continuous Delivery vs Continuous Deployment?**

    Delivery: every change is ready to deploy, and prod needs a manual approval. Deployment: every validated change goes to prod automatically.

5. **Why do most data teams choose Continuous Delivery?**

    Bad data changes are expensive to undo (backfills, reprocessing, wrong reports already used), so a human gate before prod is worth the delay.

6. **What does "build once, deploy many" mean, and why does it matter?**

    The same artifact built in CI is promoted to every environment, and only configuration changes. Rebuilding per environment risks deploying something that was never tested.

7. **Unit test vs data test?**

    A unit test checks transformation logic on tiny fake data at CI time. A data test checks properties of the real output (uniqueness, not null, freshness) on every pipeline run.

8. **How do you make PySpark transformations unit-testable?**

    Write them as pure functions (DataFrame in, DataFrame out), separate from reading and writing, so they can be tested on small in-memory DataFrames.

9. **What is dbt Slim CI?**

    In CI, build and test only the models changed in the PR and their downstream models (`state:modified+`, compared with prod's `manifest.json`), deferring unchanged upstream models to prod (`--defer`). Faster and cheaper.

10. **Why are environment branches (`dev`, `test`, `prod`) an anti-pattern?**

    Each environment ends up built from different code, the branches drift apart, and it breaks build once, deploy many.

11. **What is drift, and how do you prevent it?**

    Differences between what's deployed and what's in Git, usually from manual changes. Prevent it by allowing changes to test and prod only through the CD pipeline.

12. **Map these Azure DevOps terms to GitHub Actions: stage/job, task, agent, service connection, variable group.**

    Job, action, runner, `azure/login` with credentials or OIDC, secrets/variables.

13. **Where do you put the approval for production deployments?**

    On the **environment** (Azure DevOps approvals and checks / GitHub environment protection rules), so every pipeline deploying there hits the same gate.

14. **Why is workload identity federation (OIDC) preferred over a service principal secret?**

    No secret is stored, so nothing can leak or expire. The CI tool gets a short-lived token that Azure trusts through a federated credential.

15. **Which ADF factories are connected to Git?**

    Only the dev factory. Test and prod are updated only by the CD pipeline.

16. **What does ADF deploy between environments?**

    An ARM template of the whole factory (`ARMTemplateForFactory.json`) plus a parameters file with values that differ per environment.

17. **Manual publish vs automated publish in ADF?**

    Manual: someone clicks Publish on the collaboration branch, and the ARM template goes to the `adf_publish` branch. That depends on a person and isn't validated in CI. Automated: the build pipeline uses the ADF utilities npm package to validate and export the ARM template from `main` on every merge. Automated is recommended.

18. **How do you control which ADF properties become ARM parameters?**

    With a custom parameterization template (`arm-template-parameters-definition.json`) in the repo.

19. **Why stop triggers before deploying ADF, and what happens after?**

    Updating active triggers fails or behaves unpredictably. After deploying, removed resources are cleaned up and triggers are restarted. This happens for every target environment, test and prod.

20. **What's the best way to handle connection secrets in ADF across environments?**

    Linked services use Key Vault and managed identity, so the only per-environment parameter is the vault or resource URL.

21. **Your ADF ARM deployment fails because the template has too many parameters. What's the fix?**

    Use the linked templates generated by the export (the limit is 256 parameters per template), or trim the parameterization template.

22. **How do you hotfix a production ADF pipeline?**

    Branch from the commit deployed to prod, fix it, deploy through the pipeline, then merge the fix back into `main`. Never edit prod directly.

23. **What is a Databricks Asset Bundle?**

    A project in Git with a `databricks.yml` that defines code plus jobs, pipelines and other resources, and per-environment targets. It's deployed with the Databricks CLI.

24. **Name the main sections of `databricks.yml`.**

    `bundle` (name), `resources` (jobs, pipelines), `variables`, and `targets` (dev/test/prod settings).

25. **Development mode vs production mode in a bundle?**

    Development prefixes names with `[dev <username>]` and pauses schedules, so developers are isolated. Production uses real names and active schedules, checks the Git branch, and should run as a service principal.

26. **Why should prod Databricks jobs run as a service principal?**

    A job tied to a person breaks when they leave or lose access, and it gives the job that person's (usually broader) permissions.

27. **Bundles vs Terraform for Databricks: which does what?**

    Terraform manages the platform (workspaces, metastore, catalogs, grants). Bundles manage the data product (jobs, pipelines, code).

28. **In what order do you deploy infrastructure, Databricks and ADF, and why?**

    Infrastructure → Databricks → ADF. ADF activities reference Databricks jobs and resources that must already exist in that environment.

29. **How do you avoid hard-coding `ecom_prod` in Databricks code?**

    Pass the catalog as a bundle variable / job parameter that each target sets differently.

30. ➕ **What is Write-Audit-Publish?**

    Write new data to a staging location, run data tests on it, and publish only if they pass, so bad data never reaches consumers.

31. ➕ **A prod deploy broke the gold tables. How do you roll back?**

    Redeploy the previous artifact version through the pipeline. Use Delta time travel to find the last good version of the affected tables, then `RESTORE` them to it.

32. ➕ **Describe CI/CD for the e-commerce platform in one paragraph.**

    One repo with `infra/`, `databricks/` (bundle), `adf/` and `dbt/`. Developers work in the Git-connected dev factory and personal dev bundle targets. PRs to `main` run lint, unit tests, bundle validation, ADF validation and dbt Slim CI. Merging builds the artifacts once (ARM template, bundle) and deploys to test with OIDC-authenticated, least-privilege identities, then runs integration and data tests. Prod requires approval on the environment. It deploys infra → bundle (as a service principal) → ADF (stop triggers, deploy, restart), then runs a smoke test, and Unit 13 monitoring takes over.
