# Unit 07 — Distributed Data Processing with Spark & PySpark: Study Guide

**Scope:** Why we need distributed computing · Hadoop MapReduce and the Spark architecture · Resilient Distributed Datasets (RDDs) · The DataFrame API (transformations and actions) · Spark SQL and the Catalyst Optimizer · Performance tuning and optimization

The examples reuse the e-commerce data from Units 3, 5 and 6 (`orders`, `order_item`, `products`, `customers`). Unit 6 ended at "when data outgrows Pandas, move to Spark / Fabric". This unit starts there.

---

## Part A — Why Distributed Computing, and How Spark Is Built

### 1. The core problem

Big data doesn't fit in the **memory (RAM)** or run fast enough on the **CPU of a single machine**. You can buy a bigger machine (**scale up / vertical scaling**), but that has a limit and gets expensive fast. The other option is to split the data across many machines and **process the pieces in parallel** (**scale out / horizontal scaling**).

**Apache Spark** is a **unified analytics engine**. It manages hundreds of machines working in parallel, but lets you write code **as if all the data were on one machine**. "Unified" means one engine handles batch jobs, SQL, streaming (Structured Streaming) and machine learning (MLlib).

| | Pandas (Unit 6) | Spark |
|---|---|---|
| Runs on | One machine | A cluster of many machines (or local mode on your laptop) |
| Data limit | About the RAM of that machine | Spread across the whole cluster's RAM and disk |
| Execution | **Eager**: each line runs right away | **Lazy**: builds a plan, runs it when an action is called |
| Data | Mutable, has a row index | **Immutable**, no row index |

### 2. Before Spark: Hadoop MapReduce

MapReduce splits a job into three phases. The standard example is **word count**:

```
Input: "cat dog cat elephant dog cat tiger lion tiger cat"

Input splits (one per node)
  Split 1: cat dog cat elephant
  Split 2: dog cat tiger lion tiger cat

MAP (each node, on its own split): emit (word, 1)
  Node 1: (cat,1) (dog,1) (cat,1) (elephant,1)
  Node 2: (dog,1) (cat,1) (tiger,1) (lion,1) (tiger,1) (cat,1)

COMBINER (optional, local pre-aggregation on each node)
  Node 1: (cat,2) (dog,1) (elephant,1)
  Node 2: (dog,1) (cat,2) (tiger,2) (lion,1)

SHUFFLE & SORT (move over the network so each key ends up in one place)
  cat → [2, 2]   dog → [1, 1]   elephant → [1]   lion → [1]   tiger → [2]

REDUCE (combine the values of each key)
  (cat,4) (dog,2) (elephant,1) (lion,1) (tiger,2)
```

| Phase | What it does |
|---|---|
| **Map** | Each node **filters and transforms** the data in its own partition. No data moves between nodes. |
| **Combiner** | A "mini reduce" on each node before the shuffle. It sends `(cat,2)` once instead of `(cat,1)` twice, so **less data crosses the network**. |
| **Shuffle & Sort** | **Groups data by key**: every record with the same key is sent over the network to the same reducer. This is the expensive step. |
| **Reduce** | Aggregates each key's group into the final result. There can be many reducers, each handling a different set of keys, and they write the output files. |

> Reduce doesn't collect everything onto one single node. Shuffle makes sure all the values for one key land on the same reducer. With many keys there are usually many reducers working in parallel.

**The weakness of MapReduce:** it **writes intermediate data to disk** after every phase (map output goes to local disk, and each job's output goes to HDFS). A pipeline of 10 steps, or an ML algorithm that loops 50 times, reads and writes the disk again and again. Disk is far slower than RAM. MapReduce code is also verbose: you write Java map and reduce classes by hand.

### 3. What Spark changed

| | Hadoop MapReduce | Spark |
|---|---|---|
| Intermediate data | Written to **disk** between every step | Kept **in memory (RAM)** when possible, spills to disk only when needed |
| Multi-step jobs | Each step is a separate MapReduce job | One job becomes a **DAG** of stages |
| Optimization | None: runs exactly what you wrote | **Optimized engine** (Catalyst) rewrites your plan automatically |
| APIs | Low-level map/reduce in Java | High-level: Python, SQL, Scala, Java, R; DataFrames |
| Iterative / interactive work | Slow | Fast (often 10–100× for in-memory workloads) |

Spark still shuffles, and a shuffle still writes files to local disk. Spark avoids disk *between narrow steps*, and it avoids saving results to HDFS between steps.

### 4. Spark architecture

```
                 ┌─────────────────────────┐
                 │     DRIVER PROGRAM      │   "the boss"
                 │  SparkSession /         │   - runs your main code
                 │  SparkContext           │   - builds + optimizes the plan (DAG)
                 │  DAG + Task scheduler   │   - splits work into tasks, tracks them
                 └───────────┬─────────────┘
                             │ asks for resources
                 ┌───────────▼─────────────┐
                 │     CLUSTER MANAGER     │   Standalone, YARN, Kubernetes
                 │  allocates CPU + RAM    │   (Mesos is deprecated)
                 └───────────┬─────────────┘
              ┌──────────────┼──────────────┐
      ┌───────▼──────┐ ┌─────▼────────┐ ┌───▼──────────┐
      │ WORKER NODE  │ │ WORKER NODE  │ │ WORKER NODE  │
      │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
      │ │ Executor │ │ │ │ Executor │ │ │ │ Executor │ │   "the workers"
      │ │ task task│ │ │ │ task task│ │ │ │ task task│ │   - run tasks
      │ │ cache    │ │ │ │ cache    │ │ │ │ cache    │ │   - store cached data
      │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
      └──────────────┘ └──────────────┘ └──────────────┘
```

| Component | Role |
|---|---|
| **Driver program** | Holds the **SparkSession** (entry point for DataFrames and SQL) and the **SparkContext** (lower-level entry point for RDDs, available as `spark.sparkContext`). Turns your code into a plan, splits it into tasks, sends tasks to executors, and collects results. |
| **Cluster manager** | **Allocates resources**: decides which machines get executors and how much CPU/RAM each has. |
| **Worker node** | A machine in the cluster. It hosts one or more executors. |
| **Executor** | A JVM process on a worker. It **runs tasks, does the calculations, and stores data** in memory or disk (cache, shuffle files). |

> The cluster manager hands out *resources* (executors). Splitting the *work* into tasks and assigning tasks to executors is done by the driver's schedulers.

**Local mode** (`.master("local[*]")`) runs the driver and executors in one process on your laptop, using all cores. It's good for learning and tests. In **Microsoft Fabric** or Databricks notebooks, `spark` is already created for you and the platform manages the cluster.

### 5. Spark workflow

1. **You submit code** (notebook, `spark-submit`, a scheduled job).
2. **The driver builds a plan.** It's optimized, not run as written. Example: you write *join, then filter*. Spark runs *filter, then join*, because filtering first means fewer rows go into the expensive join (**predicate pushdown**).
3. **The cluster manager allocates resources** (executors).
4. **Executors run tasks.** Spark breaks big data into small pieces called **partitions**. **Each partition is processed by one task**, and each task runs on one executor core.
5. **Results are returned** to the driver (e.g., `count()`) or written to storage (e.g., `write.parquet()`).

### 6. Execution hierarchy

From high to low:

| Level | Created when | Divided by |
|---|---|---|
| **Application** | You create a SparkSession. One app can submit many jobs. | Each **action** creates a job |
| **Job** | An **action** is called (`count()`, `show()`, `write`) | **Shuffle boundaries**: only narrow transformations → 1 stage; every shuffle adds a new stage |
| **Stage** | A set of tasks that can run without moving data between executors | **Partitions**: number of tasks = number of partitions |
| **Task** | The **smallest unit of work**: one partition, one core | — |

```python
df = spark.read.parquet("silver/orders")          # 8 files → ~8 input partitions
paid = df.filter(df.status == "paid")             # narrow
by_user = paid.groupBy("user_id").sum("amount")   # wide → shuffle
by_user.write.parquet("gold/user_revenue")        # ACTION → 1 job
```

```
Job 0
├── Stage 0: read → filter → partial sum per partition     8 tasks (1 per input partition)
│        ── shuffle (data moves by user_id) ──
└── Stage 1: final sum per user_id → write                 200 tasks (spark.sql.shuffle.partitions,
                                                             or fewer if AQE coalesces them)
```

**How many tasks run at the same time?** `number of executors × cores per executor`. With 5 executors × 2 cores = 10 slots, 200 tasks run in about 20 "waves".

### 7. Spark configuration

From the lecture code (comments translated from Vietnamese):

```python
from pyspark.sql import SparkSession

# Start the Driver (the Boss) and request resources from the Cluster Manager
spark = (SparkSession.builder
         .appName("Medallion_Pipeline")
         .config("spark.driver.memory", "2g")      # RAM for the Boss (driver)
         .config("spark.executor.memory", "4g")    # RAM for each Worker to hold in-memory data
         .config("spark.executor.cores", "2")      # number of tasks running in parallel on one executor
         .getOrCreate())                           # reuse an existing session or create a new one
```

Remember `.getOrCreate()` at the end. The builder does nothing without it.

| Config | Meaning | Tuning notes |
|---|---|---|
| `spark.driver.memory` | Heap memory for the driver | Increase it if you `collect()` / `toPandas()` large results, or broadcast big tables |
| `spark.executor.memory` | Heap memory **per executor** | Split into **execution** memory (shuffles, joins, sorts) and **storage** memory (cache), which share one pool (`spark.memory.fraction`, 0.6 by default) |
| `spark.executor.cores` | Cores per executor = **parallel tasks per executor** | 2–5 is typical. Too many cores share the same RAM, so each task gets less. |
| `spark.executor.instances` | Number of executors (static allocation) | Or let Spark scale with `spark.dynamicAllocation.enabled` |
| `spark.executor.memoryOverhead` | Off-heap memory per executor (Python workers, network buffers) | Increase it for heavy Python UDF use |
| `spark.sql.shuffle.partitions` | Partitions after a shuffle (default **200**) | See Part E |
| `spark.sql.adaptive.enabled` | Adaptive Query Execution (on by default since Spark 3.2) | See Part E |

Cluster-level settings (driver/executor memory and cores) must be set **before the session starts**. SQL settings can be changed at runtime: `spark.conf.set("spark.sql.shuffle.partitions", "64")`.

---

## Part B — Resilient Distributed Datasets (RDDs)

### 1. What an RDD is

An **RDD** is Spark's original, low-level data structure: an **immutable collection of objects, split into partitions across the cluster**, with **no schema**. Spark doesn't know what's inside each element (a tuple, a dict, a line of text).

| Word | Meaning |
|---|---|
| **Resilient** | If an executor dies and a partition is lost, Spark **recomputes it from its lineage** (the chain of transformations that built it). No need to replicate the data. |
| **Distributed** | Split into partitions stored on different executors |
| **Dataset** | A collection of records |

Other properties: **immutable** (every transformation creates a new RDD) and **lazy** (nothing runs until an action).

### 2. Working with RDDs

```python
sc = spark.sparkContext

nums  = sc.parallelize([1, 2, 3, 4, 5, 6], numSlices=3)   # from a Python list, 3 partitions
lines = sc.textFile("data/animals.txt")                   # from a file, one element per line

nums.getNumPartitions()                                   # 3
```

**Word count with RDDs**, the same algorithm as the MapReduce diagram in a few lines:

```python
counts = (sc.textFile("data/animals.txt")
            .flatMap(lambda line: line.split())       # MAP: lines → words
            .map(lambda w: (w, 1))                    # MAP: word → (word, 1)
            .reduceByKey(lambda a, b: a + b))         # COMBINER + SHUFFLE + REDUCE

counts.collect()   # ACTION → [('cat', 4), ('dog', 2), ('elephant', 1), ('lion', 1), ('tiger', 2)]
```

| RDD transformations (lazy) | RDD actions (run the job) |
|---|---|
| `map`, `flatMap`, `filter`, `distinct` | `collect`, `count`, `first`, `take(n)` |
| `reduceByKey`, `groupByKey`, `sortByKey` | `reduce`, `foreach` |
| `join`, `union`, `mapPartitions` | `saveAsTextFile` |

**`reduceByKey` vs `groupByKey`:** `reduceByKey` sums inside each partition *before* the shuffle, exactly like the **combiner**, so it sends `(cat,2)` instead of two `(cat,1)`. `groupByKey` ships every single value across the network and then groups. Prefer `reduceByKey` (or `aggregateByKey`).

### 3. RDD vs DataFrame

| | **RDD** | **DataFrame** |
|---|---|---|
| Structure | Distributed **objects**, **schema-less** | **Rows and columns** with a **schema** (names + types), like a SQL table |
| Built on | — | **Built on top of RDDs** |
| Optimization | None: Spark runs your lambdas as written | **Catalyst Optimizer** + Tungsten (Part D) |
| Python speed | Slow: every lambda runs in a Python process, data is serialized back and forth | Fast: operations run inside the JVM, Python only sends the plan |
| Memory | Java/Python objects | Compact binary format (Tungsten) |
| API | Functional (`map`, `reduceByKey`) | Declarative (`select`, `groupBy`) + SQL |
| When to use | Unstructured data, very custom low-level logic, legacy code | **Almost always** in modern data engineering |

(Scala and Java also have a typed **Dataset** API. In Python, you only use DataFrames.)

---

## Part C — The DataFrame API

### 1. Creating and reading DataFrames

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

orders_schema = StructType([
    StructField("order_id",   IntegerType(),   nullable=False),
    StructField("user_id",    IntegerType(),   True),
    StructField("status",     StringType(),    True),
    StructField("amount",     DoubleType(),    True),
    StructField("order_date", TimestampType(), True),
])

orders = spark.read.csv("bronze/orders/*.csv", header=True, schema=orders_schema)
orders = spark.read.json("bronze/orders.jsonl")
orders = spark.read.parquet("silver/orders")
orders = spark.read.format("delta").load("lake/silver/orders")   # or spark.read.table("silver_orders")

small = spark.createDataFrame([(1, 7, "paid", 50.0)], "order_id int, user_id int, status string, amount double")
pdf_to_spark = spark.createDataFrame(pandas_df)

orders.printSchema(); orders.show(5); orders.columns
```

**Give an explicit schema for CSV and JSON.** `inferSchema=True` makes Spark read the data an extra time just to guess types, and it can guess wrong (same problem as CSV in Unit 6). Parquet and Delta already carry their schema.

```python
(orders.withColumn("order_day", F.to_date("order_date"))   # timestamp → date: one folder per day, not per timestamp
       .write
       .mode("overwrite")              # append | overwrite | ignore | error (default)
       .partitionBy("order_day")       # one folder per value
       .parquet("silver/orders"))

orders.write.format("delta").mode("append").saveAsTable("silver_orders")
```

### 2. Transformations vs actions

Every Spark statement is one of these two:

| | **Transformation** | **Action** |
|---|---|---|
| Runs | **Not immediately**: it adds a step to the plan and returns a new DataFrame | **Immediately**: triggers every transformation it depends on |
| Returns | A new (lazy) DataFrame | A value to the driver, or data written to storage |
| Examples | `select()`, `filter()`/`where()`, `withColumn()`, `groupBy().agg()`, `join()`, `orderBy()`, `distinct()`, `drop()`, `union()` | `count()`, `show()`, `collect()`, `take(n)`, `first()`, `toPandas()`, `write.parquet()`, `write.saveAsTable()` |

### 3. Lazy evaluation

Spark **doesn't run any transformation until an action is called**.

- Running each transformation right away would create a full intermediate result after every line and **use a lot of RAM** (the same problem as piled-up globals in Unit 6).
- Instead, Spark records the steps as a **DAG (directed acyclic graph)**: a graph of operations with no loops.
- When an action is called, Spark looks at **all the related transformations together** and builds **one optimized plan**. It can reorder steps, drop unused columns, and merge steps.

```python
df = spark.read.parquet("silver/orders")                     # nothing read yet (only metadata)
paid = df.filter(F.col("status") == "paid")                  # nothing runs
slim = paid.select("user_id", "amount")                      # nothing runs
slim.count()                                                 # NOW the whole chain runs
slim.show()                                                  # runs AGAIN from the source (unless cached)
```

Each action recomputes the chain from the source. If you reuse a DataFrame in several actions, **cache** it (Part E).

### 4. Narrow vs wide transformations

| | **Narrow** | **Wide** |
|---|---|---|
| Partitions | **1 input partition → 1 output partition** | **Many input partitions → 1 output partition** |
| Data movement | **None**: stays on the same executor | **Shuffle**: data moves between nodes over the network |
| Speed | **Very fast**, steps get chained in one stage (pipelining) | **Slow**, uses network, disk and memory; creates a new stage |
| Examples | `filter()`, `select()`, `withColumn()`, `map()`, `union()`, `coalesce()` | `groupBy()`, `join()` (except broadcast joins), `distinct()`, `orderBy()`, `repartition()`, window functions with `partitionBy` |

**Rule of thumb:** shuffles are the main cost in most Spark jobs. Reduce the data (filter, select) *before* the wide steps, and shuffle as few times as possible.

### 5. Common DataFrame operations

```python
# Select, rename, compute
df = orders.select("order_id", "user_id", F.col("amount").alias("amount_usd"))
df = df.withColumn("amount_vnd", F.col("amount_usd") * 25_000)
df = df.withColumnRenamed("amount_usd", "amount")
df = df.withColumns({"order_month": F.date_format("order_date", "yyyy-MM"),
                     "order_day":   F.to_date("order_date")})

# Filter (same & | ~ rules as Pandas: wrap each condition in parentheses)
paid = orders.filter((F.col("status") == "paid") & (F.col("amount") > 30))
paid = orders.where("status = 'paid' AND amount > 30")          # SQL string also works
orders.filter(F.col("status").isin("paid", "shipped"))

# Conditional logic (like np.select / CASE WHEN)
orders = orders.withColumn("tier",
    F.when(F.col("amount") >= 100, "gold")
     .when(F.col("amount") >= 30, "silver")
     .otherwise("bronze"))

# Nulls and cleaning
orders.na.drop(subset=["order_id", "amount"])
orders.na.fill({"status": "unknown"})
orders.withColumn("status", F.lower(F.trim("status")))
orders.withColumn("amount", F.col("amount").cast("double"))    # bad values → null
orders.dropDuplicates(["order_id"])

# Group and aggregate
summary = (orders.groupBy("user_id")
                 .agg(F.count("order_id").alias("order_count"),
                      F.sum("amount").alias("revenue"),
                      F.avg("amount").alias("avg_order"),
                      F.countDistinct("order_date").alias("active_days")))

# Joins
enriched = order_item.join(products, on="product_id", how="left")
orders.join(customers, orders.user_id == customers.customer_id, "inner")
orders.join(customers, orders.user_id == customers.customer_id, "left_anti")   # orders with NO matching customer
orders.join(customers, orders.user_id == customers.customer_id, "left_semi")   # orders that DO have a customer (no customer columns)

# Window functions (like Unit 5 SQL / Unit 6 groupby().shift())
from pyspark.sql.window import Window
w = Window.partitionBy("user_id").orderBy("order_date")
orders = (orders.withColumn("order_seq",   F.row_number().over(w))
                .withColumn("prev_amount", F.lag("amount").over(w))
                .withColumn("running_tot", F.sum("amount").over(w)))

# Keep the latest row per key
w_latest = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
latest = (customers.withColumn("rn", F.row_number().over(w_latest))
                   .filter("rn = 1").drop("rn"))

# Nested data (Silver flattening)
items = raw.select("order_id", F.explode("items").alias("item")) \
           .select("order_id", "item.sku", "item.qty")

# Sort, limit
orders.orderBy(F.col("amount").desc()).limit(10)
```

**Word count with DataFrames:**

```python
(spark.read.text("data/animals.txt")                                # one column: value
      .select(F.explode(F.split("value", r"\s+")).alias("word"))
      .groupBy("word").count()
      .orderBy(F.desc("count"))
      .show())
```

### 6. PySpark vs Pandas side by side

| Task | Pandas (Unit 6) | PySpark |
|---|---|---|
| New column | `df["x"] = df.a * 2` | `df = df.withColumn("x", F.col("a") * 2)` |
| Filter | `df[df.a > 5]` | `df.filter(F.col("a") > 5)` |
| If/else | `np.where` / `np.select` | `F.when(...).otherwise(...)` |
| Group | `df.groupby("k").agg(...)` | `df.groupBy("k").agg(...)` |
| Join | `merge(how="left")` | `join(..., how="left")` |
| Anti join | `indicator=True` + `left_only` | `how="left_anti"` |
| Window | `groupby().shift()`, `cumsum()` | `F.lag().over(w)`, `F.sum().over(w)` |
| Look at data | `df.head()` (already computed) | `df.show()` (**runs a job**) |

**Things that don't carry over from Pandas:** there's no row index, so no `.loc`/`.iloc`. DataFrames are **immutable**, so always reassign (`df = df.withColumn(...)`). Row order isn't guaranteed unless you `orderBy`.

**Moving between the two:** `spark_df.toPandas()` pulls **everything into the driver's memory**. Filter and aggregate first. Enable Arrow to speed it up: `spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")`.

---

## Part D — Spark SQL for Structured Data

### 1. Running SQL

Spark SQL lets you query DataFrames and tables with SQL. **SQL and the DataFrame API compile to the same plan**, so neither is faster.

```python
orders.createOrReplaceTempView("orders")        # session-scoped name for a DataFrame
customers.createOrReplaceTempView("customers")

top_cities = spark.sql("""
    SELECT c.city, SUM(o.amount) AS revenue, COUNT(*) AS orders
    FROM orders o
    JOIN customers c ON o.user_id = c.customer_id
    WHERE o.status = 'paid'
    GROUP BY c.city
    ORDER BY revenue DESC
""")                                            # returns a DataFrame (still lazy)

top_cities.filter("revenue > 1000").show()      # mix SQL and the DataFrame API freely
```

| Object | Lives | Created with |
|---|---|---|
| **Temp view** | Only in this SparkSession | `createOrReplaceTempView` |
| **Global temp view** | Across sessions in the same app (`global_temp.name`) | `createOrReplaceGlobalTempView` |
| **Table (managed)** | In the metastore/catalog. Spark owns the files: `DROP TABLE` deletes the data. | `saveAsTable`, `CREATE TABLE` |
| **Table (external)** | In the catalog, but files live at a path you control: `DROP TABLE` keeps the files | `CREATE TABLE ... LOCATION '...'` |

In Fabric lakehouses and Databricks, tables are **Delta** by default, so Spark SQL supports `MERGE INTO`, `UPDATE`, `DELETE` and time travel (`SELECT * FROM t VERSION AS OF 3`), just like in Unit 5 and Unit 6.

### 2. The Catalyst Optimizer

Catalyst is the reason DataFrames and SQL are faster than RDDs. It turns **what you wrote (the logical plan)** into **the actual execution order (the physical plan)**.

```
Your code / SQL
     │  parse: check syntax
     ▼
Unresolved logical plan      ── "SELECT amount FROM orders WHERE ..." as a tree; names not checked yet
     │  analysis: look up the catalog
     ▼
Resolved logical plan        ── Does the column exist? Is the type valid? (error here: "cannot resolve 'amt'")
     │  logical optimization (rules)
     ▼
Optimized logical plan       ── predicate pushdown, column pruning, constant folding, filter-before-join
     │  physical planning
     ▼
Physical plan(s)             ── which ALGORITHM, which JOIN STRATEGY (broadcast vs sort-merge), etc.
     │  cost model picks the best
     ▼
Selected physical plan  →  code generation (Tungsten)  →  runs on executors as RDD tasks
```

| Plan | What happens | Example |
|---|---|---|
| **Unresolved logical plan** | Your code as a tree; **syntax** checked | Typo in a keyword fails here |
| **Resolved (analyzed) logical plan** | Checks your code **can be run**: do the tables/columns exist, are types compatible? | `F.col("amt")` when the column is `amount` → `AnalysisException` |
| **Optimized logical plan** | Rule-based rewrites | Move `filter` before `join`; read only used columns; `1 + 2` → `3` |
| **Physical plan** | **How** to run it: algorithms, **join strategy**, scan method | Broadcast hash join because `products` is small |

**Key optimizations to name on an exam:**

- **Predicate pushdown:** push filters as early as possible, even into the file reader, so Parquet row groups / Delta files get skipped (Unit 6's min/max stats).
- **Column pruning (projection pushdown):** read only the columns the query uses. Big win with columnar formats.
- **Constant folding:** compute constant expressions once at planning time.
- **Join reordering / strategy selection:** pick the cheapest join.

**Tungsten** is the execution engine underneath: compact binary memory layout and **whole-stage code generation** (it compiles a chain of operators into one Java function).

**See the plan:**

```python
top_cities.explain()                    # physical plan
top_cities.explain(mode="extended")     # parsed → analyzed → optimized → physical
top_cities.explain(mode="formatted")    # easier to read
```

Look for `PushedFilters`, `BroadcastHashJoin` vs `SortMergeJoin`, and `Exchange` (**`Exchange` = a shuffle**).

**Why Python UDFs hurt:** Catalyst can't look inside a Python function. It's a black box, so filters can't be pushed through it, and every row has to be sent from the JVM to a Python process and back.

---

## Part E — Performance Tuning and Optimization

### 1. Partitioning: the right number of pieces

| Too few partitions | Too many partitions |
|---|---|
| Idle cores (fewer tasks than slots) | Scheduling overhead for thousands of tiny tasks |
| Huge partitions → memory pressure, spill to disk, OOM errors | Many tiny output files (small files problem) |

**Targets:** roughly **100–200 MB per partition**, and at least **2–3× as many partitions as total cores**.

```python
df.rdd.getNumPartitions()
spark.conf.set("spark.sql.shuffle.partitions", "64")   # default 200 is often wrong for small or huge data

df.repartition(64)                   # full shuffle; increase OR decrease; even sizes
df.repartition("user_id")            # shuffle so each user_id lands in one partition
df.coalesce(8)                       # merge partitions WITHOUT a full shuffle; decrease only; can be uneven
```

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Yes (wide) | No (narrow) |
| Can increase partitions | ✅ | ❌ |
| Balanced sizes | ✅ | Not always |
| Typical use | Fix skew, spread data before heavy work | Reduce the number of output files before writing |

### 2. Adaptive Query Execution (AQE)

AQE (`spark.sql.adaptive.enabled`, **on by default** since Spark 3.2) **re-optimizes the plan while it runs**, using real statistics from finished shuffle stages:

- **Coalesces shuffle partitions:** merges 200 tiny partitions into a few right-sized ones.
- **Switches join strategy:** turns a sort-merge join into a broadcast join if one side turns out to be small.
- **Handles skew joins:** splits oversized partitions into smaller tasks.

### 3. Join strategies

| Strategy | When | Cost |
|---|---|---|
| **Broadcast hash join** | One side is small (below `spark.sql.autoBroadcastJoinThreshold`, **10 MB** by default) | The small table is copied to **every executor**. **The big table isn't shuffled.** Fastest. |
| **Sort-merge join** | Both sides are large (default choice) | **Both sides shuffled** by the join key, then sorted and merged |
| **Shuffle hash join** | One side is medium-sized | Both shuffled, then a hash table built on the smaller side |

```python
from pyspark.sql.functions import broadcast

enriched = order_item.join(broadcast(products), "product_id")   # force a broadcast (products is a small dimension)
```

This fits star schemas (Unit 5) well: **big fact table + small dimension tables → broadcast the dimensions**. Don't broadcast something large: it's sent to the driver first and can crash it.

### 4. Data skew

**Skew** means a few keys have far more rows than the rest (e.g., one huge wholesale customer, or `user_id = null` for all guest orders). One task gets a giant partition and the whole stage waits for it. In the **Spark UI**, one task runs much longer than the others (the **straggler**).

**Fixes:**

- Let **AQE skew-join handling** split the big partition.
- **Filter or handle nulls separately** before the join.
- **Salting:** add a random number to the hot key so it spreads across several partitions, aggregate, then aggregate again without the salt.

```python
salted = (orders.withColumn("salt", (F.rand() * 10).cast("int"))
                .groupBy("user_id", "salt").agg(F.sum("amount").alias("part_sum"))   # spread the hot key
                .groupBy("user_id").agg(F.sum("part_sum").alias("revenue")))        # combine
```

### 5. Caching and persistence

Because of lazy evaluation, **every action recomputes from the source**. If one DataFrame feeds several actions, cache it:

```python
from pyspark import StorageLevel

silver = spark.read.format("delta").load("lake/silver/orders").filter("status = 'paid'")
silver.cache()            # DataFrame default: MEMORY_AND_DISK (lazy: filled by the first action)
silver.count()            # materializes the cache

daily  = silver.groupBy(F.to_date("order_date").alias("order_day")).agg(F.sum("amount"))
by_usr = silver.groupBy("user_id").agg(F.sum("amount"))    # both reuse the cached data

# silver.persist(StorageLevel.DISK_ONLY)  # alternative to cache(): choose the level yourself (use one or the other)
silver.unpersist()                       # free executor memory when done
```

**Don't cache everything.** Cache takes storage memory away from executors. Cache only what's reused, and only after filtering it down.

### 6. Write code the optimizer can help with

- **Filter early and select only the columns you need**, especially before joins and groupBys.
- **Prefer built-in functions** (`pyspark.sql.functions`) over Python UDFs. Built-ins run in the JVM and Catalyst understands them.
- If you need custom Python logic, use a **`pandas_udf`** (vectorized, sends data in Arrow batches) instead of a row-by-row `udf`.
- **Avoid `collect()` and `toPandas()` on large data.** They pull everything onto the driver → driver OOM. Use `show()`, `take(n)`, or write the result to storage.
- Avoid unnecessary `orderBy` (a full shuffle) and `distinct` on wide rows.
- Use `reduceByKey` instead of `groupByKey` if you work with RDDs.

```python
# ❌ Python UDF: row by row, black box to Catalyst
@F.udf("string")
def tier(amount):
    return "gold" if amount and amount >= 100 else "other"

# ✅ Built-in: vectorized in the JVM, optimizable
F.when(F.col("amount") >= 100, "gold").otherwise("other")

# ✅ If custom Python is truly needed
import pandas as pd
@F.pandas_udf("double")
def to_vnd(amount: pd.Series) -> pd.Series:
    return amount * 25_000
```

### 7. Storage layout

- Use **Parquet / Delta**, not CSV/JSON, after Bronze: columnar reads, predicate pushdown, stored schema (Unit 6).
- **Partition tables on disk** by a column you often filter on and that has low-to-medium cardinality (a date such as `order_day`, `country`). Spark then skips whole folders (**partition pruning**). Don't partition by a high-cardinality column like `user_id`: you'd get millions of tiny folders.
- **Avoid the small files problem:** `coalesce()` before writing. On Delta, run `OPTIMIZE` (compacts small files), and optionally **Z-ORDER** (or V-Order in Fabric) to cluster data so file skipping works better.

### 8. Memory problems and the Spark UI

| Symptom | Likely cause | Fix |
|---|---|---|
| Driver `OutOfMemoryError` | `collect()` / `toPandas()` on big data, broadcasting a large table | Aggregate first, write instead of collect, raise `spark.driver.memory` |
| Executor OOM or lots of **spill to disk** | Partitions too large, skew, too many cores sharing one executor's RAM | More partitions, fix skew, raise `spark.executor.memory`, fewer cores per executor |
| One task much slower than the others | **Skew** | AQE, salting, handle nulls separately |
| Thousands of tasks taking milliseconds each | Too many partitions | Lower `shuffle.partitions`, rely on AQE coalescing |

The **Spark UI** (port 4040, or the "Spark application" link in Fabric/Databricks) shows **jobs → stages → tasks**, the DAG, shuffle read/write sizes, spill, and task durations. It's the first place to look when a job is slow.

---

## Part F — PySpark in the Medallion Architecture

The lecture's `appName("Medallion_Pipeline")` is this pattern: the same Bronze → Silver → Gold flow as Unit 6, but on data too big for Pandas.

```python
from pyspark.sql import SparkSession, functions as F, Window

spark = (SparkSession.builder.appName("Medallion_Pipeline")
         .config("spark.executor.memory", "4g")
         .config("spark.executor.cores", "2")
         .getOrCreate())

# BRONZE: land raw data as is + metadata
raw = (spark.read.json("landing/orders/*.jsonl")
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name()))
raw.write.format("delta").mode("append").save("lake/bronze/orders")

# SILVER: fix types, clean, deduplicate (latest version per order)
w = Window.partitionBy("order_id").orderBy(F.col("_loaded_at").desc())
silver = (spark.read.format("delta").load("lake/bronze/orders")
               .withColumn("amount", F.col("amount").cast("double"))
               .withColumn("order_date", F.to_timestamp("order_date"))
               .withColumn("status", F.coalesce(F.lower(F.trim("status")), F.lit("unknown")))
               .na.drop(subset=["order_id", "amount"])
               .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))
silver.write.format("delta").mode("overwrite").save("lake/silver/orders")

# GOLD: aggregate for reporting; broadcast the small dimension
customers = spark.read.format("delta").load("lake/silver/customers")
gold = (silver.filter("status = 'paid'")
              .join(F.broadcast(customers), silver.user_id == customers.customer_id, "left")
              .groupBy(F.to_date("order_date").alias("order_day"), "city")
              .agg(F.sum("amount").alias("revenue"), F.countDistinct("order_id").alias("orders")))
(gold.coalesce(4).write.format("delta").mode("overwrite")
     .partitionBy("order_day").save("lake/gold/daily_city_revenue"))
```

Nothing runs until each `.write` (an **action**). Each write becomes one job, and Catalyst optimizes each chain as a whole.

---

## Key Terms Cheat Sheet

- **Distributed processing:** splitting data across machines and processing the pieces in parallel
- **Scale up vs scale out:** bigger machine / more machines
- **Unified analytics engine:** one engine for batch, SQL, streaming and ML (Spark)
- **Hadoop MapReduce:** Map → (Combiner) → Shuffle & Sort → Reduce; writes intermediate data to disk
- **Combiner:** local pre-aggregation before the shuffle to send less data over the network
- **In-memory processing:** keep intermediate data in RAM instead of writing it to disk
- **Driver:** runs your code, holds SparkSession/SparkContext, builds the plan, schedules tasks
- **Cluster manager:** allocates resources (Standalone, YARN, Kubernetes)
- **Worker node / executor:** machine in the cluster / JVM process that runs tasks and stores data
- **Partition:** a chunk of the data; one partition = one task
- **Application → Job → Stage → Task:** SparkSession → one per action → split at shuffles → one per partition
- **Parallel slots:** executors × cores per executor
- **RDD:** immutable, distributed, schema-less collection; resilient via **lineage**
- **Lineage:** the chain of transformations used to rebuild a lost partition
- **`reduceByKey` vs `groupByKey`:** pre-aggregates before the shuffle / ships every value
- **DataFrame:** distributed table with a schema, built on RDDs, optimized by Catalyst
- **Transformation / action:** lazy, returns a new DataFrame / runs the job
- **Lazy evaluation:** nothing runs until an action; the whole chain is optimized together
- **DAG:** directed acyclic graph of operations
- **Narrow / wide transformation:** 1→1 partition, no data movement / many→1, causes a shuffle
- **Shuffle:** moving data between executors over the network by key (`Exchange` in a plan)
- **Temp view:** session-scoped SQL name for a DataFrame
- **Catalyst Optimizer:** unresolved logical → resolved logical → optimized logical → physical plan
- **Predicate pushdown / column pruning:** filter as early as possible / read only the needed columns
- **Tungsten / whole-stage codegen:** binary memory format / compiles chained operators into one function
- **`explain()`:** shows the plan
- **`spark.sql.shuffle.partitions`:** partitions after a shuffle (default 200)
- **`repartition` vs `coalesce`:** full shuffle, up or down / no full shuffle, down only
- **AQE:** runtime re-optimization: coalesce partitions, switch joins, split skew
- **Broadcast hash join:** copy a small table to every executor, no shuffle of the big table (auto below 10 MB)
- **Sort-merge join:** default for two large tables; both sides shuffled and sorted
- **Data skew / salting:** a few keys hold most rows / add a random suffix to spread a hot key
- **`cache()` / `persist()` / `unpersist()`:** keep a reused DataFrame / choose the storage level / free it
- **Python UDF vs `pandas_udf`:** row-by-row black box / vectorized with Arrow batches
- **Partition pruning:** skipping whole folders on disk based on a filter
- **Spill:** data written to disk because execution memory ran out
- **Spark UI:** jobs, stages, tasks, DAG, shuffle and spill metrics

---

## Practice Questions

1. **Why can't big data just be processed on one powerful machine?**
   A single machine's RAM and CPU have limits, and bigger machines get very expensive. Distributed processing splits the data into partitions across many machines and processes them in parallel, and you add machines as data grows.

2. **Walk through MapReduce word count for "cat dog cat" on node 1 and "dog cat" on node 2, with a combiner.**
   Map: node 1 → (cat,1)(dog,1)(cat,1); node 2 → (dog,1)(cat,1). Combiner: node 1 → (cat,2)(dog,1); node 2 → (dog,1)(cat,1). Shuffle: cat → [2,1], dog → [1,1]. Reduce: (cat,3), (dog,2).

3. **What does the combiner do, and why does it help?**
   It pre-aggregates on each node before the shuffle, so fewer records travel over the network. `reduceByKey` in Spark does the same thing automatically.

4. **What was the main performance problem with Hadoop MapReduce, and how does Spark fix it?**
   MapReduce writes intermediate results to disk after every step, and multi-step or iterative jobs repeat that disk I/O. Spark keeps intermediate data in memory and plans the whole job as one DAG, optimized by Catalyst.

5. **Name the three parts of the Spark architecture and the job of each.**
   Driver: runs your code, holds the SparkSession, builds and optimizes the plan, schedules tasks. Cluster manager: allocates resources (executors). Worker nodes: host executors that run tasks and store data.

6. **Your plan is "join orders with customers, then filter status = 'paid'". What does Spark actually run, and why?**
   It filters first, then joins (predicate pushdown), because fewer rows go into the expensive join.

7. **A job reads 12 input partitions, filters, then does a `groupBy` with default settings and AQE off. How many stages and tasks?**
   2 stages (the groupBy shuffle splits them). Stage 0 has 12 tasks, Stage 1 has 200 tasks (`spark.sql.shuffle.partitions`).

8. **With 4 executors × 3 cores each, how many tasks run at once? How many waves for 60 tasks?**
   12 tasks at once, so 5 waves.

9. **In the lecture's SparkSession config, what does each setting control?**
   `spark.driver.memory` = RAM for the driver; `spark.executor.memory` = RAM per executor for tasks and cached data; `spark.executor.cores` = tasks each executor runs in parallel. `appName` names the app in the UI, and `getOrCreate()` actually creates (or reuses) the session.

10. **What does "resilient" mean in RDD?**
    If a partition is lost (an executor fails), Spark recomputes it from its lineage, the recorded chain of transformations, instead of relying on copies of the data.

11. **Why prefer `reduceByKey` over `groupByKey`?**
    `reduceByKey` combines values inside each partition before the shuffle, so much less data moves. `groupByKey` sends every value across the network.

12. **Give three differences between RDDs and DataFrames.**
    DataFrames have a schema (rows/columns), RDDs don't. DataFrames are optimized by Catalyst and Tungsten, RDDs run as written. In Python, DataFrame operations run in the JVM, while RDD lambdas run in Python processes with serialization overhead.

13. **Classify each as a transformation or action: `filter`, `count`, `groupBy`, `show`, `withColumn`, `write.parquet`, `select`, `collect`.**
    Transformations: `filter`, `groupBy`, `withColumn`, `select`. Actions: `count`, `show`, `write.parquet`, `collect`.

14. **What is lazy evaluation, and what are two benefits?**
    Spark records transformations in a DAG and runs nothing until an action. Benefits: it doesn't build full intermediate results after every line (saves memory), and it can optimize the whole chain together (reorder filters, prune columns, merge steps).

15. **You call `df.count()` and then `df.show()` on the same filtered DataFrame. How many times is the source read? How do you avoid it?**
    Twice: each action recomputes from the source. Call `df.cache()` (then an action), and `unpersist()` when done.

16. **Classify as narrow or wide, and explain why it matters: `filter`, `join`, `withColumn`, `groupBy`, `distinct`.**
    Narrow: `filter`, `withColumn` (no data movement, fast). Wide: `join`, `groupBy`, `distinct` (shuffle across the network, slow, creates a new stage).

17. **Write PySpark to get total paid revenue per user, highest first.**
    `orders.filter(F.col("status") == "paid").groupBy("user_id").agg(F.sum("amount").alias("revenue")).orderBy(F.desc("revenue"))`

18. **How do you find orders whose `user_id` doesn't exist in `customers` in PySpark?**
    `orders.join(customers, orders.user_id == customers.customer_id, "left_anti")`

19. **Is `spark.sql("SELECT ...")` slower or faster than the equivalent DataFrame code?**
    Neither: both are compiled by Catalyst into the same physical plan.

20. **List the Catalyst plan stages, and say where "column `amt` doesn't exist" is caught.**
    Unresolved logical plan (syntax) → resolved/analyzed logical plan (checks tables/columns against the catalog, **where the `amt` error is raised**) → optimized logical plan (pushdown, pruning, constant folding) → physical plan (algorithms, join strategy), chosen by a cost model, then code generation.

21. **What is the difference between the logical plan and the physical plan?**
    The logical plan describes *what* to compute (your code, checked and optimized). The physical plan describes *how*: the actual execution order, algorithms, and join strategy.

22. **Why can a Python UDF make a job slow even if the function is simple?**
    Rows are serialized from the JVM to a Python process and back, one at a time, and Catalyst can't optimize through it (no pushdown). Use built-in functions, or a `pandas_udf` if you really need Python.

23. **You're joining a 500 GB `order_item` fact table with a 5 MB `products` table. Which join strategy should run, and why?**
    A broadcast hash join: `products` is copied to every executor, so the 500 GB table doesn't have to be shuffled. It happens automatically below the 10 MB threshold, or you can force it with `broadcast(products)`.

24. **`repartition(10)` vs `coalesce(10)`: when do you use each?**
    `repartition` does a full shuffle, can increase or decrease, and gives even partitions. Use it to rebalance or fix skew. `coalesce` only decreases, avoids a full shuffle, and may be uneven. Use it to reduce output files before writing.

25. **A stage has 199 tasks finishing in 5 seconds and one taking 20 minutes. What's happening, and how do you fix it?**
    Data skew: one key (often null or a very large customer) holds most rows. Fixes: rely on AQE skew-join handling, handle nulls separately, or salt the hot key.

26. **Your job with 2 GB of data creates 200 tiny shuffle partitions. What two settings/features help?**
    Lower `spark.sql.shuffle.partitions` (e.g., 16–32), and keep AQE enabled so it coalesces small partitions automatically.

27. **Why is `big_df.toPandas()` dangerous?**
    It pulls all the data into the driver's memory, which can crash the driver (OOM). Filter and aggregate in Spark first, or write the result to storage.

28. **What three things does Adaptive Query Execution do at runtime?**
    Coalesces small shuffle partitions, switches sort-merge joins to broadcast joins when a side is small, and splits skewed partitions in joins.

29. **Which column should you `partitionBy` when writing an orders table: `order_day` (the date of `order_date`) or `user_id`? Why?**
    `order_day`: it's often used in filters and has manageable cardinality, so partition pruning works. (The raw `order_date` timestamp would give one folder per timestamp.) `user_id` would create a huge number of tiny folders and files.

30. **In the medallion pipeline, which lines actually trigger Spark jobs?**
    Only the actions, here the `.write...save()` calls (one job each, plus any `count`/`show`). Everything else only builds the plan.
