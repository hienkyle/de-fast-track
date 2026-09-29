# Unit 06 — Python for Data Engineering: Study Guide

**Scope:** Python's role in data engineering vs app development · NumPy for numerical computing · Pandas core concepts · Advanced data manipulation with Pandas · File formats (CSV, JSON, Parquet, Delta) · Pandas in the medallion architecture

The examples reuse the e-commerce data from Units 3 and 5 (`orders`, `order_item`, `products`, `customers`).

---

## Part A — Python in Data Engineering vs Web Applications

### 1. Two different jobs for Python

| | **Typical Python web application (OLTP)** | **Python in an OLAP lakehouse** |
|---|---|---|
| Main goal | **High availability** and **low latency** | **Throughput**: move and transform a lot of data |
| How data is processed | **Row by row**, one request at a time (one order, one user) | **In batches**: millions of rows at once |
| Key concerns | Low latency, **connection pool management**, **ACID transactions** | Processing speed, memory use, file formats, reruns without duplicates |
| Tools | **ORM (SQLAlchemy)**, **Pydantic** for validation, FastAPI (Unit 4) | **Pandas** and NumPy for small to medium data; Spark / **Microsoft Fabric** for large data |
| Python's role | Serves the business logic | **Manages and transforms data** (the "T" in ETL/ELT) |

### 2. Picking a tool by data size

| Data size | Tool | Why |
|---|---|---|
| Fits comfortably in RAM (MBs to a few GBs) | **Pandas** (+ NumPy) | Simple and fast on a single machine |
| Larger than one machine's RAM, or complex transforms | **Microsoft Fabric** (Spark notebooks, Dataflows, Lakehouse SQL) or another distributed engine | Spreads the work across a cluster |

**Rule of thumb:** Pandas often needs **5–10× the file size in RAM** while it works (copies made during transforms, plus Python object overhead for text). If a job gets close to that, use chunking (Part C) or move it to Fabric.

---

## Part B — Numerical Computing with NumPy

### 1. The `ndarray`

NumPy's core object is the **`ndarray`**: an N-dimensional array where **every element has the same type (dtype)**, stored in **one contiguous block of memory**. Pandas is built on top of it: each Pandas column is backed by a NumPy (or Arrow) array.

```python
import numpy as np

a = np.array([50.0, 20.0, 70.0])        # 1-D, dtype float64
m = np.array([[1, 2, 3], [4, 5, 6]])    # 2-D, shape (2, 3)

a.dtype, m.shape, m.ndim                # float64, (2, 3), 2
np.zeros(3), np.ones((2, 2)), np.arange(0, 10, 2), np.linspace(0, 1, 5)
```

### 2. Why arrays beat Python lists: vectorization

| | Python list | NumPy array |
|---|---|---|
| Stores | Pointers to separate Python objects (each with its own type info and ref count) | Raw values packed together in one block |
| Element types | Can be mixed | One dtype |
| Math on every element | You write a `for` loop, run by the Python interpreter | **Vectorized**: one call runs a compiled C loop |
| Memory | High overhead per element | Compact |

```python
prices = [50.0, 20.0, 70.0] * 1_000_000

with_tax = [p * 1.1 for p in prices]    # Python loop: slow
arr = np.array(prices)
with_tax = arr * 1.1                    # vectorized: often 10–100× faster
```

**Vectorization** means running one operation over a whole array at once in compiled code, instead of looping in Python.

### 3. Broadcasting

**Broadcasting** lets NumPy combine arrays of different shapes by "stretching" the smaller one, without actually copying it.

```python
qty   = np.array([[1, 2], [3, 4]])     # shape (2, 2): rows = orders, cols = products
price = np.array([10.0, 5.0])          # shape (2,): one price per product
qty * price                            # [[10., 10.], [30., 20.]]
```

The rule: compare shapes from the **right**. Two dimensions are compatible if they're **equal or one of them is 1**.

### 4. Boolean masks and conditional logic

```python
amount = np.array([50, 20, 70, 5])

amount[amount > 30]                                   # [50, 70]  (filter)
np.where(amount > 30, "big", "small")                 # if/else per element
np.select([amount >= 50, amount >= 20],               # if / elif / else
          ["high", "mid"], default="low")
```

Combine conditions with `&`, `|`, `~` and **wrap each condition in parentheses**: `(a > 10) & (a < 60)`. Python's `and` / `or` don't work element by element.

### 5. Aggregations and the `axis` argument

```python
m = np.array([[1, 2, 3], [4, 5, 6]])
m.sum()          # 21: everything
m.sum(axis=0)    # [5, 7, 9]: down the rows → one result per column
m.sum(axis=1)    # [6, 15]: across the columns → one result per row
np.mean, np.median, np.std, np.min, np.max, np.percentile(a, 95)
```

### 6. Missing values: `NaN`

- `np.nan` is a **float**, so an integer array that contains a NaN becomes `float64`.
- `NaN` spreads through math: `np.sum([1, np.nan]) → nan`. Use `np.nansum`, `np.nanmean`, and so on to skip it.
- `np.nan == np.nan` is `False`. Test for it with `np.isnan()`.

### 7. Other useful pieces

- **dtypes and memory:** `int8`/`int32`/`int64`, `float32`/`float64`, `bool`. Picking a smaller dtype cuts memory (`astype("float32")`).
- **Views vs copies:** slicing (`a[1:3]`) returns a **view** that shares memory, so changing it changes the original. Use `.copy()` when you need an independent array.
- **Random data for tests:** `rng = np.random.default_rng(42); rng.integers(1, 100, size=10)`.

---

## Part C — Pandas Core Concepts

### 1. The three building blocks

| Object | What it is |
|---|---|
| **DataFrame** | A 2-D **table** with labeled rows and columns. Think of it as a SQL table or a spreadsheet in memory. |
| **Series** | **One column**: a 1-D array holding **one type of data**, plus an index |
| **Index** | The **row labels**. Used for lookup (`.loc`), for aligning data in operations, and in joins. The default is `0, 1, 2, …` |

```python
import pandas as pd

orders = pd.DataFrame({
    "order_id":   [1, 2, 3, 4],
    "user_id":    [7, 7, 9, 12],
    "status":     ["paid", "paid", "created", None],
    "amount":     [50.0, 20.0, 70.0, None],
    "order_date": pd.to_datetime(["2026-06-01", "2026-06-01", "2026-06-02", "2026-06-03"]),
})

orders["amount"]          # Series
orders.dtypes             # int64, int64, object/str, float64, datetime64[ns]
orders.shape              # (4, 5)
orders.info(); orders.describe(); orders.head()
```

**Index alignment:** when you combine two Series, Pandas matches rows **by index label, not by position**. Labels that don't match produce NaN. This is a common source of surprise NaNs after filtering or joining.

### 2. Native collections (list of dicts) vs DataFrame

```python
# Native: list of dicts (one dict per row, how an API or ORM usually hands you data)
rows = [{"order_id": 1, "amount": 50.0}, {"order_id": 2, "amount": 20.0}]
total = 0
for r in rows:                       # a Python loop for every operation
    total += r["amount"]

# DataFrame: column-oriented
df = pd.DataFrame(rows)
total = df["amount"].sum()           # vectorized, runs in C/Cython
```

| | List of dicts | DataFrame |
|---|---|---|
| Layout | Row-oriented. Every row repeats the key names, and every value is a full Python object. | **Column-oriented**: each column is one typed array |
| Memory | **High overhead** | Compact (especially for numbers) |
| Access pattern | `for` loops | **Vectorized** column operations (C/Cython underneath) |
| Best for | Handling a single record in an app (Pydantic, ORM) | Processing **whole columns** across many rows |

This is the same row-vs-columnar idea from Unit 5, but in memory.

### 3. dtypes to know

| dtype | Use for | Note |
|---|---|---|
| `int64`, `float64` | Numbers | An int column with a missing value turns into float |
| `Int64`, `Float64`, `boolean` (capitalized) | **Nullable** numbers and booleans | Missing values are stored as `pd.NA` and the int stays an int |
| `object` / `str` / `string` | Text | Newer Pandas versions default to a dedicated string dtype |
| `category` | Repeated labels (`status`, `city`) | Stored as integer codes plus a lookup table (dictionary encoding from Unit 5), which saves a lot of memory |
| `datetime64[ns]` | Timestamps | Use `pd.to_datetime` to convert |

**Missing values:** `NaN` (float), `None` (Python), `pd.NA` (nullable dtypes), `NaT` (datetimes). `isna()` detects all of them.

### 4. Memory and the variable life cycle

Python frees an object when **nothing references it anymore**. Where you create a DataFrame decides how long it stays in memory.

| | Global variable | Local variable |
|---|---|---|
| Where it's defined | Outside any function or block (top level of a script or notebook cell) | Inside a function |
| How long it lives | Until the program or notebook kernel ends, or until you `del` it | Until the function returns, then it can be freed |
| Risk | Intermediate DataFrames (`df_raw`, `df_tmp`, `df2`) pile up in a notebook and use up RAM | Low: temporary data disappears after each step |

```python
# Risky in a notebook: every intermediate stays in memory
raw = pd.read_csv("orders.csv")
clean = raw.dropna()
final = clean.groupby("user_id")["amount"].sum()

# Better: temporary DataFrames are local and get freed when the function ends
def build_user_totals(path: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    clean = raw.dropna(subset=["amount"])
    return clean.groupby("user_id", as_index=False)["amount"].sum()

final = build_user_totals("orders.csv")

# Free a big global by hand if needed
import gc
del raw
gc.collect()
```

**More ways to reduce memory:**

- Load only the columns you need: `usecols=[...]` (CSV), `columns=[...]` (Parquet).
- Set small dtypes up front: `dtype={"status": "category", "qty": "int32"}`.
- Check usage with `df.memory_usage(deep=True)`.
- Process large CSVs in **chunks**:

```python
totals = []
for chunk in pd.read_csv("orders_big.csv", chunksize=500_000):
    totals.append(chunk.groupby("user_id")["amount"].sum())
result = pd.concat(totals).groupby(level=0).sum()
```

---

## Part D — Advanced Data Manipulation with Pandas

### 1. I/O: read in, write out

```python
df = pd.read_csv("orders.csv")
df = pd.read_json("orders.jsonl", lines=True)
df = pd.read_parquet("orders.parquet")
df = pd.read_sql("SELECT * FROM orders", engine)       # SQLAlchemy engine (Unit 4)

df.to_csv("out.csv", index=False)
df.to_parquet("out.parquet", index=False)
df.to_sql("orders_clean", engine, if_exists="append", index=False)
```

Format details are in Part E. Use **`index=False`** when writing, unless the index holds real data.

### 2. Data cleaning and handling nulls

```python
# Detect
df.isna().sum()                              # null count per column
df[df["amount"].isna()]                      # rows with a missing amount

# Drop
df.dropna()                                  # any null in the row → drop
df.dropna(subset=["order_id", "amount"])     # only look at key columns
df.dropna(thresh=4)                          # keep rows with at least 4 non-null values

# Fill
df.fillna({"status": "unknown", "amount": 0})
df["amount"] = df["amount"].fillna(df["amount"].median())
df["price"] = df.groupby("product_id")["price"].ffill()   # carry the last known value forward

# Other cleaning
df = df.drop_duplicates(subset=["order_id"], keep="last")
df["status"] = df["status"].str.strip().str.lower()
df = df.rename(columns={"UserID": "user_id"})
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")   # bad values → NaN instead of an error
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
```

**Drop or fill?** Drop when the value is required (a key, the main measure). Fill when there's a sensible default (`"unknown"`, 0 for a count). **Don't fill a money amount with 0** without thinking about it: it silently changes your totals.

### 3. Filtering

In plain Python, you have to **loop** and check each row. Pandas builds a **boolean mask with vectorization** and applies it all at once.

```python
# Native Python
paid = [r for r in rows if r["status"] == "paid" and r["amount"] > 30]

# Pandas
paid = df[(df["status"] == "paid") & (df["amount"] > 30)]
paid = df.query("status == 'paid' and amount > 30")      # same thing, SQL-like
df[df["status"].isin(["paid", "shipped"])]
df[df["amount"].between(20, 60)]
df[~df["email"].str.contains("@test.com", na=False)]
```

**`.loc` vs `.iloc`:**

- `.loc[rows, cols]` selects **by label** or mask: `df.loc[df["amount"] > 30, ["order_id", "amount"]]`
- `.iloc[rows, cols]` selects **by position**: `df.iloc[0:10, 0:3]`

**Assign with `.loc`, not chained indexing:**

```python
df.loc[df["status"].isna(), "status"] = "unknown"   # ✅
df[df["status"].isna()]["status"] = "unknown"       # ❌ changes a temporary copy, not df
```

(With Copy-on-Write, the default in recent Pandas, the chained version never modifies `df`.)

### 4. Transforming into new columns

In plain Python, computing a new field row by row is **prone to type errors**: one `None` or one `"N/A"` string in the data crashes the loop (`TypeError: unsupported operand type(s) for *: 'NoneType' and 'float'`). Pandas **handles nulls automatically**: math on a missing value just gives NaN for that row and the rest keep going.

```python
# Native: crashes on the first None
for r in rows:
    r["amount_vnd"] = r["amount"] * 25_000        # TypeError if amount is None

# Pandas: NaN in → NaN out, no crash
df["amount_vnd"] = df["amount"] * 25_000
```

Type errors can still happen in Pandas when a column has the **wrong dtype** (e.g., `amount` was read as text). Fix the type first with `pd.to_numeric(..., errors="coerce")` or `astype`.

**Ways to create columns, fastest first:**

```python
# 1. Vectorized arithmetic
df["line_total"] = df["quantity"] * df["unit_price"]

# 2. Conditional logic with NumPy
df["size"] = np.where(df["amount"] >= 50, "big", "small")
df["tier"] = np.select([df["amount"] >= 100, df["amount"] >= 30],
                       ["gold", "silver"], default="bronze")

# 3. .str and .dt accessors (vectorized text and date operations)
df["email_domain"] = df["email"].str.split("@").str[1]
df["order_month"]  = df["order_date"].dt.to_period("M")
df["weekday"]      = df["order_date"].dt.day_name()

# 4. Lookup with .map
df["status_label"] = df["status"].map({"paid": "Paid", "created": "Created"})

# 5. .apply: runs a Python function for every row, so it's slow. Last resort.
df["note"] = df.apply(lambda r: f"{r.user_id}-{r.order_id}", axis=1)

# Several at once, chain-friendly
df = df.assign(line_total=lambda d: d.quantity * d.unit_price,
               is_big=lambda d: d.line_total >= 50)

# Binning
df["amount_band"] = pd.cut(df["amount"], bins=[0, 20, 50, np.inf],
                           labels=["low", "mid", "high"])
```

### 5. Joining tables (like SQL joins)

`merge` is Pandas' version of SQL `JOIN`:

```python
enriched = order_item.merge(products, on="product_id", how="left")

enriched = orders.merge(
    customers, left_on="user_id", right_on="customer_id",
    how="left",
    validate="many_to_one",       # raises an error if customers has duplicate keys
    indicator=True,               # adds a _merge column: left_only / right_only / both
)
orphans = enriched[enriched["_merge"] == "left_only"]   # orders with no matching customer
```

| `how=` | SQL equivalent |
|---|---|
| `"inner"` (default) | `INNER JOIN` |
| `"left"` / `"right"` | `LEFT` / `RIGHT JOIN` |
| `"outer"` | `FULL OUTER JOIN` |
| `"cross"` | `CROSS JOIN` |

**Pitfalls:**

- **Row explosion:** duplicate keys on both sides multiply the rows. Check `len()` before and after, and use `validate=`.
- **Key dtype mismatch:** `int` vs `str` keys match nothing. Cast both sides to the same type first.
- **Overlapping column names** get `_x` / `_y` suffixes. Set `suffixes=("_order", "_cust")`.

**Other ways to combine:**

- `pd.concat([df_jan, df_feb], ignore_index=True)` stacks rows, like `UNION ALL`.
- `df.join(other)` joins on the index.

### 6. Grouping and aggregating

```python
# SQL: SELECT user_id, COUNT(*), SUM(amount), AVG(amount) ... GROUP BY user_id
summary = (df.groupby("user_id", as_index=False)
             .agg(order_count=("order_id", "count"),
                  revenue=("amount", "sum"),
                  avg_order=("amount", "mean")))

# Several keys
df.groupby(["order_month", "status"])["amount"].sum().reset_index()

# transform: keeps the original row count (like a SQL window function)
df["user_total"] = df.groupby("user_id")["amount"].transform("sum")
df["pct_of_user"] = df["amount"] / df["user_total"]

# filter: keep only groups that meet a condition (like HAVING)
repeat_buyers = df.groupby("user_id").filter(lambda g: len(g) >= 2)
```

| Method | Rows returned | SQL analogy |
|---|---|---|
| `.agg()` | One per group | `GROUP BY` |
| `.transform()` | Same as the input | `SUM(...) OVER (PARTITION BY ...)` |
| `.filter()` | Rows from the groups that pass | `HAVING` |

### 7. Window-style operations

```python
df = df.sort_values(["user_id", "order_date"])
df["order_seq"]   = df.groupby("user_id").cumcount() + 1                         # ROW_NUMBER()
df["prev_amount"] = df.groupby("user_id")["amount"].shift(1)                      # LAG()
df["running_tot"] = df.groupby("user_id")["amount"].cumsum()                      # running SUM
df["rank"]        = df.groupby("order_month")["amount"].rank(ascending=False, method="dense")  # DENSE_RANK()

daily = df.set_index("order_date").resample("D")["amount"].sum()
daily.rolling(7).mean()                                                            # 7-day moving average

# Keep the latest row per key (like QUALIFY ROW_NUMBER() = 1 in Unit 5)
latest = (df.sort_values("updated_at")
            .drop_duplicates(subset=["customer_id"], keep="last"))
```

### 8. Reshaping

```python
# Long → wide (pivot table): rows = month, columns = status, values = revenue
wide = df.pivot_table(index="order_month", columns="status",
                      values="amount", aggfunc="sum", fill_value=0)

# Wide → long
long = wide.reset_index().melt(id_vars="order_month",
                               var_name="status", value_name="revenue")

# Nested lists → one row per element
df.explode("tags")
```

### 9. Method chaining

Chaining keeps each step readable and avoids piling up intermediate global variables (Part C.4):

```python
result = (
    pd.read_parquet("silver/orders.parquet")
      .query("status == 'paid'")
      .assign(order_month=lambda d: d.order_date.dt.to_period("M").astype(str))
      .groupby(["order_month", "user_id"], as_index=False)
      .agg(revenue=("amount", "sum"))
      .sort_values("revenue", ascending=False)
)
```

---

## Part E — Working with File Formats

### 1. CSV

A plain-text, row-based format. Every system can read it, but it has **no types and no schema**.

```python
df = pd.read_csv(
    "orders.csv",
    usecols=["order_id", "user_id", "status", "amount", "order_date"],
    dtype={"order_id": "int64", "status": "category", "zip": "string"},
    parse_dates=["order_date"],
    na_values=["", "N/A", "null"],
    sep=",", encoding="utf-8",
)
df.to_csv("orders_clean.csv", index=False)
```

**Common problems:**

- **Types get guessed.** Leading zeros disappear (`"00123"` → `123`) and IDs become floats when they have gaps. Set `dtype=` explicitly.
- Dates arrive as text unless you use `parse_dates`.
- Commas or newlines inside values, different delimiters, encoding problems (`utf-8` vs `latin-1`).
- Large, slow to parse, no compression unless you gzip it (`to_csv("x.csv.gz")`).

### 2. JSON

Semi-structured and **nested**. It's the usual format from APIs (Unit 4).

```python
# JSON Lines: one JSON object per line. Best for logs and streaming.
df = pd.read_json("orders.jsonl", lines=True)
df.to_json("orders.jsonl", orient="records", lines=True, date_format="iso")

# Flatten nested API data
data = [
  {"order_id": 1, "customer": {"id": 7, "city": "HN"},
   "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 1}]},
]
orders = pd.json_normalize(data, sep="_")        # customer_id, customer_city, items (still a list)
items  = pd.json_normalize(data, record_path="items",
                           meta=["order_id", ["customer", "id"]])
#   sku  qty  order_id  customer.id
#   A1   2    1         7
#   B2   1    1         7
```

- `orient="records"` gives a list of row objects, which is the most common layout.
- `json_normalize` handles the **"lightly flatten nested data"** step in Silver (Unit 5).
- JSON repeats key names on every record, so files are large. It keeps only basic types (dates become strings).

### 3. Parquet

An open, **columnar**, compressed, binary format that **stores its own schema**. It's the standard file format of data lakes.

```python
df.to_parquet("orders.parquet", engine="pyarrow", compression="snappy", index=False)

# Read only the columns and rows you need
df = pd.read_parquet("orders.parquet",
                     columns=["order_id", "amount"],
                     filters=[("status", "==", "paid")])

# Partitioned dataset: one folder per value
df.to_parquet("lake/orders/", partition_cols=["order_date"], index=False)
# lake/orders/order_date=2026-06-01/part-0.parquet
# lake/orders/order_date=2026-06-02/part-0.parquet
```

- **Columnar** (Unit 5): reads only the columns it needs, and compresses well (dictionary encoding + RLE, then snappy/zstd/gzip).
- Files are split into **row groups**, each with **min/max statistics** per column. Readers skip row groups that can't match a filter (**predicate pushdown**), just like Snowflake's pruning of micro-partitions.
- **Types survive** a round trip (ints, decimals, timestamps, categories), so there's no guessing like with CSV.
- Parquet files are **immutable**: you can't update one row. You rewrite the file. There are **no transactions** either. Delta solves both.
- **Avoid many tiny files** (the "small files problem"): each file adds overhead. Aim for files of roughly 100 MB–1 GB.

### 4. Delta Lake

Delta is **Parquet files plus a transaction log** (`_delta_log/`, JSON commit files). It's an **open table format** (Unit 5), and it's the default table format in **Microsoft Fabric** lakehouses and Databricks.

```
lake/orders_delta/
├── _delta_log/
│   ├── 00000000000000000000.json    ← commit 0: create + add files
│   └── 00000000000000000001.json    ← commit 1: append
├── part-0000-....snappy.parquet
└── part-0001-....snappy.parquet
```

From Pandas, use the **`deltalake`** package (delta-rs), which needs no Spark:

```python
from deltalake import DeltaTable, write_deltalake

write_deltalake("lake/orders_delta", df, mode="overwrite")        # create/replace
write_deltalake("lake/orders_delta", new_rows, mode="append")      # add rows
write_deltalake("lake/orders_delta", df_with_new_col, mode="append",
                schema_mode="merge")                               # schema evolution

dt = DeltaTable("lake/orders_delta")
df = dt.to_pandas()
dt.history()                                                       # list of commits

# Time travel: read an older version
old = DeltaTable("lake/orders_delta", version=0).to_pandas()

# Upsert (MERGE), like Unit 5's MERGE INTO
(dt.merge(source=updates, predicate="t.order_id = s.order_id",
          source_alias="s", target_alias="t")
   .when_matched_update_all()
   .when_not_matched_insert_all()
   .execute())

dt.vacuum(retention_hours=168, dry_run=False)    # delete old files no longer referenced (limits time travel)
```

**What Delta adds on top of Parquet:**

- **ACID transactions:** a write either fully commits to the log or doesn't happen, so readers never see half-written data.
- **`UPDATE` / `DELETE` / `MERGE`**, which plain Parquet can't do.
- **Schema enforcement** (rejects mismatched writes) and **schema evolution** (adds new columns on purpose).
- **Time travel** by version or timestamp, for audits, debugging, and rollback.
- **File statistics** in the log for data skipping.

In Fabric, Spark handles big Delta tables. Pandas + `deltalake` is fine for small and medium ones.

### 5. Format comparison

| | **CSV** | **JSON** | **Parquet** | **Delta** |
|---|---|---|---|---|
| Layout | Row, text | Row, text, nested | **Columnar**, binary | Columnar (Parquet) + transaction log |
| Schema / types | ❌ Inferred | ⚠️ Basic types only | ✅ Stored in the file | ✅ Enforced + evolvable |
| Nested data | ❌ | ✅ | ✅ | ✅ |
| Compression | Poor (unless gzipped) | Poor | ✅ Excellent | ✅ Excellent |
| Read only some columns | ❌ | ❌ | ✅ | ✅ |
| Update / delete rows | Rewrite the file | Rewrite the file | Rewrite the file | ✅ `MERGE` / `UPDATE` / `DELETE` |
| ACID / time travel | ❌ | ❌ | ❌ | ✅ |
| Human readable | ✅ | ✅ | ❌ | ❌ |
| Typical use | Exports, spreadsheets, simple interchange | APIs, logs, events | Data lake files, analytics | Lakehouse tables (Fabric, Databricks) |

---

## Part F — Pandas in the Medallion Architecture

| Layer | What Pandas does here | Typical format |
|---|---|---|
| **Bronze** | **Raw data only.** Read the source and save it **as is**, adding only metadata (`_loaded_at`, `_source_file`). No cleaning. | Original format (CSV/JSON), or Parquet/Delta with values kept as the source sent them (no type conversion) |
| **Silver** | **Transform, clean, export:** fix types, handle nulls, deduplicate, flatten JSON, standardize values, join reference data | Parquet / Delta |
| **Gold** | **Aggregated data for reporting:** group, pivot, build fact and dimension tables | Delta tables that BI tools (Power BI in Fabric) read |

```python
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from deltalake import DeltaTable, write_deltalake

def to_bronze(src: str) -> None:
    raw = pd.read_json(src, lines=True, dtype=False)            # don't let Pandas convert types
    raw["_loaded_at"] = datetime.now(timezone.utc)
    raw["_source_file"] = src
    write_deltalake("lake/bronze/orders", raw, mode="append")    # append-only history

def to_silver() -> None:
    df = DeltaTable("lake/bronze/orders").to_pandas()             # read through the Delta log
    df = (df.assign(amount=lambda d: pd.to_numeric(d["amount"], errors="coerce"),
                    order_date=lambda d: pd.to_datetime(d["order_date"], errors="coerce"),
                    status=lambda d: d["status"].str.strip().str.lower().fillna("unknown"))
            .dropna(subset=["order_id", "amount"])
            .sort_values("_loaded_at")
            .drop_duplicates(subset=["order_id"], keep="last"))
    write_deltalake("lake/silver/orders", df, mode="overwrite")

def to_gold() -> None:
    df = DeltaTable("lake/silver/orders").to_pandas()
    daily = (df[df["status"] == "paid"]
               .groupby(df["order_date"].dt.date.rename("order_day"))
               .agg(revenue=("amount", "sum"), orders=("order_id", "nunique"))
               .reset_index())
    write_deltalake("lake/gold/daily_revenue", daily, mode="overwrite")
```

Each step is a **function**, so temporary DataFrames are **local** and get freed as soon as the step finishes (Part C.4).

Delta tables are read with `DeltaTable(...).to_pandas()`, never `pd.read_parquet` on the folder. After an `overwrite`, the old Parquet files stay on disk until `vacuum`, and only `_delta_log` knows which files are current (Part E.4). Reading the folder as plain Parquet would pick up old versions and create duplicates.

---

## Key Terms Cheat Sheet

- **Throughput vs latency:** amount of data processed per unit of time (OLAP) / response time of one request (OLTP)
- **Batch processing:** processing many rows at once instead of one at a time
- **ndarray:** NumPy's fixed-type array stored in one contiguous block of memory
- **Vectorization:** one operation over a whole array, run in compiled C instead of a Python loop
- **Broadcasting:** combining arrays of different shapes by stretching the smaller one
- **Boolean mask:** a True/False array used to filter rows
- **`axis=0` / `axis=1`:** down the rows (one result per column) / across the columns (one result per row)
- **DataFrame / Series / Index:** table / one typed column / row labels
- **Index alignment:** operations match rows by label, not position
- **Nullable dtypes (`Int64`, `boolean`, `string`):** keep their type when values are missing (`pd.NA`)
- **`category` dtype:** dictionary-encoded labels, saves memory
- **Global vs local variable:** lives until the program ends or `del` / freed when the function returns
- **Chunking:** `read_csv(chunksize=...)` to process a file in pieces
- **`.loc` / `.iloc`:** select by label / by position
- **`errors="coerce"`:** turn values that can't be converted into NaN instead of raising an error
- **`.apply(axis=1)`:** a slow, row-by-row Python function. Use it only as a last resort.
- **`merge` / `concat`:** SQL join / `UNION ALL`
- **`validate=`, `indicator=`:** check join cardinality / show where each row matched
- **`agg` / `transform` / `filter`:** `GROUP BY` / window function / `HAVING`
- **`shift`, `cumsum`, `rank`, `rolling`:** LAG, running total, RANK, moving average
- **`pivot_table` / `melt`:** long → wide / wide → long
- **`json_normalize`:** flattens nested JSON into columns or rows
- **Parquet:** columnar, compressed, schema included, immutable files
- **Row group / predicate pushdown:** a chunk of rows in a Parquet file / skipping chunks using min/max stats
- **Partitioning:** one folder per value of a column (`order_date=2026-06-01/`)
- **Small files problem:** too many tiny files slow reads down
- **Delta Lake:** Parquet + `_delta_log` → ACID, MERGE, schema enforcement/evolution, time travel
- **`vacuum`:** deletes old, unreferenced files (shortens how far back time travel works)
- **Medallion with Pandas:** Bronze = raw as is · Silver = clean + transform · Gold = aggregates for reports

---

## Practice Questions

1. **How does Python's role differ in a web app vs an OLAP lakehouse?**

    In a web app it handles individual requests row by row, focused on low latency, availability, connection pooling, and ACID transactions (tools: SQLAlchemy ORM, Pydantic). In a lakehouse it manages and transforms batches of millions of rows, focused on throughput (tools: Pandas for small/medium data, Fabric/Spark for large data).

2. **When should you move a job from Pandas to Microsoft Fabric (or Spark)?**

    When the data doesn't fit comfortably in one machine's memory (remember the working copies Pandas makes), or the transforms are heavy and complex enough to need a distributed engine.

3. **Why is `np.array(prices) * 1.1` faster than a list comprehension?**

    The array stores typed values in one contiguous block, and the multiplication runs as one compiled C loop (vectorization). The list comprehension runs Python bytecode for every element and handles separate Python objects.

4. **What is the result of `np.array([[1,2,3],[4,5,6]]).sum(axis=0)`?**

    `[5, 7, 9]`: it sums down the rows, giving one value per column.

5. **Can shapes `(3, 4)` and `(4,)` broadcast? What about `(3, 4)` and `(3,)`?**

    `(3, 4)` and `(4,)`: yes, the rightmost dimensions match. `(3, 4)` and `(3,)`: no, 4 ≠ 3. Reshape the second to `(3, 1)` to make it work.

6. **Why does an integer column become `float64` after reading a file with blanks? How do you keep it as an integer?**

    `NaN` is a float, so the column is upcast. Use the nullable `Int64` dtype (e.g., `dtype={"qty": "Int64"}`).

7. **Name the three core Pandas objects.**

    DataFrame (a table), Series (one typed column), Index (row labels used for lookup and alignment).

8. **Compare a list of dicts and a DataFrame for processing a column of a million rows.**

    A list of dicts is row-oriented, repeats key names, stores every value as a Python object (high memory), and needs a `for` loop. A DataFrame stores each column as a typed array and processes it with vectorized C/Cython code, so it's faster and smaller.

9. **Your notebook runs out of memory after several steps. What causes this, and what are three fixes?**

    Intermediate DataFrames stored in global variables stay in memory until the kernel ends. Fixes: wrap steps in functions so temporary DataFrames are local; `del` large objects and run `gc.collect()`; load fewer columns and smaller dtypes (`usecols`, `category`); read in chunks.

10. **Write a filter for paid orders over 30. Why can't you use `and`?**

    `df[(df["status"] == "paid") & (df["amount"] > 30)]`. `and` needs a single True/False and fails on a Series. `&` compares element by element. The parentheses are needed because `&` binds tighter than `==` and `>`.

11. **What's wrong with `df[df.amount > 30]["flag"] = True`? Fix it.**

    It's chained indexing: the assignment goes to a temporary copy, so `df` isn't changed. Use `df.loc[df["amount"] > 30, "flag"] = True`.

12. **Why does computing a new field in a Python loop often crash, when the Pandas version doesn't?**

    One `None` or text value causes a `TypeError` and stops the loop. Pandas propagates NaN for missing values, so those rows become NaN and the rest are computed. A wrong column dtype can still cause errors, so convert first with `pd.to_numeric(..., errors="coerce")`.

13. **Rank these from fastest to slowest: `.apply(axis=1)`, vectorized arithmetic, `np.where`.**

    Vectorized arithmetic ≈ `np.where` (both vectorized) > `.apply(axis=1)` (a Python function for every row).

14. **`dropna()` vs `fillna()`: when do you use each?**

    Drop rows when a required field (key or main measure) is missing. Fill when there's a sensible default (`"unknown"`, 0 for a count, the last known value). Don't fill amounts with 0 without thinking, since it changes totals.

15. **After `orders.merge(customers, left_on="user_id", right_on="customer_id")` you have more rows than `orders`. Why, and how do you catch it?**

    `customers` has duplicate `customer_id` values, so the rows multiply. Catch it with `validate="many_to_one"`, and compare `len()` before and after.

16. **How do you find orders with no matching customer?**

    `merge(..., how="left", indicator=True)` and keep the rows where `_merge == "left_only"`.

17. **`groupby().agg()` vs `groupby().transform()`: what's the difference?**

    `agg` returns one row per group (like `GROUP BY`). `transform` returns a value for every original row (like a window function), e.g., adding each user's total to every order row.

18. **Keep only the latest record per `customer_id`, using `updated_at`.**

    `df.sort_values("updated_at").drop_duplicates(subset=["customer_id"], keep="last")`

19. **Flatten API orders with nested `items` into one row per item that keeps `order_id`.**

    `pd.json_normalize(data, record_path="items", meta=["order_id"])`

20. **Give three problems with CSV and how Parquet solves them.**

    CSV has no stored types (leading zeros lost, dates as text), must be read in full even for one column, and compresses poorly. Parquet stores the schema, is columnar (reads only the needed columns), and compresses well.

21. **What are row groups and predicate pushdown in Parquet?**

    A Parquet file is split into row groups, each with min/max stats per column. With a filter like `amount > 100`, the reader skips row groups whose max is ≤ 100, so it reads less data.

22. **Why can't you update one row in a Parquet file, and how does Delta handle updates?**

    Parquet files are immutable, so you'd have to rewrite the file. Delta writes new Parquet files with the changed rows and records in `_delta_log` which files are now valid, all in one atomic commit.

23. **List four features Delta adds to Parquet.**

    Any four of: ACID transactions; `UPDATE`/`DELETE`/`MERGE`; schema enforcement and evolution; time travel; file statistics for data skipping.

24. **How do you read version 3 of a Delta table from Pandas? What can stop this from working?**

    `DeltaTable(path, version=3).to_pandas()`. It fails if `vacuum` has already deleted the files that version needs.

25. **Which format fits each case: (a) a finance export to open in Excel, (b) an API event log, (c) analytics files in a lake, (d) a Fabric lakehouse table that gets daily upserts?**

    (a) CSV, (b) JSON Lines, (c) Parquet, (d) Delta.

26. **Describe what Pandas does in each medallion layer.**

    Bronze: load the raw data as is and only add metadata (load time, source file). Silver: clean and transform (types, nulls, deduplication, flattening, standardizing) and export. Gold: aggregate into reporting tables (daily revenue, facts and dimensions) for BI.
