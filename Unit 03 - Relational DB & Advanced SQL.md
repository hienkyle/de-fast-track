# Unit 03 — Relational DB & Advanced SQL (OLTP focus): Study Guide

**Scope:** OLTP database design (normalization) · Advanced SQL (joins, subqueries, CTEs, window functions) · Stored procedures, functions, triggers · Transactions (ACID) & concurrency control · Performance tuning (indexing, query optimization)

**Naming note:** later units write `product_id` for `prod_id`, and `amount` for the order total (`total_amt` here).

---

## Part A — OLTP vs OLAP (the big picture)

| | **OLTP** (Online Transactional Processing) | **OLAP** (Online Analytical Processing) |
|---|---|---|
| Purpose | Runs the application: CRUD, stored procedures, functions | Reporting and other heavy queries |
| Optimized for | Many fast, small **write** transactions | **Read** queries over large amounts of data |
| Traits | Fast, supports async processing, frequent access | Large, infrequent queries |
| Examples | PostgreSQL, MySQL | Analytical databases and warehouses |
| Schema | Normalized, enforces ACID | Denormalized |

**Why keep them separate:** if OLTP and OLAP work run on the same system, the heavy analytical queries will likely **lock** tables or rows the application needs. The OLTP side then can't work: transactions wait or time out.

---

## Part B — Database Design for OLTP

### Rule 1: Keys and relationships
- **Primary key (PK):** uniquely identifies a row. It is never NULL and should never change. Usually a surrogate key (`id SERIAL` / `BIGINT GENERATED ALWAYS AS IDENTITY`).
- **Natural key:** a real-world unique value such as `email` or `sku`. Protect it with a `UNIQUE` constraint even when you use a surrogate PK.
- **Foreign key (FK):** a column that references another table's PK. It enforces **referential integrity**, so there are no orphan rows. Options include `ON DELETE CASCADE / RESTRICT / SET NULL`.
- **Composite key:** a PK made of more than one column, e.g., `(prod_id, categ_id)` in a junction table.

| Relationship | How to model it | Example |
|---|---|---|
| **1–1** | FK with a `UNIQUE` constraint, or the same PK in both tables | `users` ↔ `user_profile` |
| **1–M** (M–1 from the other side) | FK on the "many" side | One user → many addresses (`addresses.user_id`) |
| **M–M** | **Junction (bridge) table** with two FKs | Products ↔ categories via `product_categ(prod_id, categ_id)` |

### Rule 2: ACID (see Part E for details)
Atomicity, Consistency, Isolation, Durability. These guarantees make OLTP data trustworthy.

### Rule 3: Normalization
**Goal:** reduce **redundancy** and prevent **anomalies**:
- **Update anomaly:** a user's phone number is stored in 50 order rows. You update 49 of them and the data now contradicts itself.
- **Insert anomaly:** you can't add a product until someone orders it, because products only exist inside order rows.
- **Delete anomaly:** deleting the last order for a product also deletes everything you knew about that product.

| Normal form | Rule | Violation example |
|---|---|---|
| **1NF** | Atomic values (one value per cell), no repeating groups, each row unique (has a PK) | `categories = "shoes, sale, summer"` in one cell |
| **2NF** | 1NF + every non-key column depends on the **whole** key (no partial dependency). This only matters for composite keys. | In `order_item(order_id, prod_id, product_name, qty)` with PK `(order_id, prod_id)`, `product_name` depends only on `prod_id`. (The final schema avoids this: `order_item` gets a surrogate `id`, and product details stay in `products`.) |
| **3NF** | 2NF + no **transitive** dependency: non-key columns depend only on the key, not on other non-key columns | In `orders(id, user_id, user_email, ...)`, `user_email` depends on `user_id`, not on the order `id` |
| BCNF (bonus) | Every determinant is a candidate key (a stricter 3NF) | Rare edge cases |

Memory trick: every column depends on **the key (1NF), the whole key (2NF), and nothing but the key (3NF)**.

### Example: flat tables → normalized schema
**Before:** three flat tables (`flat_user_data`, `flat_inventory_data`, `flat_order_data`). `flat_order_data` mixes user, address, coupon, product, line item, and payment in a single row. An order with 3 products repeats all of the order and payment data 3 times.

**After:**
```
users        (id PK, email, full_name, phone, created_at)
addresses    (id PK, user_id FK, city, province, country_code, postal_code, address_type)
categories   (id PK, name, parent_id FK NULL)   -- parent_id → categories.id (sub-categories)
products     (id PK, sku, description, base_price, qty_on_hand, qty_reserved, version)
product_categ(prod_id FK, categ_id FK)          -- junction table, M–M
orders       (id PK, user_id FK, shipping_add, coupon_code,
              discount_amt, tax_amt, shipping_amt, total_amt)
order_item   (id PK, order_id FK, prod_id FK, quantity, unit_price, line_total)
payments     (id PK, order_id FK, payment_provider, payment_method,
              payment_amount, payment_currency)
```
Relationships:

- `users 1–M addresses`: putting `user_id` in `addresses` lets one user have many addresses.
- `users 1–M orders`
- `products M–M categories` (through `product_categ`)
- `categories 1–M categories`: a self-reference through `parent_id` for sub-categories (walked with a recursive CTE, Part C §4)
- `orders 1–M order_item`
- `products 1–M order_item`
- `orders 1–M payments`: this allows split or retried payments.

### Why normalize, and the tradeoff
- **Pros:** less redundancy, which saves storage and memory; one place to update; integrity enforced by FKs.
- **Cons:** reads need more **joins**.
- **Tradeoff:** *denormalize* (add columns so you don't have to query another table) for read speed, or *normalize* for less redundancy and safer writes. OLTP leans toward normalized. OLAP leans toward denormalized.

---

## Part C — Advanced SQL

### 1. Logical execution order of SQL
SQL is written in one order and evaluated in another:
```
FROM / JOIN → WHERE → GROUP BY → HAVING → SELECT (incl. window functions) → DISTINCT → ORDER BY → LIMIT/OFFSET
```
Consequences:

- You **can't use a `SELECT` alias in `WHERE`**, because `WHERE` runs first.
- `WHERE` filters rows *before* grouping. `HAVING` filters groups *after* grouping.
- You **can't filter on a window function in `WHERE`**. Wrap the query in a CTE or subquery first.
- `ORDER BY` *can* use aliases, because it runs after `SELECT`.

### 2. Joins
| Join | Returns |
|---|---|
| `INNER JOIN` | Only matching rows from both sides |
| `LEFT JOIN` | All left rows; NULLs where the right side has no match |
| `RIGHT JOIN` | All right rows (usually rewritten as a `LEFT JOIN`) |
| `FULL OUTER JOIN` | All rows from both sides |
| `CROSS JOIN` | Cartesian product (every combination) |
| **Self join** | A table joined to itself (e.g., employee → manager) |

```sql
-- Users with their order count (including users with 0 orders)
SELECT u.id, u.email, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.email;

-- Anti-join: users who have NEVER ordered
SELECT u.*
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
WHERE o.id IS NULL;
```

### 3. Subqueries
| Type | Example |
|---|---|
| **Scalar** (returns one value) | `WHERE base_price > (SELECT AVG(base_price) FROM products)` |
| **`IN` / list** | `WHERE id IN (SELECT prod_id FROM order_item)` |
| **Correlated** (runs once per outer row; references the outer query) | see below |
| **Derived table** (subquery in `FROM`) | `FROM (SELECT ...) t` |
| **`EXISTS`** | Stops at the first match, and is safe with NULLs |

```sql
-- Correlated: products priced above the average of their own category
SELECT p.id, p.sku, p.base_price
FROM products p
JOIN product_categ pc ON pc.prod_id = p.id
WHERE p.base_price > (
    SELECT AVG(p2.base_price)
    FROM products p2
    JOIN product_categ pc2 ON pc2.prod_id = p2.id
    WHERE pc2.categ_id = pc.categ_id
);
```
**Gotcha:** `NOT IN (subquery)` returns **no rows** if the subquery returns any NULL. Prefer `NOT EXISTS`.

### 4. CTEs (Common Table Expressions)
`WITH name AS (...)` creates a named, temporary result set. It makes queries readable, lets one step build on another, and can be **recursive**.
```sql
WITH order_totals AS (
    SELECT order_id, SUM(line_total) AS items_total
    FROM order_item
    GROUP BY order_id
),
big_orders AS (
    SELECT * FROM order_totals WHERE items_total > 1000
)
SELECT o.id, o.user_id, b.items_total
FROM big_orders b
JOIN orders o ON o.id = b.order_id;
```
**Recursive CTE** for hierarchies (category tree, org chart):
```sql
WITH RECURSIVE tree AS (
    SELECT id, name, parent_id, 1 AS depth FROM categories WHERE parent_id IS NULL   -- anchor
    UNION ALL
    SELECT c.id, c.name, c.parent_id, t.depth + 1
    FROM categories c JOIN tree t ON c.parent_id = t.id                              -- recursive step
)
SELECT * FROM tree;
```

### 5. Window functions
A window function calculates across a set of rows related to the current row, **without collapsing rows** the way `GROUP BY` does.
- `PARTITION BY` splits the rows into groups. `ORDER BY` sets the order inside each group.
- Example: `ROW_NUMBER() OVER (PARTITION BY city ORDER BY salary DESC)` numbers employees within each city, highest salary first.

| Function | Use |
|---|---|
| `ROW_NUMBER()` | Unique sequence 1,2,3… (dedup, top-N per group) |
| `RANK()` / `DENSE_RANK()` | Ties share a rank; `RANK` leaves gaps (1,1,3), `DENSE_RANK` doesn't (1,1,2) |
| `LAG()` / `LEAD()` | Value from the previous / next row |
| `SUM() / AVG() OVER (...)` | Running totals, moving averages |

```sql
-- Top 3 most recent orders per user (window + CTE, since WHERE can't see window results)
WITH ranked AS (
    SELECT o.*, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
    FROM orders o
)
SELECT * FROM ranked WHERE rn <= 3;

-- Running total of spend per user
SELECT user_id, id, total_amt,
       SUM(total_amt) OVER (PARTITION BY user_id ORDER BY id) AS running_spend
FROM orders;
```

---

## Part D — Stored Procedures, Functions, and Triggers
Examples use PostgreSQL (PL/pgSQL).

| | **Function** | **Stored procedure** | **Trigger** |
|---|---|---|---|
| Called by | Inside SQL (`SELECT fn(x)`) | `CALL proc(...)` | The DB automatically, on `INSERT/UPDATE/DELETE` |
| Returns | A value or a table | Nothing (can use `INOUT` params) | Modified row (`NEW`) or nothing |
| Transaction control | ❌ Can't `COMMIT` | ✅ Can `COMMIT/ROLLBACK` (Postgres 11+) | Runs inside the caller's transaction |
| Use for | Reusable calculations | Multi-step business operations | Automatic side effects, audit, derived columns |

**Function:**
```sql
CREATE OR REPLACE FUNCTION order_items_total(p_order_id INT)
RETURNS NUMERIC LANGUAGE sql STABLE AS $$
    SELECT COALESCE(SUM(line_total), 0) FROM order_item WHERE order_id = p_order_id;
$$;

SELECT id, order_items_total(id) FROM orders;
```

**Stored procedure** that reserves stock without overselling:
```sql
CREATE OR REPLACE PROCEDURE reserve_stock(p_prod_id INT, p_qty INT)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE products
       SET qty_reserved = qty_reserved + p_qty
     WHERE id = p_prod_id
       AND qty_on_hand - qty_reserved >= p_qty;     -- check and update in ONE atomic statement
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Not enough stock for product %', p_prod_id;
    END IF;
END; $$;

CALL reserve_stock(10, 2);
```

**Trigger** that keeps a derived column correct:
```sql
CREATE OR REPLACE FUNCTION set_line_total() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.line_total := NEW.quantity * NEW.unit_price;
    RETURN NEW;
END; $$;

CREATE TRIGGER trg_order_item_line_total
BEFORE INSERT OR UPDATE ON order_item
FOR EACH ROW EXECUTE FUNCTION set_line_total();
```
- `BEFORE` triggers can change `NEW` before it is written. `AFTER` triggers suit audit logs and updates to other tables.
- `FOR EACH ROW` fires once per row. `FOR EACH STATEMENT` fires once per statement.

**Pros:** logic runs close to the data (fewer network round trips); rules are enforced for every client app; good for integrity and audit.
**Cons:** "hidden" logic that is hard to debug and version-control; harder to test; ties you to one database vendor; triggers add cost to **every** write (important for OLTP throughput); trigger chains can cascade.

---

## Part E — Transactions & Concurrency Control

### Transactions
```sql
BEGIN;
    INSERT INTO orders (...) VALUES (...);
    INSERT INTO order_item (...) VALUES (...);
    CALL reserve_stock(10, 2);
    INSERT INTO payments (...) VALUES (...);
COMMIT;      -- or ROLLBACK on any error
```
`SAVEPOINT sp1; ... ROLLBACK TO sp1;` undoes part of a transaction without abandoning all of it.

### ACID
| Property | Meaning | How the DB delivers it |
|---|---|---|
| **Atomicity** | The transaction is a single unit: all or nothing | Undo/rollback using the log |
| **Consistency** | After a transaction, all data satisfies the rules (PK, FK, `CHECK`, `UNIQUE`) | Constraints and triggers |
| **Isolation** | Concurrent transactions don't interfere with each other | Locks and **MVCC** (multi-version concurrency control) |
| **Durability** | Once committed, data survives a crash (it is on disk) | **Write-ahead log (WAL)** flushed to disk at commit |

### Concurrency problems
| Anomaly | What happens |
|---|---|
| **Dirty read** | You read another transaction's *uncommitted* change, which might later roll back |
| **Non-repeatable read** | You read the same row twice and get different values (someone committed an update in between) |
| **Phantom read** | You run the same range query twice and new rows appear (someone inserted) |
| **Lost update** | Two transactions read the same value, both update it, and the second overwrites the first |

### Isolation levels
| Level | Dirty | Non-repeatable | Phantom |
|---|---|---|---|
| Read Uncommitted | possible | possible | possible |
| **Read Committed** (Postgres default) | ✅ prevented | possible | possible |
| **Repeatable Read** (MySQL InnoDB default) | ✅ | ✅ | possible per the SQL standard (Postgres/InnoDB mostly prevent it) |
| **Serializable** | ✅ | ✅ | ✅ (may abort transactions, so you must retry) |

Higher isolation means safer data but less concurrency.

### Locking strategies
**Pessimistic locking** assumes a conflict will happen and locks the row first.
```sql
BEGIN;
SELECT qty_on_hand, qty_reserved FROM products WHERE id = 10 FOR UPDATE;   -- others wait on this row
UPDATE products SET qty_reserved = qty_reserved + 2 WHERE id = 10;
COMMIT;
```
- Safe and simple, but other users wait, it scales poorly under heavy traffic, and it risks deadlocks. Best suited to smaller applications. It also fits cases where conflicts are frequent and transactions are short.

**Optimistic locking** assumes conflicts are rare and uses a **version** column.
```sql
-- read: id=10, qty_reserved=4, version=3
UPDATE products
   SET qty_reserved = 6, version = version + 1
 WHERE id = 10 AND version = 3;
-- 0 rows updated → someone else changed it → reload and retry (or report a conflict)
```
- No locks are held while the user "thinks," so it scales well. Conflicts turn into retries. Common in web apps and ORMs.

**Deadlock:** transaction A locks row 1 and waits for row 2, while B locks row 2 and waits for row 1. The DB detects this and aborts one of them. **Prevention:** always lock rows in a consistent order (e.g., by ascending `id`), keep transactions short, and retry the aborted one.

**MVCC:** readers see a snapshot and **don't block writers** (and writers don't block readers). Old row versions are cleaned up later, by `VACUUM` in Postgres.

---

## Part F — Performance Tuning: Indexing & Query Optimization

### Physical storage
- Data is stored in **pages** (8 KB in Postgres, 16 KB in InnoDB). The DB always reads a **whole page**, never a single row.
- **Disk I/O is almost always the bottleneck.** Tuning mostly means reading fewer pages.

### B-Tree indexes
- A **balanced** tree: every leaf is at the same depth, so every lookup costs about the same.
- Each node is a page that holds **many keys** (high fan-out, often hundreds), so the tree stays **shallow**. Depth grows as log base *fan-out* of the row count, so millions of rows usually need only 3–4 levels.
- **Each level is roughly one page read (disk I/O).** A lookup costs about *depth* I/Os instead of scanning the whole table.
- **Leaves are linked in a doubly linked list** (strictly speaking, a B+Tree). This makes **range queries** fast: for `BETWEEN 100 AND 300`, find 100, then walk neighboring leaves until you pass 300. It also supports `ORDER BY` in either direction.

### Clustered vs non-clustered index
| | **Clustered** | **Non-clustered (secondary)** |
|---|---|---|
| Built on | Usually the **primary key** | Other columns (`email`, `user_id`, …) |
| Leaves contain | **The actual data rows** | Key + **pointer** (in InnoDB: the PK value → lookup in the clustered index) |
| Physical order | Table is **physically stored** in index order | Separate structure |
| How many | **Only 1** per table | Many |

Engine note: MySQL InnoDB always clusters on the PK. PostgreSQL stores tables as an unordered **heap**, so all its indexes are non-clustered. The `CLUSTER` command reorders the table once, and the order is not maintained.

**Using two indexed columns in one query**, e.g., `WHERE city = 'HN' AND age > 30`:
- **Index filter:** estimate which index narrows the rows down more (is more **selective**), use it to load candidate rows into RAM, then check the other condition in memory.
- **Index merge / intersection:** scan both B-Trees and **intersect** the row pointers. Used when the two conditions are similarly selective. Postgres calls this a *Bitmap AND*.
- Often better: a **composite index** `(city, age)`.

### More index types and rules
- **Composite index `(a, b, c)`:** follows the **leftmost-prefix rule**. It helps `a`, `a,b`, and `a,b,c`, but not `b` alone. Put equality columns first, then range columns.
- **Covering index:** includes every column the query needs (`CREATE INDEX ... ON orders(user_id) INCLUDE (total_amt)`), so the DB answers from the index alone (**index-only scan**) with no table lookup.
- **Unique index:** enforces uniqueness (`email`, `sku`).
- **Partial index:** indexes only some rows (`WHERE status = 'pending'`).
- **Index FK columns** (`orders.user_id`, `order_item.order_id`). They are used in joins, and many databases don't index them automatically.
- **Indexes have a cost:** every `INSERT/UPDATE/DELETE` must also update every index, and indexes use disk and memory. Don't index everything. This matters a lot for write-heavy OLTP.
- **Low-cardinality columns** (e.g., `is_active` true/false) rarely benefit from a B-Tree index.

### Query optimization
**Read the execution plan:** `EXPLAIN` shows the estimated plan, and `EXPLAIN ANALYZE` runs the query and shows actual times.
- `Seq Scan`: reads the whole table. Fine for small tables, a red flag on big ones.
- `Index Scan` / `Index Only Scan` / `Bitmap Heap Scan`
- Join algorithms: `Nested Loop` (small inputs), `Hash Join` (large, equality), `Merge Join` (sorted inputs)
- A big gap between estimated and actual row counts means statistics are stale. Run `ANALYZE`.

**Common fixes:**
| Problem | Fix |
|---|---|
| `SELECT *` | Select only needed columns (enables covering indexes, less I/O) |
| Function on an indexed column: `WHERE LOWER(email) = ...`, `WHERE EXTRACT(YEAR FROM created_at) = 2026` | Keep the column bare (**sargable**): `created_at >= '2026-01-01' AND created_at < '2027-01-01'`, or use an expression index |
| Leading wildcard `LIKE '%abc'` | Can't use a B-Tree; use full-text search or a trigram index |
| Implicit type cast (comparing an `int` column to `'42'`) | Match the types |
| Large `OFFSET` pagination | **Keyset pagination:** `WHERE id > :last_id ORDER BY id LIMIT 50` |
| N+1 queries from the app (one query per row) | A single `JOIN` or `WHERE id IN (...)` |
| `NOT IN` with NULLs, slow correlated subqueries | `NOT EXISTS`, or rewrite as a join or CTE |
| Huge tables | **Partitioning** (by date or range), archiving old data |
| Long transactions | Keep them short; they hold locks and block `VACUUM` |

---

## Key Terms Cheat Sheet
- **OLTP / OLAP:** write-optimized transactional system vs read-optimized analytical system
- **PK / FK / composite key / junction table:** identity, reference, multi-column key, M–M bridge
- **1NF / 2NF / 3NF:** atomic values / whole key / nothing but the key
- **Update/insert/delete anomaly:** problems caused by redundancy
- **Denormalization:** adding redundancy on purpose for read speed
- **Logical execution order:** FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT
- **Correlated subquery:** re-evaluated for each outer row
- **CTE / recursive CTE:** named temporary result / hierarchy traversal
- **Window function:** calculation over a partition without collapsing rows
- **Function vs procedure vs trigger:** returns a value / multi-step `CALL` with transaction control / runs automatically on DML
- **ACID:** atomicity, consistency, isolation, durability
- **WAL:** write-ahead log, which provides durability
- **MVCC:** snapshots so readers and writers don't block each other
- **Dirty / non-repeatable / phantom read, lost update:** concurrency anomalies
- **Pessimistic vs optimistic locking:** `SELECT ... FOR UPDATE` vs a version column
- **Deadlock:** a circular wait; fix with consistent lock order
- **Page:** 8–16 KB unit of disk I/O
- **B-Tree / B+Tree:** shallow balanced index with linked leaves for range scans
- **Clustered vs non-clustered index:** data in the leaves (1 per table) vs pointer to the data (many)
- **Composite, covering, partial index:** multi-column, answers the query alone, subset of rows
- **Sargable:** a predicate that can use an index
- **EXPLAIN ANALYZE:** shows the actual execution plan

---

## Practice Questions

1. **What is OLTP optimized for, and what is OLAP optimized for?**
   OLTP handles fast, frequent write transactions (CRUD, procedures, functions). OLAP handles heavy read queries for reporting.

2. **What can happen if you run OLAP-style queries on the OLTP system?**
   The heavy queries can lock data the application needs, so its transactions wait, time out, or fail.

3. **A table has `categories = "shoes, sale"`. Which normal form does it break, and how do you fix it?**
   1NF (the value isn't atomic). Create `categories` and a junction table `product_categ(prod_id, categ_id)`.

4. **In `order_item(order_id, prod_id, product_name, qty)` with PK `(order_id, prod_id)`, what's wrong?**
   A 2NF violation: `product_name` depends only on `prod_id`, which is part of the key. Move it to `products`.

5. **How do you model "a user can have many addresses" and "a product can be in many categories"?**
   1–M: put `user_id FK` in `addresses`. M–M: use a junction table `product_categ(prod_id, categ_id)`.

6. **Why does `SELECT price * qty AS total FROM t WHERE total > 100` fail?**
   `WHERE` runs before `SELECT`, so the alias doesn't exist yet. Repeat the expression, or wrap the query in a CTE or subquery.

7. **Write a query to find users who have never placed an order.**
   `LEFT JOIN orders` and filter with `WHERE o.id IS NULL`, or use `WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)`.

8. **Why can `NOT IN (subquery)` return no rows unexpectedly?**
   If the subquery returns any NULL, every comparison becomes UNKNOWN. Use `NOT EXISTS`.

9. **Get the highest-paid employee in each city.**
   ```sql
   WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY salary DESC) rn FROM employees)
   SELECT * FROM r WHERE rn = 1;
   ```
   Use `RANK()` if ties should all be returned.

10. **`ROW_NUMBER` vs `RANK` vs `DENSE_RANK` for salaries 100, 100, 90?**
    1,2,3 / 1,1,3 / 1,1,2.

11. **Function vs stored procedure vs trigger: when would you use each?**
    A function for a reusable calculation inside SQL. A procedure for a multi-step operation that needs transaction control. A trigger for automatic side effects on writes (audit logs, derived columns).

12. **Name two downsides of heavy trigger use in OLTP.**
    They hide logic, which makes debugging harder, and they add cost to every write, which lowers throughput. Chained triggers can also cascade unexpectedly.

13. **Explain each ACID property using a checkout transaction.**
    Atomicity: the order, items, stock reservation, and payment all commit, or none do. Consistency: FKs and `CHECK`s hold (e.g., stock can't go negative). Isolation: two shoppers buying the last item don't corrupt each other's transaction. Durability: once "Order placed" shows, the order survives a crash because the WAL is on disk.

14. **Two admins edit the same product at the same time and one change disappears. What is this called, and give two fixes.**
    A lost update. Fix with pessimistic locking (`SELECT ... FOR UPDATE`) or optimistic locking (a `version` column, update `WHERE version = n`, and retry when 0 rows are updated).

15. **When would you choose optimistic over pessimistic locking?**
    When conflicts are rare and throughput matters, such as web apps where users hold data for a long time before saving. Choose pessimistic when conflicts are frequent and transactions are short.

16. **What causes a deadlock and how do you prevent it?**
    Two transactions each hold a lock the other needs. Lock rows in a consistent order, keep transactions short, and retry the aborted transaction.

17. **Why are B-Tree lookups fast, and why are leaves linked?**
    Nodes hold many keys, so the tree is only a few levels deep, and each level is about one page read. Linked leaves let range queries (`BETWEEN`, `ORDER BY`) walk neighboring leaves without climbing back up the tree.

18. **Clustered vs non-clustered index: how many of each can a table have?**
    One clustered index (the table's physical order, with data in the leaves). Many non-clustered indexes (their leaves point to the row or PK).

19. **You have an index on `(last_name, first_name)`. Which queries can use it?**
    `WHERE last_name = ?` and `WHERE last_name = ? AND first_name = ?`. Not `WHERE first_name = ?` alone (leftmost-prefix rule).

20. **Why might `WHERE EXTRACT(YEAR FROM created_at) = 2026` be slow, and how do you rewrite it?**
    The function on the column prevents index use (not sargable). Rewrite as `created_at >= '2026-01-01' AND created_at < '2027-01-01'`.

21. **Why not just index every column?**
    Every write must update every index, which slows `INSERT/UPDATE/DELETE`, and indexes use disk and memory. Low-selectivity columns gain little from an index.

22. **Pagination with `OFFSET 100000` is slow. Why, and what's the fix?**
    The DB still reads and throws away 100,000 rows. Use keyset pagination: `WHERE id > :last_id ORDER BY id LIMIT 50`.
