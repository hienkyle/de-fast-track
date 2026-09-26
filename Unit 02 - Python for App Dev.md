# Unit 02 — Python for App Dev: Study Guide

**Scope:** Python fundamentals (data types, control flow, functions) · OOP in Python · Error handling, modules, and packaging · Lecture topics: backend architecture, REST APIs, DI/IoC, security (JWT, OAuth2, RBAC, CORS), API design for production, and scaling to large data.

---

## Part A — Python Fundamentals

### 1. Data types

| Type | Example | Mutable? | Notes |
|---|---|---|---|
| `int`, `float`, `complex` | `42`, `3.14`, `2+3j` | No | `int` has arbitrary precision; beware float rounding (`0.1 + 0.2 != 0.3`) |
| `bool` | `True`, `False` | No | Subclass of `int` (`True == 1`) |
| `str` | `"data"` | No | Unicode; slicing `s[1:4]`; f-strings `f"{x:.2f}"` |
| `bytes` | `b"\x00\xff"` | No | Raw binary (files, network payloads) |
| `list` | `[1, 2, 3]` | Yes | Ordered; O(1) append, O(n) search |
| `tuple` | `(1, 2)` | No | Hashable if contents are; good for fixed records |
| `dict` | `{"id": 1}` | Yes | Key → value; O(1) average lookup; insertion-ordered (3.7+) |
| `set` / `frozenset` | `{1, 2}` | Yes / No | Unique items; O(1) membership; set algebra `| & -` |
| `None` | `None` | — | Absence of value; compare with `is None` |

**Key ideas**
- **Mutable vs immutable:** mutating a list passed into a function changes the caller's list. Never use a mutable default argument:
  ```python
  def add(item, bucket=None):   # correct
      if bucket is None:        # not `bucket or []`, which would swap a caller's empty list for a new one
          bucket = []
      bucket.append(item)
      return bucket
  ```
- **`==` vs `is`:** `==` compares value, `is` compares identity.
- **Truthiness:** `0`, `""`, `[]`, `{}`, `None` are falsy.
- **Type hints** (`def f(x: int) -> str:`) are not enforced at runtime but power IDEs, `mypy`, and frameworks like FastAPI/Pydantic.
- **Comprehensions:** `[x*2 for x in rows if x > 0]`, `{k: v for k, v in pairs}`, generator `(x for x in rows)` — generators are lazy and memory-efficient (important for big data).

### 2. Control flow
- `if / elif / else`; ternary `a if cond else b`
- `for` over any iterable; `while` with `break` / `continue`; `for ... else` (runs if no `break`)
- `match` (3.10+) for structural pattern matching:
  ```python
  match event:
      case {"type": "click", "x": x}: handle_click(x)
      case _: ignore()
  ```
- Useful built-ins:
  - `enumerate(items)`: loops with an index and the item together
  - `zip(a, b)`: pairs up items from several iterables
  - `range(start, stop, step)`: produces a sequence of integers
  - `sorted(items, key=...)`: returns a new sorted list, using `key` to choose what to sort by
  - `any(items)` / `all(items)`: `True` if at least one / every item is truthy
  - `map(fn, items)`: applies a function to every item
  - `filter(fn, items)`: keeps only the items where the function returns `True`

### 3. Functions
- Parameters:
  - **Positional:** matched by order (`f(1, 2)`)
  - **Keyword:** matched by name (`f(a=1, b=2)`)
  - **Default:** used when no value is passed (`def f(a, b=10)`)
  - **`*args`:** collects any extra positional arguments into a tuple
  - **`**kwargs`:** collects any extra keyword arguments into a dict
  - **Keyword-only:** anything after `*` must be passed by name (`def f(a, *, b)`)
- **Closures:** an inner function that remembers variables from the function that created it, even after that function has returned (e.g., `make_multiplier(3)` returns a function that always multiplies by 3). **Lambda:** a small, one-expression function with no name, often used as a `key` or callback (`sorted(rows, key=lambda r: r["age"])`).
- **Decorators** wrap functions (used heavily for routes, auth, retries, logging):
  ```python
  import functools, time
  def timed(fn):
      @functools.wraps(fn)
      def wrapper(*a, **kw):
          start = time.perf_counter()
          try:
              return fn(*a, **kw)
          finally:
              print(f"{fn.__name__} took {time.perf_counter()-start:.3f}s")
      return wrapper
  ```
- **Generators** (`yield`) stream data one item at a time — process a 10 GB file without loading it into memory.

---

## Part B — OOP in Python

### Core concepts
| Concept | Meaning | Python mechanism |
|---|---|---|
| Encapsulation | Bundle data + behavior; hide internals | `_protected`, `__private` (name mangling), `@property` |
| Inheritance | Reuse/extend a parent class | `class Child(Parent)`, `super()` |
| Polymorphism | Same interface, different behavior | Method overriding, duck typing |
| Abstraction | Define *what*, not *how* | `abc.ABC`, `@abstractmethod`, `typing.Protocol` |

```python
from abc import ABC, abstractmethod

class Storage(ABC):                     # interface / abstraction
    @abstractmethod
    def save(self, record: dict) -> None: ...

class PostgresStorage(Storage):
    def __init__(self, dsn: str):
        self._dsn = dsn                 # encapsulated
    def save(self, record):
        print("INSERT", record)

class S3Storage(Storage):
    def save(self, record):
        print("PUT object", record)
```

### Things to know
- **`__init__`:** the initializer that runs when an object is created and sets its starting attributes.
- **`self`:** refers to the current object, so a method can reach that object's own data.
- **Class attributes:** defined on the class and shared by every object (e.g., a default `table_name`).
- **Instance attributes:** set on `self` and unique to each object (e.g., each connection's `dsn`).
- **Composition over inheritance:** prefer "has-a" (inject a `Storage`) over deep "is-a" trees.
- **SOLID** — especially *Dependency Inversion*: depend on abstractions (`Storage`), not concrete classes (`PostgresStorage`). This leads directly to DI (Part D).

---

## Part C — Error Handling, Modules, and Packaging

### Error handling
```python
try:
    value = int(raw)
except ValueError as e:
    log.warning("bad input", extra={"raw": raw})
    raise InvalidInput("value must be an integer") from e
else:
    ...          # runs only if no exception
finally:
    ...          # always runs — cleanup
```
- Catch **specific** exceptions; never bare `except:`.
- Create a **custom exception hierarchy** (`class AppError(Exception)`, `class NotFound(AppError)`) so API layers can map them to status codes.
- `raise ... from e` preserves the root cause.
- **Context managers (`with`)** guarantee cleanup — the Pythonic fix for *resource leaks* (see Part E):
  ```python
  with open("data.csv") as f, db.connect() as conn:
      ...
  ```
- EAFP ("easier to ask forgiveness") is idiomatic Python vs LBYL ("look before you leap").

### Modules and packages
- **Module** = one `.py` file. **Package** = directory of modules (with `__init__.py`).
- Imports: `import pkg.mod`, `from pkg.mod import name`; relative `from .utils import x`.
- `if __name__ == "__main__":` → code runs only when executed directly.
- `sys.path` determines where Python looks for imports.
- Avoid circular imports by restructuring or importing inside functions.

### Packaging & environments
- **Virtual environments** isolate dependencies: `python -m venv .venv`, or tools like `uv`, `poetry`.
- **Dependencies:** `requirements.txt` (pinned) or `pyproject.toml` (modern standard).
- **Build/distribute:** `pyproject.toml` + build backend → `python -m build` → wheel (`.whl`) → `pip install` / publish to PyPI.
- Typical project layout:
  ```
  my_app/
  ├── pyproject.toml
  ├── src/my_app/
  │   ├── __init__.py
  │   ├── api/  services/  repositories/  models/
  └── tests/
  ```

---

## Part D — Backend Architecture & APIs

### 1. Backend architecture styles
| Style | Description | Pros | Cons |
|---|---|---|---|
| **Monolith** | One codebase, one deployable | Simple to build, test, deploy | Hard to scale parts independently; grows tangled |
| **Modular monolith** | One deployable, but split into well-bounded modules | Clean boundaries without distributed complexity; easy path to microservices | Needs discipline to keep module boundaries |
| **Microservices** | Many small independently deployed services | Independent scaling/deploys, team autonomy | Network failures, distributed tracing, data consistency, ops overhead |

### 2. RESTful API
REST = standard, resource-oriented convention for client–server communication over HTTP.

| Method | Purpose | Idempotent? |
|---|---|---|
| GET | Read | Yes |
| POST | Create | **No** |
| PUT | Replace | Yes |
| PATCH | Partial update | Not guaranteed |
| DELETE | Remove | Yes |

- **Path parameter** → identifies **a single resource**: `GET /users/42`
- **Query parameter** → **filtering, sorting, pagination**: `GET /users?role=admin&sort=-created_at&page=2&limit=50`
- **Sending files via POST** → body type is **form data** (`multipart/form-data`), not JSON.
- Know your data: be familiar with every field, its type, and format (dates, IDs, enums) the API accepts and returns.
- **API documentation** tools depend on language/framework — e.g., FastAPI auto-generates OpenAPI/Swagger at `/docs`; Django REST uses drf-spectacular; Java Spring uses springdoc; Node uses swagger-jsdoc.

Common status codes: `200 OK`, `201 Created`, `204 No Content`, `400 Bad Request`, `401 Unauthorized` (not authenticated), `403 Forbidden` (no permission), `404 Not Found`, `409 Conflict`, `422 Unprocessable Entity` (validation), `429 Too Many Requests`, `500 Internal Server Error`, `503 Service Unavailable`.

### 3. DI (Dependency Injection) and IoC (Inversion of Control)
- **IoC:** the *framework* controls object creation and flow, not your code.
- **DI:** a form of IoC — a class **receives** its dependencies instead of creating them. In class terms: **"DI = call the interface"**, not the concrete implementation.
```python
class UserService:
    def __init__(self, repo: UserRepository):   # depends on an interface
        self.repo = repo

service = UserService(PostgresUserRepo())       # production
service = UserService(FakeUserRepo())           # tests — easy to swap
```
FastAPI example: `def get_user(db: Session = Depends(get_db)): ...`
**Why:** loose coupling, easy testing/mocking, swap implementations without code changes.

---

## Part E — Building Production-Grade APIs

### 1. API contract and versioning
- **Contract** = the agreed request/response shape (fields, types, status codes, errors), usually documented in OpenAPI.
- **Versioning** ensures each client gets the correct data for the version it was built against.
- When the API changes in a breaking way, publish a new version (`/api/v1/...` → `/api/v2/...`). **Old versions are kept for legacy clients** until deprecated.
- Other styles: header (`Accept: application/vnd.app.v2+json`) or query param (`?version=2`).

### 2. Idempotency & retry safety
- **Idempotent** = doing the request N times has the same effect as once.
- Problem: concurrent users, double-clicks, or client retries can cause duplicate orders/records.
- Solution: **check whether the request has already been processed** or the data is a duplicate:
  - Client sends an **`Idempotency-Key`** header; server stores key → result and returns the stored result on repeats.
  - Use DB **unique constraints** / upserts (`INSERT ... ON CONFLICT DO NOTHING`).

### 3. Partial failure in batches
When inserting thousands of records and some fail:
| Strategy | Behavior | Use when |
|---|---|---|
| **All-or-nothing** | Wrap in one transaction; any failure → rollback all | Financial data, strong consistency needed |
| **Partial success** | Commit valid rows, report failures per item (often `207 Multi-Status` or a result list) | Bulk imports, ingestion pipelines |

Techniques for large inserts: chunk into batches (e.g., 1,000 rows), use bulk APIs (`executemany`, `COPY`), send bad records to a **dead-letter/quarantine table**, make batches idempotent so retries are safe.

### 4. Error design: human vs machine
A good error response serves both:
- **Consistent status code** (machine)
- **Stable error code** (machine) and **human-readable message** (human)
- **`details` array** showing exactly which fields failed
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Some fields are invalid.",
    "details": [
      {"field": "email", "issue": "missing"},
      {"field": "age", "issue": "must be >= 0"}
    ],
    "request_id": "a1b2c3"
  }
}
```

### 5. Dependencies & resource leaks
- **When you open a resource, close it** — DB connections, files, sockets, HTTP sessions, cursors.
- Use `with` / `try...finally`, connection pools, and framework dependencies with cleanup (`yield` in FastAPI `Depends`).
- Leaks lead to exhausted connection pools, "too many open files," and memory growth.

### 6. Timeouts & retries
- **Always set timeouts** on outbound calls (`requests.get(url, timeout=5)`) — otherwise one slow service can hang yours.
- **Retry** transient failures (timeouts, `502/503/504`, `429`) — not other `4xx` client errors (`400`, `401`, `403`, `404`, `422`), which will fail the same way every time.
- Use **exponential backoff + jitter**, cap the number of attempts, and only retry **idempotent** operations (or ones protected by an idempotency key).
- Related: **circuit breaker** stops calling a service that keeps failing.

### 7. Tracing and logging
- A system's **log is critical** for debugging and auditing.
- Pass a **`request_id` / `correlation_id`** through every call so one user request can be followed across components. `correlation_id` is especially common in **microservices**, where one request touches many services.
- Logging can be **active** (your code writes logs) or **passive** (other services/agents capture them — e.g., proxies, sidecars, APM tools).
- Logs should be **structured** (JSON with fields like `timestamp`, `level`, `request_id`, `user_id`) and have a **retention policy** that removes old logs.

### 8. Security
**JWT (JSON Web Token)**
- Format: **`header.payload.signature`** (each Base64URL-encoded)
  - **Header:** algorithm and type (`{"alg": "HS256", "typ": "JWT"}`)
  - **Payload (claims):** user/session data and timing — `sub` (user id), `role`, `iat` (issued at), **`exp` (expiry/timeout)**
  - **Signature:** `HMAC(secret, header + "." + payload)` — proves the token wasn't tampered with
- Payload is **encoded, not encrypted** → never put secrets in it.
- Stateless: server verifies signature + `exp` without a DB lookup. Common pattern: short-lived access token + longer-lived refresh token.

**OAuth2**
- Authorization framework letting an app access resources **on behalf of a user** without seeing their password ("Log in with Google").
- Roles: resource owner, client, authorization server, resource server.
- Common flows: Authorization Code (+ PKCE) for user apps; Client Credentials for service-to-service.
- OAuth2 often issues JWTs as access tokens.

**RBAC (Role-Based Access Control)** — case study
- Store **roles in the DB and map them to users** (`users`, `roles`, `user_roles`, optionally `permissions`/`role_permissions`).
- Check the role/permission on each endpoint (e.g., a decorator or dependency). Return `403` when authenticated but not allowed.

**CORS (Cross-Origin Resource Sharing)** — case study
- Browsers block JS from calling a different origin (scheme + domain + port) unless the server allows it.
- Server replies with headers like `Access-Control-Allow-Origin`; non-simple requests trigger a **preflight `OPTIONS`** request.
- Allow-list specific origins; avoid `*` with credentials.

Other basics: HTTPS everywhere, hash passwords (bcrypt/argon2), validate all input, parameterized SQL (prevents SQL injection), rate limiting, keep secrets in env vars/secret managers.

### 9. Testing
- **Unit tests** (`pytest`) for functions/classes — use DI to inject fakes/mocks.
- **Integration tests** against a real DB/service (e.g., in Docker).
- **API tests** with a test client (`fastapi.testclient`, `httpx`) checking status codes, contract, and error format.
- Test edge cases: duplicates (idempotency), partial batch failures, timeouts, auth failures (401/403).

---

## Part F — Handling Lots of Data (DE perspective)

Big questions from lecture:
1. **How do we handle a lot of data?** Stream/process in chunks (generators, batching), parallelize, push work to the database/engine instead of Python loops.
2. **How do we query a lot of data efficiently?** Indexes, pagination (prefer **keyset/cursor pagination** over large `OFFSET`), select only needed columns, partitioning, caching, columnar warehouses for analytics.
3. **How do we store a lot of data efficiently?** Columnar formats (Parquet), compression, partitioning by date/key, choosing OLTP vs OLAP storage, retention/archival policies.

**"DE = data executioner":** the data engineer **builds pipelines** that centralize data:
**Crawl/ingest → Normalize/clean/transform → Distribute/serve** (to warehouses, APIs, dashboards, ML).
Everything in Part E (idempotency, batch failures, retries, logging) applies directly to pipelines.

---

## Key Terms Cheat Sheet
- **Mutable/immutable** — can/can't change in place
- **Generator** — lazy iterator using `yield`
- **Decorator** — function that wraps another function
- **Context manager** — `with` block guaranteeing cleanup
- **Monolith / modular monolith / microservices** — deployment/architecture styles
- **Path vs query param** — single resource vs filter/sort/paginate
- **DI / IoC** — inject dependencies via interfaces; framework controls creation
- **Idempotency** — repeated requests have same effect as one
- **Correlation ID** — ID traced across services for one request
- **JWT** — `header.payload.signature`, carries session data + expiry
- **OAuth2** — delegated authorization framework
- **RBAC** — permissions through roles mapped to users
- **CORS** — browser cross-origin permission mechanism

---

## Practice Questions

1. **Why is `def f(x=[])` a bug? How do you fix it?**
   The default list is created once and shared by every call, so data leaks between calls. Use `x=None` and create the list inside: `if x is None: x = []`. (Avoid `x = x or []`: it replaces a caller's empty list with a new one.)


2. **What's the difference between a list comprehension and a generator expression? When would you use each for a 50 GB file?**
   A list comprehension builds the whole list in memory; a generator produces items one at a time. For a 50 GB file, use a generator.


3. **Write a `@retry(times=3)` decorator with exponential backoff.**
   ```python
   def retry(times=3, base=0.5):
       def deco(fn):
           @functools.wraps(fn)
           def wrapper(*a, **kw):
               for attempt in range(times):
                   try:
                       return fn(*a, **kw)
                   except TransientError:
                       if attempt == times - 1:
                           raise
                       time.sleep(base * 2 ** attempt)
           return wrapper
       return deco
   ```


4. **Explain encapsulation, inheritance, polymorphism, and abstraction with a data-pipeline example.**
   Abstraction: a `Storage` interface with `save()`. Inheritance: `PostgresStorage` and `S3Storage` extend it. Polymorphism: the pipeline calls `storage.save()` without knowing which one it has. Encapsulation: each class hides its own connection details (`self._dsn`).


5. **What do `else` and `finally` do in a `try` block?**
   `else` runs only if no exception was raised; `finally` always runs and is used for cleanup.


6. **Difference between a module and a package? What is `pyproject.toml` for?**
   A module is one `.py` file; a package is a folder of modules. `pyproject.toml` declares the project's metadata, dependencies, and build settings.


7. **Compare monolith, modular monolith, and microservices. Which would you pick for a 3-person startup, and why?**
   Usually a modular monolith: simple to build and deploy, with clean module boundaries so parts can be split into services later.


8. **Design the endpoint to get order #17, and one to list a user's orders filtered by status, sorted by date, page 3.**
   `GET /orders/17` (path parameter) and `GET /users/{id}/orders?status=shipped&sort=-created_at&page=3` (query parameters).


9. **What content type do you use to upload a CSV via POST?**
    `multipart/form-data` (form data).


10. **Explain DI in one sentence. How does it make testing easier?**
    A class receives its dependencies through an interface instead of creating them, so tests can pass in a fake or mock.


11. **What are the three parts of a JWT, and what's in each? Is the payload secret?**
    Header (algorithm, type), payload (claims such as user ID, role, `iat`, `exp`), and signature (proves it wasn't changed). The payload is only Base64-encoded, so anyone can read it; it is not secret.


12. **401 vs 403 — what's the difference?**
    401: the server doesn't know who you are (not logged in or bad token). 403: it knows who you are, but you aren't allowed.


13. **How do you prevent a user double-clicking "Pay" from creating two charges?**
    Send an idempotency key with the request and store processed keys, plus a unique constraint in the database.


14. **You're inserting 100,000 rows and row 52,310 fails. Describe both batch strategies and when you'd pick each.**
    All-or-nothing: one transaction, roll back everything — for data that must stay consistent, like payments. Partial success: commit valid rows and report or quarantine the failed ones — for bulk imports and pipelines.


15. **Design an error response for a signup request missing `email` and `password`.**
    ```json
    {"error": {"code": "VALIDATION_ERROR", "message": "Some required fields are missing.",
      "details": [{"field": "email", "issue": "missing"}, {"field": "password", "issue": "missing"}]}}
    ```
    Returned with status `400` or `422`.


16. **Which errors should be retried, and why must retries be combined with idempotency?**
    Retry temporary errors: timeouts, `429`, `502`, `503`, `504`. Don't retry other `4xx` errors. Without idempotency, a retried request that actually succeeded the first time creates duplicate data.


17. **What is a correlation ID and why does it matter more in microservices?**
    An ID attached to a request and passed along to every service it touches. In microservices one request crosses many services, so it's the only way to connect their logs.


18. **A frontend at `app.example.com` can't call `api.example.com`. What's happening and how do you fix it?**
    The browser blocks it under CORS because it's a different origin. The API must return `Access-Control-Allow-Origin: https://app.example.com` and handle the preflight `OPTIONS` request.


19. **Name three techniques each for querying and storing large datasets efficiently.**
    Querying: indexes, keyset/cursor pagination, selecting only needed columns (also partitioning, caching). Storing: columnar formats like Parquet, compression, partitioning by date/key (also retention policies).
