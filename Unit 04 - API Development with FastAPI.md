# Unit 04 — API Development with FastAPI: Study Guide

**Scope:** HTTP methods & RESTful API principles · Building a CRUD API with FastAPI · Data validation with Pydantic · Connecting the API to a relational database

The examples use the Unit 3 e-commerce schema (`users`, `products`, `orders`). The code is written for FastAPI with **Pydantic v2** and **SQLAlchemy 2.0**. **Naming note:** like Unit 3, this unit uses `prod_id`. Units 6, 7 and 12 call it `product_id`.

---

## Part A — HTTP & RESTful API Principles

### 1. Anatomy of an HTTP exchange

```
REQUEST                                     RESPONSE
POST /api/v1/products?notify=true HTTP/1.1  HTTP/1.1 201 Created
Host: shop.example.com                      Content-Type: application/json
Content-Type: application/json              Location: /api/v1/products/42
Authorization: Bearer <token>
                                            {"id": 42, "sku": "SH-001", ...}
{"sku": "SH-001", "base_price": 59.9}
```

| Part | What it holds |
|---|---|
| **Method** | The action (GET, POST, …) |
| **Path** | Which resource (`/products/42`); path parameters identify it |
| **Query string** | Options: filters, sort, pagination (`?category=shoes&limit=20`) |
| **Headers** | Metadata: `Content-Type`, `Authorization`, `Accept`, caching |
| **Body** | The data, usually JSON. GET and DELETE normally have no body. |
| **Status code** | The outcome (see below) |

### 2. HTTP methods

| Method | Purpose | Safe? | Idempotent? | Typical success code |
|---|---|---|---|---|
| **GET** | Read a resource or a list | ✅ | ✅ | 200 |
| **POST** | Create a new resource (or trigger an action) | ❌ | ❌ | 201 (+ `Location` header) |
| **PUT** | **Replace** a resource completely | ❌ | ✅ | 200 or 204 |
| **PATCH** | **Partially** update a resource | ❌ | not guaranteed | 200 |
| **DELETE** | Remove a resource | ❌ | ✅ | 204 |
| HEAD | Same as GET but returns headers only | ✅ | ✅ | 200 |
| OPTIONS | Which methods are allowed (used in the CORS preflight) | ✅ | ✅ | 204 / 200 |

- **Safe:** the request doesn't change server state.
- **Idempotent:** sending the same request once or ten times leaves the server in the **same state**. Call `DELETE /products/42` twice: the first call returns 204 and the second returns 404. The product is gone either way, so DELETE is idempotent. Calling `POST /orders` twice creates **two orders**, so POST is not.
- **Why it matters:** clients and proxies can safely **retry** idempotent requests after a timeout. To make a POST safe to retry, use an **idempotency key** header: the server stores the key and returns the original result when the same key comes in again.
- **PUT vs PATCH:** PUT sends the *whole* object, and any missing fields are reset or rejected. PATCH sends only the fields that change.

### 3. Status codes

| Range | Meaning | Common codes |
|---|---|---|
| **2xx** Success | It worked | `200 OK`, `201 Created`, `204 No Content` |
| **3xx** Redirect | Look elsewhere | `301 Moved Permanently`, `304 Not Modified` (cache) |
| **4xx** Client error | Caller did something wrong | `400 Bad Request`, `401 Unauthorized` (not logged in), `403 Forbidden` (logged in, no permission), `404 Not Found`, `405 Method Not Allowed`, `409 Conflict` (duplicate / version clash), `422 Unprocessable Entity` (validation failed; FastAPI's default), `429 Too Many Requests` |
| **5xx** Server error | Server failed | `500 Internal Server Error`, `502 Bad Gateway`, `503 Service Unavailable` |

Rule of thumb: **with a 4xx, the client should fix the request. With a 5xx, the server has to fix itself.** Never return 200 with `{"error": ...}` in the body.

### 4. REST constraints

REST (Representational State Transfer) is an architectural style, not a protocol.

| Constraint | Meaning |
|---|---|
| **Client–server** | The UI and the data storage are separate and can change independently |
| **Stateless** | Each request carries everything it needs (e.g., the auth token). The server keeps no session memory between requests, so any server instance can handle any request, which makes horizontal scaling easy. |
| **Cacheable** | Responses say whether they can be cached (`Cache-Control`, `ETag`) |
| **Uniform interface** | URIs identify resources, clients work with them through representations (JSON), each message describes itself, and hypermedia links (HATEOAS) are optional |
| **Layered system** | The client can't tell whether it's talking to the server, a load balancer, or a cache |
| **Code on demand** (optional) | The server can send executable code (rare in APIs) |

### 5. Resource design best practices

| Do | Don't |
|---|---|
| Nouns, plural: `/products`, `/orders/7` | Verbs: `/getProducts`, `/deleteOrder?id=7` |
| Let the method carry the action: `DELETE /orders/7` | `POST /orders/7/delete` |
| Nest to show ownership (1–2 levels max): `/users/5/orders` | `/users/5/orders/7/items/3/product/category` |
| Query parameters for filtering, sorting, and paging: `/products?category=shoes&sort=-price&limit=20` | A new endpoint for every filter |
| Version the API: `/api/v1/...` | Breaking changes without a new version |
| Consistent JSON (pick `snake_case` or `camelCase`) and one error format | A different format for each endpoint |
| Paginate lists (limit/offset, or keyset pagination from Unit 3) | Return 1 million rows |

### 6. CRUD ↔ HTTP ↔ SQL

| CRUD | HTTP | Endpoint | SQL |
|---|---|---|---|
| Create | POST | `/products` | `INSERT` |
| Read (list) | GET | `/products` | `SELECT ... LIMIT` |
| Read (one) | GET | `/products/{id}` | `SELECT ... WHERE id = ?` |
| Update (full) | PUT | `/products/{id}` | `UPDATE` (all columns) |
| Update (partial) | PATCH | `/products/{id}` | `UPDATE` (some columns) |
| Delete | DELETE | `/products/{id}` | `DELETE` |

---

## Part B — Building a CRUD API with FastAPI

### 1. What FastAPI is

- A Python web framework built on **Starlette** (the web/ASGI layer) and **Pydantic** (validation).
- It uses **type hints** to parse, validate, convert, and document requests automatically.
- It generates **interactive docs** from your code: Swagger UI at `/docs`, ReDoc at `/redoc`, and the OpenAPI spec at `/openapi.json`.
- It runs on an **ASGI** server (Uvicorn) and supports both `async def` and plain `def` endpoints.

### 2. Where request data comes from

FastAPI works out where each parameter comes from based on how you declare it:

| Declared as | Comes from | Example |
|---|---|---|
| Name appears in the path `{...}` | **Path** | `def get(product_id: int)` with route `/products/{product_id}` |
| Simple type, not in the path | **Query string** | `def list(limit: int = 20, category: str \| None = None)` |
| Pydantic model | **JSON body** | `def create(payload: ProductCreate)` |
| `Header()`, `Cookie()` | Headers / cookies | `x_request_id: str = Header()` |
| `Depends(fn)` | The result of a **dependency** | `db: Session = Depends(get_db)` |

If a value can't be converted (e.g., `/products/abc` when the parameter is an `int`), FastAPI returns **422** automatically.

### 3. Key endpoint pieces

```python
@app.post("/products", response_model=ProductRead, status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate):
    ...
    return product

@app.patch("/products/{product_id}", response_model=ProductRead)
def update_product(product_id: int, payload: ProductUpdate):
    if product_id not in _db:
        raise HTTPException(status_code=404, detail="Product not found")
    changes = payload.model_dump(exclude_unset=True)
    _db[product_id] = _db[product_id].model_copy(update=changes)
    return _db[product_id]
```

- **`response_model`** validates and **filters** the output. Fields that aren't in the model are never returned, so values like `password_hash` can't leak.
- **`status_code`** sets the status code for a successful response (201 for create, 204 for delete).
- **`HTTPException`** stops the handler and returns an error response with `{"detail": ...}`. That's FastAPI's default. To use one consistent format across the API (the `{"error": {code, message, details, request_id}}` shape from Unit 2), register custom exception handlers that return it.
- **`exclude_unset=True`** is what makes PATCH work: it separates "the client didn't send this field" from "the client sent `null`."

### 4. Organizing a real project

```
app/
├── main.py            # creates FastAPI(), includes routers
├── database.py        # engine, SessionLocal, get_db
├── models.py          # SQLAlchemy ORM tables
├── schemas.py         # Pydantic request/response models
└── routers/
    ├── products.py    # APIRouter(prefix="/products", tags=["products"])
    └── orders.py
```

```python
# routers/products.py
from fastapi import APIRouter
router = APIRouter(prefix="/products", tags=["products"])

# main.py
app.include_router(products.router, prefix="/api/v1")
```

### 5. Dependency injection (`Depends`)

A dependency is a function FastAPI runs **before** your endpoint, and its return value is passed into the endpoint as a parameter. Use dependencies for DB sessions, the current user, pagination parameters, or shared settings.

```python
from fastapi import Depends

def pagination(limit: int = 20, offset: int = 0):
    return {"limit": min(limit, 100), "offset": offset}   # cap page size

@router.get("/")
def list_products(page: dict = Depends(pagination)):
    ...
```

A dependency that uses `yield` can run **cleanup code after the response**. That's how DB sessions get closed (Part D).

### 6. `async def` vs `def`

| Use | When |
|---|---|
| `async def` | Everything inside is non-blocking and awaited (an async DB driver, `httpx.AsyncClient`) |
| `def` | You call **blocking** code (sync SQLAlchemy, `requests`, file I/O). FastAPI runs the endpoint in a **thread pool**, so it doesn't freeze the server. |

**Pitfall:** calling blocking code inside `async def` blocks the event loop, and every other request has to wait.

---

## Part C — Data Validation with Pydantic

### 1. What Pydantic does

- You define the shape of the data with **type hints** on a `BaseModel`.
- It **parses and converts** input (in the default lax mode, `"42"` becomes `42` for an `int` field) and **rejects** invalid data with a detailed list of errors.
- In FastAPI, a request body that fails validation gets a **422** response automatically, showing where each error is and why:

```json
{"detail": [{"type": "greater_than", "loc": ["body", "base_price"],
             "msg": "Input should be greater than 0", "input": -5}]}
```

### 2. Constraints with `Field`

```python
from decimal import Decimal
from pydantic import BaseModel, Field, EmailStr

class UserCreate(BaseModel):
    email: EmailStr                                   # needs: pip install email-validator
    full_name: str = Field(min_length=1, max_length=100)
    phone: str | None = Field(default=None, pattern=r"^\+?\d{9,15}$")

class OrderItemIn(BaseModel):
    prod_id: int
    quantity: int = Field(gt=0, le=100)
    unit_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
```

| Constraint | Applies to |
|---|---|
| `gt`, `ge`, `lt`, `le` | Numbers |
| `min_length`, `max_length` | Strings, lists |
| `pattern` | Strings (regex) |
| `max_digits`, `decimal_places` | `Decimal` (use it for money, not `float`) |
| `default` / `default_factory` | Optional values; mutable defaults (`default_factory=list`) |

**Required vs optional:**

- `x: int` is required.
- `x: int = 0` is optional, with a default.
- `x: int | None = None` is optional and may be `null`.

### 3. Nested models

```python
class OrderCreate(BaseModel):
    user_id: int
    shipping_add: int
    coupon_code: str | None = None
    items: list[OrderItemIn] = Field(min_length=1)   # at least one line item
```

Pydantic validates the whole nested structure. An error in `items[2].quantity` is reported at that exact location.

### 4. The "separate schemas" pattern

One database table usually gets **several Pydantic models**, because data going into the API and data coming out need different fields:

| Schema | Used for | Contains |
|---|---|---|
| `ProductCreate` | POST body | Fields the client supplies. **No `id`**, because the DB generates it. |
| `ProductUpdate` | PATCH body | The same fields, **all optional** |
| `ProductRead` | Response | Everything safe to show, **including `id`** and the `version` used for optimistic locking |
| (never exposed) | — | Secrets like `password_hash` never go in a Read schema |

```python
from pydantic import ConfigDict

class ProductRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # read from ORM objects, not just dicts
    id: int
    sku: str
    base_price: Decimal
    qty_on_hand: int
    version: int                                     # the client sends it back on updates (optimistic locking)
```

With `from_attributes=True`, FastAPI can turn a SQLAlchemy object into this schema, reading `product.sku` instead of `product["sku"]`.

---

## Part D — Connecting the API to a Relational Database

### 1. The layers

```
HTTP request → FastAPI route → Pydantic schema (validate)
            → SQLAlchemy ORM / session → DB driver (psycopg) → PostgreSQL
            ← ORM object → Pydantic Read schema (filter) → JSON response
```

- **Driver** (`psycopg`, `asyncpg`): talks to the database in its own network protocol.
- **SQLAlchemy Core**: builds SQL in Python.
- **SQLAlchemy ORM**: maps tables to classes and rows to objects.
- **Alternatives:** raw SQL with psycopg (full control, more boilerplate), or **SQLModel** (a single class that works as both the Pydantic model and the ORM table; it was written by FastAPI's author).

### 2. Database setup (`database.py`)

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

DATABASE_URL = settings.database_url   # e.g. postgresql+psycopg://user:pw@localhost:5432/shop

engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

def get_db():
    db = SessionLocal()
    try:
        yield db              # the endpoint runs here
    finally:
        db.close()            # always runs, even on errors → connection returns to the pool
```

> `settings` comes from a **settings class** (a pydantic-settings `BaseSettings`) defined elsewhere in the project. It loads configuration such as the database URL from the environment or a `.env` file. The URL is never hardcoded in the code, and you don't read it with `os.environ` yourself.

- **Engine:** one per app. It holds the **connection pool**. Opening a DB connection is expensive, so the pool keeps connections open and reuses them.
- **Session:** one per **request**. It is the unit of work: it tracks changes and wraps a transaction.
- **`pool_pre_ping`:** tests each connection before using it, so a connection the DB has already dropped doesn't cause an error.

### 3. ORM models (`models.py`)

```python
from decimal import Decimal
from sqlalchemy import String, Numeric, ForeignKey, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    description: Mapped[str | None]
    base_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    qty_on_hand: Mapped[int] = mapped_column(default=0)
    qty_reserved: Mapped[int] = mapped_column(default=0)
    version: Mapped[int] = mapped_column(default=1)          # for optimistic locking
    __table_args__ = (CheckConstraint("qty_reserved BETWEEN 0 AND qty_on_hand"),)

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)   # index FK columns
    shipping_add: Mapped[int] = mapped_column(ForeignKey("addresses.id"))
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")
```

The **ORM model** (the table) and the **Pydantic schema** (the API contract) are kept separate on purpose. You can change the database without breaking clients, and change the API without changing the database.

### 4. Transactions across several tables

Creating an order means inserting into `orders` and `order_item` and reserving stock. All of it must succeed or none of it should (Atomicity, from Unit 3):

```python
@router.post("/orders", status_code=201, response_model=OrderRead)
def create_order(payload: OrderCreate, db: Session = Depends(get_db)):
    try:
        with db.begin():                               # commits on success, rolls back on any exception
            order = Order(user_id=payload.user_id, shipping_add=payload.shipping_add)
            db.add(order)
            for item in payload.items:
                result = db.execute(
                    update(Product)
                    .where(Product.id == item.prod_id,
                           Product.qty_on_hand - Product.qty_reserved >= item.quantity)   # check and update atomically
                    .values(qty_reserved=Product.qty_reserved + item.quantity)      # reserve, as in Unit 3
                )
                if result.rowcount == 0:
                    raise HTTPException(409, f"Not enough stock for product {item.prod_id}")
                order.items.append(OrderItem(prod_id=item.prod_id, quantity=item.quantity,
                                             unit_price=item.unit_price))
    except IntegrityError:
        raise HTTPException(400, "Invalid user, address, or product reference")
    db.refresh(order)
    return order
```

An `HTTPException` raised inside `with db.begin():` rolls back the whole transaction before FastAPI sends the 409.

**Optimistic locking through the API:** the client sends back the `version` it read (returned in `ProductRead`). The server runs `UPDATE ... WHERE id = :id AND version = :v`. If 0 rows change, it returns **409 Conflict** so the client can reload the data and retry.

### 5. Common problems and fixes

| Problem | Fix |
|---|---|
| **N+1 queries:** listing 50 orders, then lazy-loading `order.items` for each one, runs 50 more queries | Load the relationship up front (eager loading): `select(Order).options(selectinload(Order.items))` |
| Session left open / connection leak | `get_db` with `yield` and `finally: db.close()` |
| Sharing one session across requests | One session per request, provided by the dependency. Sessions are **not** thread-safe. |
| Returning ORM objects without `response_model` | Always use a Read schema: it filters the fields and documents the response shape |
| DB constraint error surfaces as a 500 | Catch `IntegrityError`, call `db.rollback()`, return 409 or 400 |
| Unbounded list endpoints | Cap `limit`, and prefer keyset pagination |
| SQL injection with raw SQL | Always use **bound parameters**: `text("... WHERE id = :id"), {"id": x}`. Never build SQL with f-strings. |
| Changing the schema by hand | **Alembic** migrations: `alembic revision --autogenerate -m "add version"`, then `alembic upgrade head`. Use `Base.metadata.create_all()` only for quick demos. |
| Hard-coded DB password | Load it through the settings class (`pydantic-settings` + env var / `.env`), and keep `.env` out of Git |

### 6. Async option

With `create_async_engine("postgresql+asyncpg://...")` and `AsyncSession`, endpoints become `async def` and every DB call is awaited (`await db.execute(...)`). This helps when there is a very large number of concurrent requests. Sync SQLAlchemy with `def` endpoints is simpler and is fine for most class projects.

### 7. Testing the API

```python
from fastapi.testclient import TestClient

app.dependency_overrides[get_db] = get_test_db      # swap in a test database
client = TestClient(app)

def test_create_product():
    r = client.post("/api/v1/products", json={"sku": "SH-001", "base_price": "59.90"})
    assert r.status_code == 201
    assert r.json()["sku"] == "SH-001"

def test_invalid_price():
    r = client.post("/api/v1/products", json={"sku": "SH-002", "base_price": -1})
    assert r.status_code == 422
```

`dependency_overrides` is one of the biggest benefits of `Depends`: tests can replace the real database without changing any endpoint code.

---

## Key Terms Cheat Sheet
- **HTTP method:** the action verb (GET/POST/PUT/PATCH/DELETE)
- **Safe / idempotent:** doesn't change state / repeating it gives the same final state
- **Idempotency key:** a header that makes POST retries safe
- **REST:** a stateless, resource-oriented style built on URIs, methods, and representations
- **Stateless:** each request carries its own context, so any server can handle it
- **Resource / endpoint:** a noun addressed by a URI / a method + path combination
- **Path vs query parameter:** identifies the resource / filters or modifies the result
- **2xx / 4xx / 5xx:** success / client error / server error. 422 = validation failed, 409 = conflict.
- **ASGI / Uvicorn:** the async server interface / the server that runs FastAPI
- **OpenAPI / Swagger UI:** machine-readable API spec / interactive docs at `/docs`
- **`response_model`:** validates and filters the output
- **`HTTPException`:** returns an error status from a handler
- **`APIRouter`:** groups related endpoints
- **Dependency injection (`Depends`):** FastAPI passes shared resources (a DB session, the current user) into endpoints
- **Pydantic `BaseModel`:** a schema built from type hints that parses and validates data
- **`Field`:** constraints (`gt`, `max_length`, `pattern`)
- **Create / Update / Read schemas:** separate models for data coming in and data going out
- **`exclude_unset`:** only the fields the client sent (for PATCH)
- **`from_attributes`:** builds a schema from an ORM object
- **Settings class (`pydantic-settings`):** loads config such as `DATABASE_URL` from the environment or a `.env` file
- **ORM:** maps tables ↔ classes and rows ↔ objects
- **Engine / connection pool / session:** app-wide connection manager / reused connections / per-request unit of work
- **`commit` / `rollback` / `refresh`:** save / undo / reload values the DB generated
- **N+1 problem:** one extra query per row; fix it with eager loading
- **Alembic:** the schema migration tool for SQLAlchemy

---

## Practice Questions

1. **Which HTTP methods are idempotent, and why does it matter?**

    GET, PUT, and DELETE (plus HEAD and OPTIONS). Clients and proxies can safely retry them after a timeout without side effects piling up.

2. **Is DELETE still idempotent if the second call returns 404?**

    Yes. Idempotency is about the server's **state**, not the response. The resource is gone either way.

3. **PUT vs PATCH: what's the difference?**

    PUT replaces the entire resource, so you send every field. PATCH updates only the fields you send.

4. **What status code should each of these return: creating a product, deleting a product, product not found, duplicate SKU, a negative price in the body?**

    201, 204, 404, 409, 422.

5. **401 vs 403?**

    401 means the caller is not authenticated (credentials are missing or invalid). 403 means the caller is authenticated but not allowed to do this.

6. **What does "stateless" mean in REST, and what does it make easier?**

    The server stores no client session between requests, because each request carries its own context (e.g., the token). This makes horizontal scaling and load balancing easy.

7. **Rewrite these endpoints RESTfully: `GET /getAllOrders`, `POST /deleteProduct?id=5`, `GET /orders?userId=5`.**

    `GET /orders`, `DELETE /products/5`, and `GET /users/5/orders` (or `GET /orders?user_id=5`).

8. **How does FastAPI decide whether a parameter comes from the path, the query string, or the body?**

    A name in the path template comes from the path. A simple type that isn't in the path comes from the query string. A Pydantic model comes from the JSON body.

9. **What does `response_model` do besides documenting the response?**

    It validates the output and **filters out** fields that aren't in the schema, which prevents leaks like `password_hash`.

10. **Why have separate `ProductCreate`, `ProductUpdate`, and `ProductRead` schemas?**

    Create has no `id` because the DB generates it. Update has all-optional fields for PATCH. Read includes `id` and leaves out secrets. Each direction has a different contract.

11. **Why use `model_dump(exclude_unset=True)` in a PATCH handler?**

    So that fields the client didn't send aren't overwritten with defaults or `None`. Only the fields that were actually sent get updated.

12. **Write a Pydantic field that only accepts a `quantity` from 1 to 100.**

    `quantity: int = Field(ge=1, le=100)`.

13. **What happens when a request body fails Pydantic validation in FastAPI?**

    FastAPI returns 422 automatically, with a `detail` list that gives each error's location (`loc`), message, and input. Your endpoint code never runs.

14. **Why use `Decimal` rather than `float` for prices?**

    Floats are binary approximations (0.1 + 0.2 ≠ 0.3), so rounding errors creep into money. `Decimal` maps exactly to SQL `NUMERIC`.

15. **What does `from_attributes=True` enable?**

    Building a Pydantic model from an object's attributes, such as a SQLAlchemy row, instead of only from a dict.

16. **Where does `database.py` get the database URL, and why?**

    From the settings class (`settings.database_url`), which loads it from the environment or a `.env` file. Credentials stay out of the code and out of Git, and each environment (dev, test, prod) can use a different database.

17. **Why does `get_db` use `yield` and `finally`?**

    The session is created before the endpoint runs and is **always closed afterward**, even when there is an error, so the connection goes back to the pool.

18. **Why one session per request, not one global session?**

    Sessions aren't thread-safe, and a single shared session would mix different users' transactions and state together.

19. **What's the purpose of the engine's connection pool?**

    Opening DB connections is expensive. The pool keeps some connections open and reuses them across requests.

20. **A duplicate SKU insert crashes with a 500. How do you fix it?**

    Catch `IntegrityError`, call `db.rollback()`, and raise `HTTPException(409, "SKU already exists")`.

21. **How do you make "create order + items + reserve stock" all-or-nothing?**

    Wrap it in one transaction (`with db.begin():`) so that any exception rolls everything back. Use a conditional `UPDATE ... SET qty_reserved = qty_reserved + :qty WHERE qty_on_hand - qty_reserved >= :qty` and check `rowcount` so stock can't be oversold.

22. **Listing 50 orders triggers 51 SQL queries. What is this called, and how do you fix it?**

    The N+1 problem. Load the relationship up front: `select(Order).options(selectinload(Order.items))`.

23. **When should an endpoint be `def` instead of `async def`?**

    When it calls blocking code, such as sync SQLAlchemy or `requests`. FastAPI runs `def` endpoints in a thread pool, while blocking code inside `async def` freezes the event loop for every request.

24. **Why use Alembic instead of `Base.metadata.create_all()`?**

    `create_all` only creates tables that are missing. It can't change existing tables or keep a history of changes. Alembic keeps versioned, reversible migrations that you can apply in every environment.

25. **How do you test endpoints without touching the real database?**

    Use `TestClient` with `app.dependency_overrides[get_db] = get_test_db` to swap in a test database.
