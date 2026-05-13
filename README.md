<div align="center">

<img src="https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png" width="260" alt="FastAPI"/>

# FastAPI Secure Backend
### by **NK. Nafiz Khan**

**A high-security, production-ready async backend** — multi-tenant user isolation, Vault-managed dynamic secrets, Redis-backed JWT blacklisting, vector-powered AI recommendations, and zero-downtime credential rotation.

---

[![FastAPI](https://img.shields.io/badge/FastAPI-0.116.1-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7+-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Storage-C72E49?style=for-the-badge&logo=minio&logoColor=white)](https://min.io)
[![Vault](https://img.shields.io/badge/HashiCorp_Vault-Secret_Mgmt-000000?style=for-the-badge&logo=vault&logoColor=white)](https://vaultproject.io)
[![Qdrant](https://img.shields.io/badge/Qdrant-Vector_DB-FF4785?style=for-the-badge)](https://qdrant.tech)
[![Celery](https://img.shields.io/badge/Celery-5.3.1-37814A?style=for-the-badge&logo=celery&logoColor=white)](https://docs.celeryq.dev)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

</div>

---

## 🧠 What Makes This Different

This is not a basic CRUD boilerplate. Every design decision here solves a real enterprise security or scalability problem:

| Problem | What Most Backends Do | What This Backend Does |
|---|---|---|
| Credential leaks | Secrets in `.env` | HashiCorp Vault — secrets never touch disk in production |
| Stolen JWT tokens | No way to invalidate | Redis blacklist per `jti` — logout truly kills the token |
| User data isolation | Everyone shares one schema | Each user gets their own PostgreSQL schema (`user_x_at_y_dot_com`) |
| Credential rotation | Requires downtime | Background task rotates secrets every 5 min, zero downtime |
| Brute-force attacks | No protection | Per-IP + per-user rate limiting, auto account lock after 5 failures |
| Cold-start recommendations | Show nothing or popular items | MMR-diverse Qdrant vector search with epsilon-greedy exploration |
| Blocking the async loop | One DB for everything | Dual DB: async engine for API, sync engine for MLflow/Celery |

---

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Security Features](#-security-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Environment Configuration](#-environment-configuration)
- [HashiCorp Vault Setup](#-hashicorp-vault-setup)
- [Running the App](#-running-the-app)
- [All API Endpoints](#-all-api-endpoints)
- [How Key Features Work](#-how-key-features-work)
- [Docker Setup](#-docker-setup-optional)
- [What You Can Build](#-what-you-can-build-with-this)

---

## 🏗 Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                    Client  (Browser / Mobile / CLI)               │
└──────────────────────────────┬────────────────────────────────────┘
                               │ HTTPS
┌──────────────────────────────▼────────────────────────────────────┐
│                   FastAPI  (Uvicorn + uvloop ASGI)                │
│                                                                   │
│  /auth/*          /admin/*         /health        /docs           │
│  auth.py          admin.py         main.py        OpenAPI         │
│      │                │                                           │
│  deps/auth.py ────────┘  (JWT decode + role check on every req)  │
│      │                                                            │
│  ┌───▼──────────────────────────────────────────────────────┐    │
│  │                     core/ layer                          │    │
│  │  config.py   db.py   security.py   redis_cache.py        │    │
│  └───┬──────────────┬──────────┬──────────────┬────────────┘    │
└──────┼──────────────┼──────────┼──────────────┼─────────────────┘
       │              │          │              │
  ┌────▼────┐  ┌──────▼──────┐  ┌▼──────┐  ┌──▼──────────┐
  │  Vault  │  │  PostgreSQL  │  │ Redis │  │    MinIO    │
  │ KV  v2  │  │async + sync  │  │ 7+    │  │  S3 store   │
  └─────────┘  └──────────────┘  └───────┘  └─────────────┘
                      │
             ┌────────┴───────────┐
        api_app (async)     my_api (sync)
        FastAPI routes      MLflow / Celery
        User schemas        Experiment tracking
```

---

## 🔐 Security Features

This backend implements **5 layers of security** — most backends have 1 or 2.

### Layer 1 — HashiCorp Vault (Secret Management)
- All DB, Redis, MinIO, and VectorDB credentials fetched from Vault at runtime
- Credentials **never stored in code or `.env`** in production
- In-memory secret cache with 90% TTL prevents hammering Vault
- Background `rotate_secrets()` task refreshes every 5 minutes with ±20% jitter

### Layer 2 — JWT with Real Logout (Redis Blacklist)
```
Access Token:   signed with SECRET_KEY       → short-lived (60 min)
Refresh Token:  signed with JWT_REFRESH_TOKEN_KEY → long-lived (100 days)

On Logout:
  jti  →  Redis blacklist key  →  expires when token would have expired
  Every authenticated request checks blacklist first → O(1) lookup
```

### Layer 3 — Multi-Tenant PostgreSQL Schema Isolation
```
User A (alice@email.com) → schema: "user_alice_at_email_dot_com"
User B (bob@corp.com)    → schema: "user_bob_at_corp_dot_com"
Superuser                → schema: "public"

Each user's data is physically isolated — a bug in one tenant
cannot leak another tenant's data even with a SQL injection.
```

### Layer 4 — Rate Limiting + Account Lockout
```
Per-IP  rate limit:  10 requests / 60 seconds  → HTTP 429
Per-user rate limit: 10 requests / 60 seconds  → HTTP 429
Failed login:        5 failures → account locked 15 minutes → HTTP 423
Superadmin:          Cannot be blocked/deleted by any admin
```

### Layer 5 — IP Whitelist / Blocklist
```
IP_WHITELIST:      only listed IPs can log in
IP_BLOCKLIST:      listed IPs always rejected
ALLOWED_COUNTRIES: country-level access control (configurable)
```

---

## 🛠 Tech Stack

| Layer | Technology | Version | Role |
|---|---|---|---|
| Web Framework | FastAPI | 0.116.1 | Async REST API + OpenAPI |
| ASGI Server | Uvicorn + uvloop | 0.35.0 | High-performance event loop |
| ORM | SQLModel + SQLAlchemy | 0.0.24 / 2.0.43 | Type-safe models |
| Async DB Driver | AsyncPG | 0.30.0 | Non-blocking PostgreSQL |
| Cache / Queue Broker | Redis | 6.4.0 | JWT blacklist + session store |
| Object Storage | MinIO | Latest | S3-compatible file storage |
| Vector Database | Qdrant | Latest | AI embeddings + semantic search |
| Secret Management | HashiCorp Vault | Latest | Dynamic credential rotation |
| Authentication | python-jose + passlib | 3.5.0 / 1.7.4 | JWT HS256 + bcrypt |
| Task Queue | Celery + Kombu | 5.3.1 | Background jobs |
| Schema Validation | Pydantic v2 | 2.11.7 | Request/response types |
| Migrations | Alembic | Latest | DB schema versioning |
| HTTP Client | HTTPX | 0.28.1 | Async HTTP calls |

---

## 📁 Project Structure

```
FastAPI-Secure-Backend/
│
├── 📂 alembic/                      # Database migration engine
│   ├── versions/                    # Auto-generated migration files
│   └── env.py                       # Alembic config
│
├── 📂 src/app/
│   │
│   ├── 📂 api/                      # HTTP route handlers
│   │   ├── auth.py                  # /auth/* — register, login, logout,
│   │   │                            #   refresh, forgot/reset password, /me
│   │   └── admin.py                 # /admin/* — list users, block/unblock,
│   │                                #   grant/revoke admin rights
│   │
│   ├── 📂 core/                     # Infrastructure (most critical)
│   │   ├── config.py                # Pydantic Settings + Vault integration
│   │   │                            # + connection pool init
│   │   ├── db.py                    # Async/sync SQLAlchemy engines
│   │   │                            # + Vault secret rotation task
│   │   ├── security.py              # JWT mint / decode / revoke / rotate
│   │   ├── redis_cache.py           # Blacklist, rate limit, login history,
│   │   │                            # refresh token store, IP check
│   │   └── initial_data.py          # Auto-seed superuser on first startup
│   │
│   ├── 📂 crud/                     # Pure database queries
│   │   ├── admin.py                 # Admin create, get, ensure_superuser
│   │   └── public_user.py           # Block, unblock, grant/revoke admin,
│   │                                # list all users
│   │
│   ├── 📂 deps/                     # FastAPI dependency injectors
│   │   └── auth.py                  # get_current_superadmin — JWT decode
│   │                                # + role validation on every request
│   │
│   ├── 📂 models/                   # SQLModel DB table definitions
│   │   ├── admin.py                 # Admin table (public schema)
│   │   └── user_registry.py         # UserRegistry table (public schema)
│   │                                # maps email → tenant schema name
│   │
│   ├── 📂 schemas/                  # Pydantic request/response models
│   │   ├── admin.py                 # AdminCreate, AdminRead
│   │   └── user.py                  # create_user_schema() — creates
│   │                                # per-user PostgreSQL schema + tables
│   │
│   ├── 📂 service/                  # Business logic
│   │   └── recomed.py               # Full AI recommendation engine:
│   │                                # Qdrant ANN + MMR diversity +
│   │                                # epsilon-greedy exploration +
│   │                                # session + persistent vector fusion
│   │
│   └── main.py                      # FastAPI app + lifespan hooks
│
├── 📂 tests/                        # Pytest test suite
├── .env                             # Local config (never commit)
├── .env.example                     # Safe template for team
├── alembic.ini
├── pyproject.toml
├── requirements.in                  # Direct deps (pip-tools)
├── requirements.txt                 # Pinned full tree
└── runserver.py                     # Dev entry point
```

---

## ⚙️ Prerequisites

| Tool | Min Version | Check | Install |
|---|---|---|---|
| Python | 3.11+ | `python --version` | [python.org](https://python.org/downloads/) |
| PostgreSQL | 15+ | `psql --version` | [postgresql.org](https://www.postgresql.org/download/) |
| Redis | 7+ | `redis-cli --version` | [redis.io](https://redis.io/docs/getting-started/installation/) |
| MinIO | Latest | `minio --version` | [min.io](https://min.io/docs/minio/linux/index.html) |
| HashiCorp Vault | Latest | `vault version` | [hashicorp.com](https://developer.hashicorp.com/vault/docs/install) |
| Qdrant | Latest | — | [qdrant.tech](https://qdrant.tech/documentation/quick-start/) |

---

## 🚀 Installation

### 1. Clone

```bash
git clone https://github.com/nknafiz/FastAPI-Secure-Backend-with-Vault-PostgreSQL-Redis-MinIO.git
cd FastAPI-Secure-Backend-with-Vault-PostgreSQL-Redis-MinIO
```

### 2. Virtual Environment

```bash
python -m venv venv

# macOS / Linux
source venv/bin/activate

# Windows PowerShell
venv\Scripts\Activate.ps1
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment File

```bash
cp .env.example .env
# Edit .env with your values
```

### 5. Create PostgreSQL Databases

```bash
psql -U postgres

CREATE DATABASE api_app;    -- async API database
CREATE DATABASE my_api;     -- sync experiment database
\q
```

### 6. Start Services

**Vault** (Terminal 1):
```bash
vault server -dev -dev-root-token-id="root"
```

**Vault config** (Terminal 2):
```bash
export VAULT_ADDR='http://127.0.0.1:8200'
export VAULT_TOKEN='root'
```

**Redis**:
```bash
# macOS
brew services start redis

# Linux
sudo systemctl start redis

# Docker
docker run -d -p 6379:6379 redis:7-alpine
```

**MinIO**:
```bash
minio server ~/minio-data --console-address ":9001"
# Console → http://localhost:9001
```

**Qdrant**:
```bash
docker run -d -p 6333:6333 qdrant/qdrant
```

### 7. Store Vault Secrets

```bash
vault secrets enable -path=secret kv-v2

vault kv put secret/db/main \
  username="postgres" password="your_db_password" \
  host="localhost" port="5432" dbname="api_app"

vault kv put secret/db/experiment dbname="my_api"

vault kv put secret/redis/main \
  host="localhost" port="6379" password="your_redis_password"

vault kv put secret/minio/main \
  username="admin" password="your_minio_password" \
  url="127.0.0.1:9000" bucket="your_bucket"

vault kv put secret/vector/main \
  url="http://localhost:6333" api_key="your_api_key"
```

### 8. Run Migrations

```bash
alembic upgrade head
```

### 9. Start the Application

```bash
# Development (auto-reload)
python runserver.py

# OR
uvicorn src.app.main:app --host 127.0.0.1 --port 8000 --reload
```

| Interface | URL |
|---|---|
| **Swagger UI** | http://localhost:8000/docs |
| **ReDoc** | http://localhost:8000/redoc |
| **Health Check** | http://localhost:8000/health |

---

## 🔧 Environment Configuration

```env
# ── Application ─────────────────────────────────────────────────────
MODE=development                    # development | staging | production
PROJECT_NAME=protfoliyo
API_VERSION=v1
API_V1_STR=/api/v1
JWT_ISSUER=protfoliyo
JWT_AUDIENCE=protfoliyo_users

# ── Security (generate with: openssl rand -hex 32) ──────────────────
SECRET_KEY=your-256-bit-secret-key
ENCRYPT_KEY=your-encryption-key
JWT_REFRESH_TOKEN_KEY=your-refresh-token-secret
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=60
REFRESH_TOKEN_EXPIRE_MINUTES=144000

# ── Rate Limiting ────────────────────────────────────────────────────
LOGIN_RATE_LIMIT=10                 # max requests per window
LOGIN_RATE_WINDOW=60                # window in seconds
FAILED_LOCK_THRESHOLD=5            # failed attempts before lock
FAILED_LOCK_SECONDS=900            # lockout duration (15 min)

# ── IP Security ──────────────────────────────────────────────────────
IP_WHITELIST=[]                     # e.g. ["1.2.3.4","5.6.7.8"]
IP_BLOCKLIST=[]
ALLOWED_COUNTRIES=[]               # e.g. ["BD","US"]

# ── Primary Database (Async — FastAPI routes) ────────────────────────
DATABASE_USER=postgres
DATABASE_PASSWORD=your_password
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=api_app

# ── Experiment Database (Sync — MLflow / Celery) ─────────────────────
EXPERIMENT_DB_NAME=my_api

# ── Redis ────────────────────────────────────────────────────────────
REDIS_HOST=localhost
REDIS_PORT=6379

# ── MinIO ────────────────────────────────────────────────────────────
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=your_minio_password
MINIO_URL=127.0.0.1:9000
MINIO_BUCKET=your_bucket

# ── Qdrant Vector DB ─────────────────────────────────────────────────
VECTOR_DB_URL=http://localhost:6333
VECTOR_DB_API_KEY=your_vector_api_key

# ── HashiCorp Vault ──────────────────────────────────────────────────
VAULT_URL=http://127.0.0.1:8200
VAULT_TOKEN=root
VAULT_DB_MAIN_PATH=db/main
VAULT_DB_EXP_PATH=db/experiment
VAULT_REDIS_PATH=redis/main
VAULT_MINIO_PATH=minio/main
VAULT_VECTOR_PATH=vector/main

# ── First Superuser (auto-created on startup) ────────────────────────
FIRST_SUPERUSER_EMAIL=admin@example.com
FIRST_SUPERUSER_PASSWORD=strong_password_here

# ── Performance ──────────────────────────────────────────────────────
DB_POOL_SIZE=20
WEB_CONCURRENCY=4

# ── CORS ─────────────────────────────────────────────────────────────
BACKEND_CORS_ORIGINS=["http://localhost:3000"]

# ── Observability ────────────────────────────────────────────────────
PROMETHEUS_URL=http://localhost:9090
GRAFANA_URL=http://localhost:3000
LOG_LEVEL=INFO
```

> ⚠️ **Production enforcement:** When `MODE=production`, startup **fails** if `SECRET_KEY`, `DATABASE_USER`, or `DATABASE_PASSWORD` are missing or set to known insecure defaults. Fail-fast is intentional.

---

## 📡 All API Endpoints

### Auth  (`/auth`)

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| `POST` | `/auth/register` | Register new user → creates isolated PostgreSQL schema | ❌ |
| `POST` | `/auth/login` | Login → returns access + refresh token | ❌ |
| `POST` | `/auth/logout` | Blacklist token in Redis → real invalidation | ✅ Bearer |
| `POST` | `/auth/refresh` | Rotate refresh token → new access token | ✅ Refresh |
| `POST` | `/auth/forgot-password` | Generate reset token (30 min TTL, stored in Redis) | ❌ |
| `POST` | `/auth/reset-password` | Verify reset token → update password + revoke session | ❌ |
| `GET`  | `/auth/me` | Current user info + login history | ✅ Bearer |

### Admin  (`/admin`)

| Method | Endpoint | Description | Auth |
|---|---|---|---|
| `GET` | `/admin/users` | List all registered users | ✅ Superadmin |
| `POST` | `/admin/block/{email}` | Block user account | ✅ Superadmin |
| `POST` | `/admin/unblock/{email}` | Unblock user account | ✅ Superadmin |
| `POST` | `/admin/grant_admin/{email}` | Promote user to admin | ✅ Superadmin |
| `POST` | `/admin/revoke_admin/{email}` | Demote admin to user | ✅ Superadmin |

### System

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Returns DB URI + Redis status |
| `GET` | `/docs` | Swagger interactive UI |
| `GET` | `/redoc` | ReDoc documentation |

### Example Requests

**Register:**
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "SecurePass123"}'
```

**Login:**
```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=alice@example.com&password=SecurePass123"
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

**Authenticated Request:**
```bash
curl http://localhost:8000/auth/me \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

---

## 🔍 How Key Features Work

### Multi-Tenant Schema Isolation

When a user registers with `alice@example.com`:

```sql
-- 1. Creates a dedicated schema
CREATE SCHEMA IF NOT EXISTS "user_alice_at_example_dot_com";

-- 2. Creates a user table inside that schema
CREATE TABLE IF NOT EXISTS "user_alice_at_example_dot_com".master_user (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 3. Registers the mapping in the public registry
INSERT INTO public.user_registry (email, schema_name, is_active, is_superuser)
VALUES ('alice@example.com', 'user_alice_at_example_dot_com', true, false);
```

Every login looks up the user's schema from `user_registry`, then queries only their isolated schema. A SQL bug in one tenant's schema **cannot read another tenant's data**.

---

### JWT Token Lifecycle

```
Login
  │
  ├─→ access_token   (signed: SECRET_KEY, TTL: 60 min)
  │     contains: sub, jti(UUID), iat, exp, iss, aud, roles
  │
  └─→ refresh_token  (signed: JWT_REFRESH_TOKEN_KEY, TTL: 100 days)
        stored in Redis: user:{email}:refresh → {jti, token}

Authenticated Request
  │
  ├─→ decode token → verify signature
  ├─→ check Redis: blacklist:{jti} → exists? → 401 Token revoked
  └─→ pass → handler

Logout
  │
  ├─→ add jti to Redis blacklist (TTL = remaining token lifetime)
  └─→ delete user:{email}:refresh → refresh token dead too

Refresh
  │
  ├─→ old refresh token revoked
  ├─→ new refresh token minted + stored in Redis
  └─→ new access token returned
```

---

### AI Recommendation Engine (`service/recomed.py`)

A production-grade ML recommendation system with 6 stages:

```
User makes a request for recommendations
            │
            ▼
1. Session Events      ← Last 50 interactions from Redis (lpush/lrange)
            │
            ▼
2. Session Vector      ← Weighted average of item embeddings
                         (exponential decay: recent events weighted more)
            │
            ▼
3. Persistent Vector   ← Long-term user profile from PostgreSQL
                         (blended: 70% session + 30% persistent)
            │
            ▼
4. Qdrant ANN Search   ← Top 300 candidates by cosine similarity
                         (cold start? → fallback to popular items)
            │
            ▼
5. Multi-factor Scoring:
   final = 0.70 × similarity
         + 0.20 × log(popularity)
         + 0.10 × recency_decay
         + 0.05 × content_boost (tags/category)
            │
            ▼
6. MMR Diversity       ← Maximal Marginal Relevance (λ=0.7)
                         Balances relevance vs. variety in results
            │
            ▼
7. ε-Greedy Explore    ← 5% chance: replace last result with
                         a random popular unseen item
                         (prevents filter bubbles)
            │
            ▼
   Final top-K results with metadata
```

---

### Vault Secret Rotation (Zero Downtime)

```
App Startup
  ├─→ Load all 5 Vault paths concurrently (asyncio.gather)
  └─→ Init DB / Redis / MinIO connections

Background Task: rotate_secrets() — runs every 5 min ±20% jitter
  ├─→ Re-fetch Vault secrets → rebuild async DB engine
  ├─→ Re-init Redis / MinIO clients
  ├─→ On failure: exponential backoff (1s → 2s → 4s → ... 60s max)
  └─→ After 5 consecutive failures: log CRITICAL alert to ops

Result: rotate DB passwords in Vault → app picks it up automatically
        → no restart, no downtime, no manual intervention
```

---

## 🐳 Docker Setup (Optional)

Docker is not required. If you want to run all services with Docker:

```bash
# Copy and edit the env file first
cp .env.example .env
```

Create `docker-compose.yml`:

```yaml
version: "3.9"

services:
  api:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      - postgres
      - redis
      - minio
      - vault
      - qdrant

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: your_password
      POSTGRES_MULTIPLE_DATABASES: api_app,my_api
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: your_minio_password
    volumes:
      - minio_data:/data

  vault:
    image: hashicorp/vault:latest
    cap_add:
      - IPC_LOCK
    environment:
      VAULT_DEV_ROOT_TOKEN_ID: root
    ports:
      - "8200:8200"

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  pgdata:
  minio_data:
  qdrant_data:
```

```bash
# Build and start everything
docker compose up -d

# Check logs
docker compose logs -f api

# Stop
docker compose down
```

> **Note:** After starting with Docker, you still need to run the Vault secret setup commands and `alembic upgrade head` once.

---

## 🏢 What You Can Build With This

| Product | Key Features Used |
|---|---|
| **Multi-tenant SaaS** | Per-user schema isolation, role system, Vault-managed secrets |
| **Banking / Fintech** | 5-layer security, rate limiting, account lockout, real JWT revocation |
| **AI / ML Platform** | Qdrant vector search, async API + sync MLflow dual-DB, MinIO model storage |
| **E-commerce Backend** | Recommendation engine, Redis caching, MinIO product media, Celery orders |
| **Healthcare App** | Tenant data isolation, auto-rotating credentials, structured audit logs |
| **Recommendation System** | MMR diversity, session + persistent vector fusion, ε-greedy exploration |

---

## ⚡ Performance

```
POOL_SIZE = max(DB_POOL_SIZE ÷ WEB_CONCURRENCY, 2)

Example: DB_POOL_SIZE=20, WEB_CONCURRENCY=4
→ 5 connections per worker, 20 total — matches your PostgreSQL max_connections

Recommended settings by server:
  2 CPU  / 4GB  → WEB_CONCURRENCY=4,  DB_POOL_SIZE=20
  4 CPU  / 8GB  → WEB_CONCURRENCY=8,  DB_POOL_SIZE=40
  8 CPU  / 16GB → WEB_CONCURRENCY=16, DB_POOL_SIZE=80
```

The async `Semaphore(POOL_SIZE)` in `get_session()` ensures concurrent requests never exceed pool capacity — preventing `asyncpg.TooManyConnectionsError` under traffic bursts.

---

## 🤝 Contributing

```bash
git checkout -b feature/your-feature
pytest tests/ -v --asyncio-mode=auto
git commit -m "feat: add X"
git push origin feature/your-feature
# → open Pull Request
```

Commit format: `feat:` `fix:` `docs:` `refactor:` `test:`


---

<div align="center">

**Built with ❤️ by NK. Nafiz Khan**

*Backend engineer focused on high-security, async-first Python systems.*

[![GitHub](https://img.shields.io/badge/GitHub-nknafiz-181717?style=for-the-badge&logo=github)](https://github.com/nknafiz)
[![Star this repo](https://img.shields.io/github/stars/nknafiz/FastAPI-Secure-Backend-with-Vault-PostgreSQL-Redis-MinIO?style=for-the-badge&logo=github&label=⭐%20Star)](https://github.com/nknafiz/FastAPI-Secure-Backend-with-Vault-PostgreSQL-Redis-MinIO)

*Found a bug? [Open an issue](https://github.com/nknafiz/FastAPI-Secure-Backend-with-Vault-PostgreSQL-Redis-MinIO/issues)*

</div>
