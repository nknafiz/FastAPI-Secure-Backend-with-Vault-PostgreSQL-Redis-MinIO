FastAPI Project – Secure Backend & Service Integration

This repository contains a highly secure and production-ready backend system built with FastAPI, SQLModel, and Pydantic Settings.
It is designed for enterprise-level applications where ephemeral secrets and dynamic service rotation are critical.

🔐 Key Features

Vault Integration (HashiCorp Vault):
Securely fetch and rotate ephemeral secrets (DB, Redis, MinIO, VectorDB).

Database Engines:

Async DB (PostgreSQL + AsyncPG)

Sync Experiment DB (PostgreSQL + Psycopg2)
With automatic connection validation and atomic swapping.

Redis (Async & Sync):
Fully integrated with password rotation from Vault.

MinIO / S3 Object Storage:
Secure MinIO client setup using Vault secret keys.

VectorDB Support (Qdrant / Weaviate / Pinecone):
API key management via Vault.

Secret Rotation System:
Periodic background task for refreshing credentials with jitter + exponential backoff.

Concurrency & Pool Management:
Adaptive pool sizing with semaphore-based concurrency control.

Startup & Shutdown Hooks:
Ensures safe initialization and graceful shutdown of DB engines and external services.

🚀 Technology Stack

FastAPI (backend framework)

SQLModel + SQLAlchemy (ORM & database engine)

Redis (cache & queue system)

MinIO / S3 (object storage)

Vault (HVAC client) for secret management

AnyIO + AsyncIO for concurrency

⚡ Use Cases

Banking and Financial Systems 🏦

AI/ML Experiment Tracking ⚙️

High-Security SaaS Applications 🔒

E-Commerce Platforms with secure data handling 🛒
