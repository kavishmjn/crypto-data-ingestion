🚀 Crypto Assets ELT Pipeline
Python | PostgreSQL | dbt | Batch Architecture

📌 Project Summary
Designed and implemented a file-based batch ELT pipeline that ingests cryptocurrency asset data from an external API, stores raw snapshots in PostgreSQL, and prepares the foundation for transformation using dbt.
This project demonstrates practical understanding of:
Data Engineering architecture
ELT design principles
Modular Python development
Bulk database ingestion strategies
Batch processing systems

🏗 Architecture Overview
External API
   ↓
Python Ingestion Layer
   ↓
File-Based Batch Storage (JSON + CSV)
   ↓
PostgreSQL (Raw Layer - Append Only)
   ↓
dbt (Staging → Mart Models)

🔧 Key Features
✅ Modular Ingestion Framework
Structured package design (ingestion/, database/)
Clean orchestration in main.py
Environment-driven configuration via .env
Centralized config management
✅ Append-Only Raw Layer
Preserves historical snapshots
Supports replayability and auditing
Includes batch_id and ingestion timestamps
No transformations applied during ingestion (ELT pattern)

✅ Bulk Insert Optimization
Uses psycopg2.extras.execute_values
Parameterized queries (SQL injection safe)
Single-transaction batch ingestion

✅ File-Based Batch System
Each pipeline run generates:
data/<batch_id>/
   ├── raw/assets.json
   └── CSV/assets.csv
Enables:
Deterministic replay
Auditability
Debugging
Historical traceability

✅ Logging & Error Handling
Structured logging to file and console
Exception propagation
Validation layers (file-level, schema-level, data-level)

🗄 Database Design
Raw Schema (raw.assets)
Structured but source-aligned
Stored as TEXT for ELT flexibility
Append-only ingestion
Designed for downstream transformation via dbt
Transformation and type casting are intentionally deferred to dbt.

🧠 Engineering Decisions
Decision	Rationale
Append-only raw	Preserves historical state and enables analytics
No transformation in Python	Clear separation of concerns (ELT)
Schema creation separated from ingestion	Clean architecture principle
Batch-based directory structure	Supports reproducibility and auditability
execute_values for insert	Optimized for micro-batch performance

🛠 Technology Stack
Python 3.11+
PostgreSQL
psycopg2
dbt (Postgres adapter)
dotenv (environment configuration)
Standard logging framework

📈 Skills Demonstrated
Data pipeline design
ELT architecture implementation
Batch processing systems
Modular Python engineering
SQL bulk loading strategies
Schema management and evolution thinking
Source-to-warehouse data flow modeling
Configuration-driven system design

🔮 Roadmap
dbt staging models (type casting + cleaning)
Incremental dbt models
Snapshot (SCD2) implementation
Batch metadata tracking table
Retry logic and resiliency enhancements
Containerization (Docker)
Orchestration (Airflow)

👨‍💻 Author
Kavish Mahajan
Data Engineer
Python | SQL | PostgreSQL | dbt
