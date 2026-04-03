# Financial ETL Pipeline Engine

![Portfolio Asset](https://img.shields.io/badge/Architecture-Medallion-blue)
![Quality](https://img.shields.io/badge/Code_Quality-SOLID-brightgreen)
![Readiness](https://img.shields.io/badge/Production-Ready-success)

## Executive Summary

Financial data ingestion is prone to severe technical debt: floating point rounding errors, silent data duplication, and untraceable accounting failures. 

This **Financial ETL Pipeline** solves these systemic problems by implementing a strict **Medallion Architecture (Bronze -> Silver -> Gold)**, backed by rigorous Type Checking and exact Decimal mathematical precision, ensuring CFO-grade reporting accuracy and fault-resilient idempotency.

## Architecture & Data Flow

We implement a Medallion Architecture with aggressive validation checkpoints:

```mermaid
graph TD
    A[Raw Transactions CSV] -->|"Extract: Lazy pl.scan_csv"| B(Bronze Layer)
    B -->|"Pydantic V2 Models"| C{Silver Quality Gate}
    C -->|"Invalid format OR Negative Amount"| F[Quarantine / DLQ]
    C -->|"Valid Model"| D{Idempotency Check}
    D -->|"Hash Exists"| F
    D -->|"New Record"| E{Accounting Guardrail}
    E -->|"sum(Debits) != sum(Credits)"| F
    E -->|"sum(Debits) == sum(Credits)"| G[Gold Layer / Valid DataFrame]
    
    style F fill:#f9d0c4,stroke:#333,stroke-width:2px;
    style G fill:#d4f1f4,stroke:#333,stroke-width:2px;
```

## Technology Stack Rationale

- **Polars over Pandas**: Polars is built in Rust natively using Arrow arrays, providing superior memory efficiency (`pl.scan_csv`) allowing us to process gigabytes of transactions on a single node without OOM (Out Of Memory) issues compared to standard Pandas.
- **Pydantic V2 over Vanilla Classes**: We need to enforce types STRICTLY before data touches internal math logic. Python class type hinting is a suggestion; Pydantic V2 is enforced validation.
- **`decimal.Decimal` over `Float64`**: Floating point arithmetic (e.g., `0.1 + 0.2 = 0.30000000000000004`) destroys financial accounting. Pure string-to-decimal ingestion guarantees zero discrepancy in Ledger Balancing.

## Key Features

1. **Accounting Guardrail Engine**: Rejects unbalanced batches systematically via `sum(debits) = sum(credits)`.
2. **SHA-256 Idempotency**: Automatically creates a deterministic fingerprint for each row. Identical logs are caught and routed to quarantine before they replicate into the Silver layer.
3. **Structured JSON Logging**: Replacing `print()` statements with an Audit Trail via Python's native `logging` configured with a JSON formatter, pushing `transaction_id` records directly for ELK/Datadog integration.

## How to Run & Verify

**1. Setup Environment**
```bash
python -m venv .venv
# Activate your environment
source .venv/bin/activate  # Or .\.venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
```

**2. Run Test Suite (Integrity Verification)**
```bash
pytest tests/ -v
```

**3. Run Orchestrator Flow**
```bash
python main.py
```
