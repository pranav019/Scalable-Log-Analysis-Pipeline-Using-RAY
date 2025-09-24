# Scalable Log Analysis Pipeline (Ray)

**Scalable Log Analysis Pipeline** — A production-grade example demonstrating how to build a high-throughput, distributed log ingestion and analytics system using **Ray**. This project simulates massive web-server logs, ingests and parses them in parallel, aggregates metrics with stateful Actors, and detects anomalies (e.g., spikes in error rate or unusual user activity) in near real-time.

This repository is designed for developers who want to showcase distributed computing, parallel data processing, and Ray primitives (remote tasks, actors, object store, Ray Datasets).

---

## Problem Solved

Modern applications produce large volumes of heterogeneous logs. Centralized processing can be slow or expensive. This project demonstrates how to scale log parsing, enrichment, and analytics using Ray to:

- Parse and enrich logs in parallel.
- Maintain stateful aggregates (per-service, per-endpoint, sliding windows).
- Detect anomalies quickly and send alerts via an actor-backed alerting service.
- Efficiently use Ray's object store to share parsed data between workers without copying.

---

## How It Works (High-level architecture)

1. **Log Generator** (simulated) produces compressed batches of log lines.
2. **Ingestion Pipeline** reads batches and places them in Ray's object store.
3. **Parsing Workers** (`@ray.remote` functions) parse/enrich logs in parallel.
4. **Aggregator Actor** (`@ray.remote(class)`) maintains aggregate metrics and sliding windows.
5. **Anomaly Detector Actor** monitors aggregated metrics and triggers alerts.
6. **Alerting Actor** receives alerts and records them (could be extended to send webhooks/Slack).

## Files Information :-

**main.py**: Orchestrator — starts Ray, sets up components, runs a demo ingestion, and shuts down Ray.
**src/data_processing.py**: Code to generate synthetic logs and to stream/load batches.
**src/core_logic.py**: Contains all Ray remote functions and Actors (parsers, aggregator, detector, alerting).
**src/utils.py**: Helpers (regex-based log parser, sliding-window aggregator, typed dataclasses).

## Setup & Execution

**Python version**: 3.9+ (recommended 3.10 or 3.11)
**Install dependencies**: pip install -r requirements.txt
**Run (demo)**:python main.py
**The demo will**:
Start a local Ray instance.
Generate simulated log batches.
Parse and aggregate them in parallel.
Detect anomalies and print/store alerts.

## Files & Roles

- `requirements.txt`: Python packages required to run.
- `main.py`: Entry point; contains a `run_demo()` function demonstrating ingestion and processing.
- `src/data_processing.py`: `LogGenerator`, `batch_logs()` and helpers for producing log lines.
- `src/core_logic.py`: `parse_logs_remote`, `AggregatorActor`, `AnomalyDetectorActor`, `AlertingActor`.
- `src/utils.py`: `LogLine` dataclass, parsing utilities, sliding-window logic.

## Design & Ray Usage Notes

---

- **`ray.init()` / `ray.shutdown()`**: Proper lifecycle is handled in `main.py`.
- **`@ray.remote` functions**: `parse_logs_remote` is stateless and scaled to many parallel workers.
- **`@ray.remote(class)`**: `Aggregator`, `Detector`, and `Alerting` are stateful actors that maintain in-memory sliding windows and persistent aggregates.
- **Object store**: Batches of raw log lines and parsed record lists are stored and passed as Ray object references to avoid copies. Parsers return these object references for downstream consumption.
- **Ray Datasets**: This was not required for the demo. However, the design is compatible with Ray Datasets for scaling to a production pipeline, and extension points are commented in the code.
