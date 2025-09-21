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

Mermaid diagram:

```mermaid
flowchart LR
  A[Log Generator] --> B[Ray Object Store]
  B --> C[Parser Tasks\n(@ray.remote)]
  C --> D[Aggregator Actor\n(@ray.remote(class))]
  D --> E[Anomaly Detector Actor\n(@ray.remote(class))]
  E --> F[Alerting Actor\n(@ray.remote(class))]
```
