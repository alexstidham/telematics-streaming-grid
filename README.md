# High-Throughput Real-Time Telematics Ingestion & CEP Grid

A production-grade, fault-tolerant distributed streaming architecture designed to ingest, analyze, and store sub-second vehicle telematics. This system uses a hybrid dual-tier storage strategy (Hot/Cold path) to serve ultra-low latency operational metrics while executing complex event processing (CEP) and time-series analytical aggregations.

## System Architecture

The infrastructure decouples ingestion, real-time alerting, and historical cold-storage archiving to handle bursty edge signaling smoothly.

[ Edge Simulator ] ──(High-Velocity Signals)──► [ Apache Kafka ]
│
▼
[ Stream Processor (CEP) ]
╱                  ╲
(Hot Path) ╱                    ╲ (Cold Path)
▼                      ▼
[ Redis Cache ]       [ TimescaleDB ]
(Sub-2ms Updates)     (Time-Series Hypertable)
▲                      ▲
│                      │
└────── [ FastAPI ] ───┘

### Infrastructure Stack & Design Rationale

* **Edge Telematics Layer (`simulator.py`):** Simulates a multi-asset fleet executing high-frequency coordinate, thermal, and mechanical streaming data patterns.
* **Ingestion Backbone (Apache Kafka):** Acts as an immutable distributed event log to safely absorb bursty write-heavy workloads and decouple edge ingestion from downstream processing.
* **Complex Event Processing (`processor.py`):** A stateful stream computation engine running sliding window evaluations on vibration thresholds to detect mechanical asset degradation in real time.
* **Operational Hot Tier Cache (Redis):** Implements an in-memory caching layer to enable immediate, ultra-low latency operational lookups for live asset statuses.
* **Analytical Storage Lake (TimescaleDB):** Leverages time-partitioned Hypertables to execute analytical aggregation queries across millions of structured logs without degrading disk read/write performance.
* **API Microservice Gateway (FastAPI):** Exposes a unified query interface, instantly routing operational lookups to Redis and analytical reporting to TimescaleDB.

---

## Features

* **Dual-Tier Storage Strategy (Hot vs. Cold):** Separates concerns by routing sub-2ms state lookups to Redis while keeping heavy historical time-series analytics off the main execution loop via TimescaleDB.
* **Resilient Infrastructure Orchestration:** Implements stateful connection-retry fallback loops in Python to gracefully resolve distributed container boot race conditions during startup.
* **Decoupled Event Streaming:** Utilizes a Kafka broker at the ingestion edge to eliminate packet loss and tolerate downstream maintenance windows.
* **Unbuffered Stream Telemetry:** Configured via custom Docker network parameters to force immediate I/O flushing for real-time console telemetry visibility.

---

## Getting Started

The entire grid is fully containerized, network-isolated, and ready to spin up out of the box.

### Prerequisites
* Docker Desktop installed, configured, and running locally.

### 1. Boot the Distributed Grid Infrastructure
Clone this repository, navigate to the project directory, and spin up the environment mesh:
```bash
docker compose up
```
_Note: The Python microservices feature resilient connection retry loops, meaning they will automatically await full database and broker initialization without crashing._

### 2. Launch the Edge Stream Simulator
In a separate terminal window on your local machine, activate your virtual environment and kick off the telematics asset stream generator: 
```bash
source .venv/bin/activate
python3 simulator.py
```
### 3. Verify and Query the API Gateways
Open your web browser or execute via `curl` to view live outputs passing through the dual-tier storage layers:
* ** Analytical Pred
