# WarpSchema: The Self-Healing Data Gateway — Integration Guide

> **Trial Edition** — evaluate WarpSchema locally with your own data pipelines.

---

## Core Capabilities

| Capability | Description |
|---|---|
| **Schema Drift Detection** | Automatically identifies missing keys, type mismatches, and field renames across streaming payloads. |
| **Self-Healing Transforms** | Applies deterministic + ML-powered corrections (Levenshtein matching, ONNX-accelerated embeddings) to repair payloads before they hit your data lake. |
| **Human-in-the-Loop (HITL)** | Uncertain corrections surface in a Streamlit dashboard for human approval, building a feedback loop that improves accuracy over time. |
| **Zero-Waste Compute** | Only anomalous payloads enter the ML pipeline; clean data passes through at wire speed with negligible overhead. |
| **ONNX Runtime Acceleration** | Inference runs on optimised ONNX models — no GPU required for the trial tier. |

---

## Architecture Overview

```
┌──────────────┐     ┌──────────────────────────────────────────┐
│ Your Pipeline│     │  WarpSchema Trial Stack (Docker Compose) │
│              │     │                                          │
│  Kafka / SQS │────▶│  ┌────────────────┐   ┌──────────┐      │
│  Adapter     │ HTTP│  │ FastAPI Gateway │──▶│ Redis    │      │
│              │ :8000  │ (Ingest + Heal) │   │ (Cache)  │      │
└──────────────┘     │  └───────┬────────┘   └──────────┘      │
                     │          │                               │
                     │          ▼                               │
                     │  ┌───────────────┐   ┌──────────────┐   │
                     │  │ PostgreSQL    │   │ Streamlit    │   │
                     │  │ (Schema Reg.) │   │ HITL Dashboard│   │
                     │  └───────────────┘   │ :8501        │   │
                     │                      └──────────────┘   │
                     └──────────────────────────────────────────┘
```

---

## 3-Step Quickstart

### Prerequisites

- Docker ≥ 24 and Docker Compose V2
- Python ≥ 3.10 (for adapters and `warp-check.py`)
- A trial license key — request one at **<https://warpschema.io/trial>**

### Step 1 — Clone

```bash
git clone https://github.com/warpschema/warpschema-quickstart.git
cd warpschema-quickstart
```

### Step 2 — Configure

```bash
cp .env.example .env
# Open .env and paste your trial license key:
#   WARPSCHEMA_LICENSE_KEY=trial_abc123...
```

### Step 3 — Launch

```bash
docker compose up -d
```

The gateway will be available at:

| Service | URL |
|---|---|
| Ingestion API | `http://localhost:8000` |
| API docs (Swagger) | `http://localhost:8000/docs` |
| HITL Dashboard | `http://localhost:8501` |

#### Send test data (no Kafka/SQS required)

```bash
cd adapters
pip install -r requirements.txt
python kafka_adapter.py --generate-test-data
```

#### Run the local ROI assessment (no Docker required)

```bash
python warp-check.py sample_payloads.json --cost-per-minute 75 --mttr 12
```

---

## Connecting Your Pipeline

### Kafka

```bash
python adapters/kafka_adapter.py \
  --broker your-broker:9092 \
  --topic your-events-topic
```

### AWS SQS

```bash
python adapters/sqs_adapter.py \
  --queue-url https://sqs.us-east-1.amazonaws.com/123456789/your-queue
```

### Direct HTTP

Any client that can POST JSON can send payloads directly:

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"event_id":"test-1","timestamp":1718000000,"user_id":1,"action":"click","metadata":{}}'
```

---

## Project Structure

```
warpschema-quickstart/
├── docker-compose.yml      # Full trial stack
├── .env.example            # Environment variable template
├── adapters/
│   ├── kafka_adapter.py    # Kafka → WarpSchema forwarder
│   ├── sqs_adapter.py      # SQS → WarpSchema forwarder
│   └── requirements.txt    # Adapter Python dependencies
├── warp-check.py           # Offline ROI / drift analysis CLI
├── sample_payloads.json    # Example payloads for warp-check
└── README.md               # This file
```

---

## Production Licensing

The trial edition is limited to:

- **1,000 payloads per hour** ingestion cap
- **Single-node** deployment
- **Community support** via GitHub Issues

To remove these limits and unlock multi-node clustering, SSO integration,
custom ONNX model uploads, and dedicated support:

📧 **sales@warpschema.io**
🌐 **<https://warpschema.io/pricing>**

---

## License

The integration code in this repository (adapters, `warp-check.py`, and
configuration files) is released under the [MIT License](LICENSE).

The WarpSchema gateway Docker image is proprietary software governed by the
WarpSchema Enterprise License Agreement included in the image.

---

*Built by the WarpSchema team — reliability engineering for the streaming era.*
