# 🌀 Data in Motion

**Real-Time Intelligent Tiering, Streaming Analytics & ML Dashboard**

**Data in Motion** is a full-stack, real-time data management and analytics platform built for the **NetApp Data-in-Motion Hackathon (2025)**.
It intelligently migrates datasets across **HOT / WARM / COLD tiers** and between **On-Prem / Private / Public** cloud storage layers — powered by live Kafka streaming, machine learning–based scoring, and a visually interactive dashboard.

---

## 📘 Overview

This project demonstrates how modern intelligent data systems:

* Stream dataset activity in **real time** using **Kafka (via Redpanda)**
* Aggregate and persist usage patterns in **SQLite**
* Predict tier transitions using a **lightweight ML model**
* Migrate and encrypt data across **simulated multi-cloud tiers**
* Provide an **interactive Streamlit dashboard** for visualization, analytics, and control

---

## 🏗️ Architecture

```
Dataset Events → FastAPI Producer → Redpanda Stream → Kafka Aggregator Consumer
         ↓                                   ↓
       SQLite ←-------------------------- EWMA + ML Predictor
         ↓
  Auto-Scheduler + Migrator → HOT / WARM / COLD tiers
         ↓
      Streamlit Dashboard (Visualization)
```

**Core Components**

| Component                      | Description                                                                |
| ------------------------------ | -------------------------------------------------------------------------- |
| `app/main.py`                  | FastAPI backend — APIs, ML scoring, auto-scheduler, and migrations         |
| `app/kafka_aggregator.py`      | Kafka consumer that processes access events and updates dataset analytics  |
| `app/ml_predictor.py`          | Lightweight ML predictor — computes EWMA score and recommends tier changes |
| `app/db.py`, `app/models.py`   | SQLite ORM models for datasets, access events, and migration history       |
| `app/dashboard.py`             | Streamlit dashboard for real-time visualization and control                |
| `app/encryption.py`            | Fernet encryption layer securing data during migrations                    |
| `scripts/test_instructions.sh` | End-to-end verification script for demo validation                         |

---

## ⚙️ Installation

> 🧩 Tested on **Python 3.12+**, **macOS/Linux**

```bash
# Clone the repository
https://github.com/rish0407/Net-App_Hackathon.git
cd data-in-motion

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # macOS/Linux
# .venv\Scripts\activate        # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## 🔐 Generate & Use Fernet Encryption Key

Each setup must have its **own unique encryption key** to secure dataset files.

### 1️⃣ Generate a key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > encryption.key
chmod 600 encryption.key
```

### 2️⃣ Load it into your environment

**Option A — Temporary session:**

```bash
export ENCRYPTION_KEY="$(cat encryption.key)"
```

**Option B — Permanent (recommended for judges):**

```bash
cp .env.example .env
echo "ENCRYPTION_KEY=$(cat encryption.key)" >> .env
```

---

## 🧰 Configuration

Create a `.env` file in your project root (or edit the existing one):

```
API_KEY=OCD_dataflow
ENCRYPTION_KEY=<your_fernet_key>
KAFKA_BOOTSTRAP=localhost:9092
BASE_URL=http://localhost:8000
```

All services automatically read from `.env`.

---

## ⚡ Run Redpanda (Kafka-Compatible Stream)

Below is a minimal **Redpanda** setup (acts as your local Kafka broker).

```yaml
version: "3.8"
services:
  redpanda:
    image: redpandadata/redpanda:latest
    container_name: redpanda
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp 1
      - --memory 1G
      - --reserve-memory 0M
      - --node-id 0
      - --check=false
    ports:
      - "9092:9092"      # Kafka API
      - "9644:9644"      # Admin API
```

Start it:

```bash
docker compose up -d redpanda
```

---

## ▶️ Running the Application (Local Setup)

You’ll need **3 terminals** for full functionality:

### 🖥️ Terminal 1 — Backend (FastAPI)

```bash
source .venv/bin/activate
export API_KEY=OCD_dataflow
export ENCRYPTION_KEY="$(cat encryption.key)"
export KAFKA_BOOTSTRAP=localhost:9092
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### ⚙️ Terminal 2 — Kafka Aggregator (Consumer)

```bash
source .venv/bin/activate
export KAFKA_BOOTSTRAP=localhost:9092
python -m app.kafka_aggregator
```

### 📊 Terminal 3 — Streamlit Dashboard

```bash
source .venv/bin/activate
streamlit run app/dashboard.py --server.port 8501
```

Now open [http://localhost:8501](http://localhost:8501) 🌐

---

## 🧩 Example Workflow

- Open the dashboard.
- **Create a dataset** (give it a name and size).
- **Send events** using the dashboard or this cURL command:

```bash
for i in $(seq 1 20); do
  curl -s -X POST "http://localhost:8000/api/stream/send" \
    -H "Content-Type: application/json" \
    -H "x-api-key: OCD_dataflow" \
    -d '{"dataset_id":1,"access_type":"read"}' >/dev/null
done
```

- Watch the **aggregator logs** update access counts in real-time.
- View **recommendations** for tier migration.
- Trigger or wait for **auto-migration** to different tiers or storages.
- See migrations visualized live in the **dashboard Sankey & timeline charts**.

---

## 🎁 Demo 


https://github.com/user-attachments/assets/6bff7562-4b3e-4b4f-b294-0e7e2c8fe1a8



## 🧠 Key Features

| Feature                         | Description                                            |
| ------------------------------- | ------------------------------------------------------ |
| ⚡ **Real-Time Kafka Streaming** | Streams dataset activity continuously via Redpanda     |
| 🧮 **ML-based Tier Prediction** | EWMA + recency model recommends optimal tier           |
| 🔁 **Auto Migration Engine**    | Periodically migrates between HOT / WARM / COLD        |
| ☁️ **Multi-Cloud Simulation**   | Moves encrypted data across On-Prem / Private / Public |
| 🔐 **Data Security**            | End-to-end Fernet encryption at rest                   |
| 📈 **Visual Dashboard**         | Streamlit-based analytics with live charts             |
| 🧩 **Automated Testing**        | Verifies full data flow in seconds                     |

---

## 📊 Tech Stack

**Backend:** FastAPI, Uvicorn \
**Streaming:** Redpanda (Kafka-compatible), Aiokafka \
**Database:** SQLite + SQLAlchemy \
**Machine Learning:** EWMA predictor, NumPy \
**Visualization:** Streamlit + Plotly \
**Security:** Fernet Encryption (Cryptography) \
**Automation:** Bash Testing Script

---

## 🧩 Troubleshooting

| Issue                   | Cause                              | Fix                                               |
| ----------------------- | ---------------------------------- | ------------------------------------------------- |
| `No events processed`   | Aggregator not running             | Start `python -m app.kafka_aggregator`            |
| Dashboard shows blank   | No datasets created                | Use “Create dataset” in dashboard                 |
| Encryption error        | Key not set                        | Run `export ENCRYPTION_KEY=$(cat encryption.key)` |
| Migration not triggered | Low ML score or scheduler interval | Send more events or restart backend               |
| SQLite locked           | Concurrent writes                  | Restart backend process                           |

---

## 👩‍💻 Team

**Team Name:** *OCD* \
Developed for **NetApp Data-in-Motion Hackathon 2025**\
**Rishita Khare** (Team Leader) \
**Anushka Aggarwal** \
**Riddhi Agarwal**   

---

## 🏁 Summary

**Data in Motion** unifies **real-time streaming, intelligent ML-driven tiering, encrypted storage migration**, and a **visual control dashboard** — providing a clean, modular foundation for next-generation intelligent data management systems.


