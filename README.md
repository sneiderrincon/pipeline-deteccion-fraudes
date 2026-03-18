# Fraud Detection Pipeline

A real-time fraud detection system for financial transactions using **Apache Kafka**, **Apache Spark**, **ClickHouse**, and **Grafana** — fully Dockerized, with a lightweight local mode (8 GB RAM) and a production cloud mode (3-broker cluster).

```
Kaggle CSV → Java Producer → Kafka → Spark Streaming → XGBoost → ClickHouse → Grafana
```

## Architecture

| Component | Local (dev) | Cloud (prod) |
|-----------|-------------|--------------|
| Kafka brokers | 1 | 3 (KRaft) |
| Spark workers | 1 (3 GB) | 3 (4 GB each) |
| Replication factor | 1 | 3 |
| Est. RAM needed | ~5.5 GB | 16 GB+ |

## Model

XGBoost classifier trained on the [Kaggle Credit Card Fraud Detection dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection) (1.2M training / 555k test rows). Feature engineering mirrors Simone Brancato's pipeline:

- Temporal: `hour`, `day_of_week`, `is_night`, `is_weekend`
- Geospatial: Haversine distance user → merchant, `log_distance`
- Log transforms: `log_amt`, `log_city_pop`
- Behavioural: `tx_count_user`, `amt_mean_user` (per micro-batch)
- Categorical: `gender`, `category`, `state`, `job` (OrdinalEncoder)

Classification threshold: **0.3** (optimised for recall).

---

## Prerequisites

- **Docker** ≥ 24 + **Docker Compose** v2
- **Java 11** + **Maven** (for producer build)
- **Python 3.9+** (for model training only)
- Kaggle dataset downloaded (train + test CSVs)

---

## Local setup (8 GB RAM)

### 1. Clone and prepare data

```bash
git clone https://github.com/<your-user>/fraud-detection-pipeline
cd fraud-detection-pipeline
mkdir -p data
# Place fraudTrain.csv and fraudTest.csv in data/
```

### 2. Download Spark-Kafka JARs

```bash
chmod +x download_jars.sh
./download_jars.sh
```

### 3. Train the model

```bash
pip install xgboost scikit-learn pandas joblib
python model/train_model.py
# Output: model/xgb_full_pipeline.pkl
```

### 4. Start the pipeline

```bash
docker compose up --build
```

### 5. Access services

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana dashboard | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Spark UI | http://localhost:8081 | — |
| Spark job UI | http://localhost:4040 | — |
| ClickHouse HTTP | http://localhost:8123 | default / password |

---

## Cloud setup (Oracle Cloud Always Free / GCP / AWS)

Oracle Cloud Always Free gives you **4 OCPUs + 24 GB RAM** — enough for the full 3-broker cluster.

### Provision the VM

1. Create an **Ampere A1** instance (4 OCPUs, 24 GB RAM, Ubuntu 22.04)
2. Open ports: `3000`, `8080`, `8081`, `4040`, `9092`, `8123`

### Install Docker on the VM

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker compose version   # verify v2
```

### Deploy production cluster

```bash
git clone https://github.com/<your-user>/fraud-detection-pipeline
cd fraud-detection-pipeline
./download_jars.sh
python3 model/train_model.py    # or scp the pkl from local

# Launch with production override (3 brokers, 3 workers)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
```

### Access remotely

Replace `localhost` with your VM's public IP. Secure with a reverse proxy (Nginx + Certbot) before exposing to the internet.

---

## Environment variables

All services are configurable via environment variables — no hardcoded values.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker list |
| `KAFKA_TOPIC` | `fraud-transactions` | Topic name |
| `DELAY_MIN_MS` / `DELAY_MAX_MS` | `50` / `300` | Producer message delay range |
| `FRAUD_THRESHOLD` | `0.3` | Classification threshold |
| `CLICKHOUSE_PASSWORD` | `password` | Change in production |
| `MODEL_PATH` | `/opt/.../model/xgb_full_pipeline.pkl` | Path to serialised pipeline |

---

## Project structure

```
fraud-detection-pipeline/
├── docker-compose.yml            # Local (1 broker, 1 worker)
├── docker-compose.prod.yml       # Cloud override (3 brokers, 3 workers)
├── download_jars.sh              # Downloads Spark-Kafka connector JARs
├── data/                         # Place fraudTrain.csv + fraudTest.csv here
├── producer/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/fraudpipeline/
│       └── TransactionProducer.java
├── kafka/code/
│   └── init-topic.sh
├── spark/
│   ├── code/
│   │   ├── streaming_job.py      # Main Spark Structured Streaming job
│   │   ├── run_worker.sh
│   │   └── submit.sh
│   └── jars/                     # Populated by download_jars.sh
├── model/
│   ├── train_model.py            # XGBoost training script
│   └── xgb_full_pipeline.pkl     # Generated after training
└── grafana/
    ├── Dockerfile
    └── provisioning/
        ├── datasources/clickhouse.yaml
        └── dashboards/
            ├── dashboard.yaml
            └── fraud_dashboard.json
```

---

## Grafana dashboard panels

- **Total Transactions** / **Detected Frauds** / **Fraud Rate %** — stat cards
- **Transactions over time** (30 s granularity) — time series
- **Frauds over time** (30 s granularity) — time series
- **Transaction amount distribution** — histogram
- **Fraud by category** — pie chart
- **Detected frauds detail table** — timestamp, customer, city, amount, probability score

---

## Contacts

Built for the UNAD Big Data course portfolio — adapted from [SimoneBrancato/Fraud-Detection-Pipeline](https://github.com/SimoneBrancato/Fraud-Detection-Pipeline).
