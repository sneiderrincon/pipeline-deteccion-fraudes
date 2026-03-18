# 🚨 Fraud Detection Pipeline

> **Pipeline de detección de fraudes en tiempo real sobre transacciones financieras**, construido con tecnologías de Big Data de nivel productivo: Apache Kafka, Apache Spark Structured Streaming, XGBoost, ClickHouse y Grafana — completamente orquestado con Docker Compose en dos modos: desarrollo local (8 GB RAM) y producción en nube (clúster de 3 brokers).

Este proyecto demuestra la integración end-to-end de un sistema de streaming con machine learning embebido, desde la ingesta de datos hasta la visualización de alertas en tiempo real.

```
Kaggle CSV → Java Producer → Kafka → Spark Streaming → XGBoost → ClickHouse → Grafana
```

---

## Architecture

| Component | Local (dev) | Cloud (prod) |
| --- | --- | --- |
| Kafka brokers | 1 | 3 (KRaft) |
| Spark workers | 1 (3 GB) | 3 (4 GB each) |
| Replication factor | 1 | 3 |
| Est. RAM needed | ~5.5 GB | 16 GB+ |

---

## Model

XGBoost classifier trained on the [Kaggle Credit Card Fraud Detection dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection) (1.2M training / 555k test rows). Feature engineering mirrors Simone Brancato's pipeline:

* Temporal: `hour`, `day_of_week`, `is_night`, `is_weekend`
* Geospatial: Haversine distance user → merchant, `log_distance`
* Log transforms: `log_amt`, `log_city_pop`
* Behavioural: `tx_count_user`, `amt_mean_user` (per micro-batch)
* Categorical: `gender`, `category`, `state`, `job` (OrdinalEncoder)

Classification threshold: **0.3** (optimised for recall).

---

## Prerequisites

* **Docker** ≥ 24 + **Docker Compose** v2
* **Java 11** + **Maven** (for producer build)
* **Python 3.9+** (for model training only)
* Kaggle dataset downloaded (train + test CSVs)

---

## Local Setup (8 GB RAM)

### 1. Clone and prepare data

```bash
git clone https://github.com/sneiderrincon/pipeline-deteccion-fraudes
cd pipeline-deteccion-fraudes
```

> **⚠️ DATASET NOT INCLUDED**
> The datasets are not included in this repository due to size limitations.
> Download them from [Kaggle — Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) and place them in `data/`:
>
> ```
> data/
> ├── fraudTrain.csv
> ├── fraudTest.csv
> └── README.txt
> ```

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
| --- | --- | --- |
| Grafana dashboard | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Spark UI | http://localhost:8081 | — |
| Spark job UI | http://localhost:4040 | — |
| ClickHouse HTTP | http://localhost:8123 | default / password |

---

## Cloud Setup (Oracle Cloud Always Free / GCP / AWS)

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
git clone https://github.com/sneiderrincon/pipeline-deteccion-fraudes
cd pipeline-deteccion-fraudes
./download_jars.sh
python3 model/train_model.py    # or scp the pkl from local

# Launch with production override (3 brokers, 3 workers)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
```

Replace `localhost` with your VM's public IP. Secure with a reverse proxy (Nginx + Certbot) before exposing to the internet.

---

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker list |
| `KAFKA_TOPIC` | `fraud-transactions` | Topic name |
| `DELAY_MIN_MS` / `DELAY_MAX_MS` | `50` / `300` | Producer message delay range |
| `FRAUD_THRESHOLD` | `0.3` | Classification threshold |
| `CLICKHOUSE_PASSWORD` | `password` | Change in production |
| `MODEL_PATH` | `/opt/.../model/xgb_full_pipeline.pkl` | Path to serialised pipeline |

---

## Project Structure

```
pipeline-deteccion-fraudes/
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

## Grafana Dashboard Panels

* **Total Transactions** / **Detected Frauds** / **Fraud Rate %** — stat cards
* **Transactions over time** (30 s granularity) — time series
* **Frauds over time** (30 s granularity) — time series
* **Transaction amount distribution** — histogram
* **Fraud by category** — pie chart
* **Detected frauds detail table** — timestamp, customer, city, amount, probability score

---

## Troubleshooting

**Spark job crashes on restart with offset error**

When restarting after the containers were previously stopped, Kafka may have reset the topic offsets. Spark detects the mismatch and fails by default. Fix: add `failOnDataLoss = false` to the `readStream` in `spark/code/streaming_job.py`:

```python
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
```

Then apply the change without rebuilding:

```bash
docker cp spark/code/streaming_job.py spark-submit:/opt/spark/apps/streaming_job.py
docker compose restart spark-submit
```

**Grafana dashboard shows no data**

The dashboard queries use `inserted_at` (the real insertion timestamp) as the time filter — not `trans_date_trans_time`, which reflects the original 2019–2020 dataset dates and will always fall outside the `now-1h` window. If panels are blank, verify the time range in Grafana covers the last hour and that `inserted_at` is populated:

```bash
docker exec -it clickhouse clickhouse-client --password password \
  --query "SELECT min(inserted_at), max(inserted_at), count() FROM fraud_detection.transactions"
```

**sklearn/XGBoost version warnings on startup**

These warnings appear if the model was trained with a different scikit-learn version than what runs inside the container. The pipeline still works correctly. To eliminate them, retrain the model inside the container environment or pin the versions in your local environment to match.

---



Adapted from [SimoneBrancato/Fraud-Detection-Pipeline](https://github.com/SimoneBrancato/Fraud-Detection-Pipeline).

---

## Contact

**Sneider Rincón Castrillón**
🔗 [github.com/sneiderrincon](https://github.com/sneiderrincon)
📧 sneider.rincon@udea.edu.co
📱 +57 310 658 6063
