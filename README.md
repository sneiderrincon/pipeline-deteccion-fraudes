🚨 Fraud Detection Pipeline

Pipeline de detección de fraudes en tiempo real sobre transacciones financieras, construido con tecnologías de Big Data de nivel productivo: Apache Kafka, Apache Spark Structured Streaming, XGBoost, ClickHouse y Grafana — completamente orquestado con Docker Compose en dos modos: desarrollo local (8 GB RAM) y producción en nube (clúster de 3 brokers).

Este proyecto demuestra la integración end-to-end de un sistema de streaming con machine learning embebido, desde la ingesta de datos hasta la visualización de alertas en tiempo real.
Kaggle CSV → Java Producer → Kafka → Spark Streaming → XGBoost → ClickHouse → Grafana

Architecture
ComponentLocal (dev)Cloud (prod)Kafka brokers13 (KRaft)Spark workers1 (3 GB)3 (4 GB each)Replication factor13Est. RAM needed~5.5 GB16 GB+

Model
XGBoost classifier trained on the Kaggle Credit Card Fraud Detection dataset (1.2M training / 555k test rows). Feature engineering mirrors Simone Brancato's pipeline:

Temporal: hour, day_of_week, is_night, is_weekend
Geospatial: Haversine distance user → merchant, log_distance
Log transforms: log_amt, log_city_pop
Behavioural: tx_count_user, amt_mean_user (per micro-batch)
Categorical: gender, category, state, job (OrdinalEncoder)

Classification threshold: 0.3 (optimised for recall).

Prerequisites

Docker ≥ 24 + Docker Compose v2
Java 11 + Maven (for producer build)
Python 3.9+ (for model training only)
Kaggle dataset downloaded (train + test CSVs)


Local Setup (8 GB RAM)
1. Clone and prepare data
bashgit clone https://github.com/sneiderrincon/pipeline-deteccion-fraudes
cd pipeline-deteccion-fraudes

⚠️ DATASET NOT INCLUDED
The datasets are not included in this repository due to size limitations.
Download them from Kaggle — Credit Card Fraud Detection and place them in data/:
data/
├── fraudTrain.csv
├── fraudTest.csv
└── README.txt

2. Download Spark-Kafka JARs
bashchmod +x download_jars.sh
./download_jars.sh
3. Train the model
bashpip install xgboost scikit-learn pandas joblib
python model/train_model.py
# Output: model/xgb_full_pipeline.pkl
4. Start the pipeline
bashdocker compose up --build
5. Access services
ServiceURLCredentialsGrafana dashboardhttp://localhost:3000admin / adminKafka UIhttp://localhost:8080—Spark UIhttp://localhost:8081—Spark job UIhttp://localhost:4040—ClickHouse HTTPhttp://localhost:8123default / password

Cloud Setup (Oracle Cloud Always Free / GCP / AWS)
Oracle Cloud Always Free gives you 4 OCPUs + 24 GB RAM — enough for the full 3-broker cluster.
Provision the VM

Create an Ampere A1 instance (4 OCPUs, 24 GB RAM, Ubuntu 22.04)
Open ports: 3000, 8080, 8081, 4040, 9092, 8123

Install Docker on the VM
bashcurl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker compose version   # verify v2
Deploy production cluster
bashgit clone https://github.com/sneiderrincon/pipeline-deteccion-fraudes
cd pipeline-deteccion-fraudes
./download_jars.sh
python3 model/train_model.py    # or scp the pkl from local

# Launch with production override (3 brokers, 3 workers)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
Replace localhost with your VM's public IP. Secure with a reverse proxy (Nginx + Certbot) before exposing to the internet.

Environment Variables
VariableDefaultDescriptionKAFKA_BOOTSTRAP_SERVERSkafka:9092Kafka broker listKAFKA_TOPICfraud-transactionsTopic nameDELAY_MIN_MS / DELAY_MAX_MS50 / 300Producer message delay rangeFRAUD_THRESHOLD0.3Classification thresholdCLICKHOUSE_PASSWORDpasswordChange in productionMODEL_PATH/opt/.../model/xgb_full_pipeline.pklPath to serialised pipeline

Project Structure
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

Grafana Dashboard Panels

Total Transactions / Detected Frauds / Fraud Rate % — stat cards
Transactions over time (30 s granularity) — time series
Frauds over time (30 s granularity) — time series
Transaction amount distribution — histogram
Fraud by category — pie chart
Detected frauds detail table — timestamp, customer, city, amount, probability score


Contact
Sneider Rincón Castrillón
🔗 github.com/sneiderrincon
📧 sneider.rincon@udea.edu.co
📱 +57 310 658 6063