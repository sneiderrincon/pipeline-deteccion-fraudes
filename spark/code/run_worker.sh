#!/bin/bash
set -e
echo "[spark-worker] Installing Python dependencies..."
pip install --quiet \
    xgboost==2.0.3 \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    joblib==1.4.2 \
    clickhouse-connect==0.7.16

echo "[spark-worker] Starting Spark worker..."
exec /opt/bitnami/scripts/spark/run.sh
