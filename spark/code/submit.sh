#!/bin/bash
set -e

echo "[spark-submit] Installing Python dependencies..."
pip install --quiet \
    xgboost==2.1.3 \
    scikit-learn==1.7.2 \
    pandas==2.2.2 \
    joblib==1.4.2 \
    clickhouse-connect==0.7.16

echo "[spark-submit] Waiting 25s for workers to register..."
sleep 25

echo "[spark-submit] Submitting streaming job..."
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "fraud-detection-streaming" \
  --conf spark.sql.streaming.checkpointLocation=/opt/spark/work-dir/checkpoint \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.driver.host=spark-submit \
  --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/extra-jars/kafka-clients-3.7.0.jar,\
/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/extra-jars/commons-pool2-2.11.1.jar \
  /opt/spark/apps/streaming_job.py
