"""
streaming_job.py — Spark Structured Streaming fraud detection
─────────────────────────────────────────────────────────────
Versión corregida: Escritura directa a fraud_detection.transactions
"""

import os
import joblib
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",             "fraud-transactions")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST",         "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT",     "8123"))
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD",     "password")
MODEL_PATH      = os.getenv("MODEL_PATH", "/opt/spark/model/xgb_full_pipeline.pkl")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD",   "0.3"))
CHECKPOINT_DIR  = "/opt/spark/work-dir/checkpoint"

# ── Feature columns ───────────────────────────────────────────────────────────
NUMERIC_COLS = [
    'age', 'hour', 'day_of_week',
    'is_night', 'is_weekend',
    'log_amt', 'log_city_pop', 'log_distance',
    'tx_count_user', 'amt_mean_user'
]
CAT_COLS     = ['gender', 'category', 'state', 'job']
FEATURE_COLS = CAT_COLS + NUMERIC_COLS

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

def engineer_features(pdf: pd.DataFrame) -> pd.DataFrame:
    df = pdf.copy()
    df['dob'] = pd.to_datetime(df['dob'])
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])

    df['age']         = (df['trans_date_trans_time'] - df['dob']).dt.days // 365
    df['hour']        = df['trans_date_trans_time'].dt.hour
    df['day_of_week'] = df['trans_date_trans_time'].dt.dayofweek
    df['is_night']    = df['hour'].apply(lambda x: 1 if x < 6 or x >= 22 else 0)
    df['is_weekend']  = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

    df['distance_user_to_merch'] = df.apply(
        lambda r: haversine(r['lat'], r['long'], r['merch_lat'], r['merch_long']),
        axis=1
    )
    df['log_amt']      = np.log1p(df['amt'])
    df['log_city_pop'] = np.log1p(df['city_pop'])
    df['log_distance'] = np.log1p(df['distance_user_to_merch'])

    df['user_id'] = df['cc_num'].astype(str)
    df.sort_values(['user_id', 'trans_date_trans_time'], inplace=True)
    df['tx_count_user'] = df.groupby('user_id').cumcount()
    df['amt_mean_user'] = df.groupby('user_id')['amt'].transform(
        lambda x: x.rolling(10, min_periods=1).mean()
    )
    return df

def process_batch(batch_df, batch_id, model, ch_client):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    pdf = engineer_features(pdf)

    # Inferencia
    pdf["fraud_probability"] = model.predict_proba(pdf[FEATURE_COLS])[:, 1]
    pdf["fraud_prediction"]  = (pdf["fraud_probability"] >= FRAUD_THRESHOLD).astype(int)

    # Seleccionar columnas exactas para ClickHouse
    out = pdf[[
        "idx", "trans_date_trans_time", "cc_num", "merchant", "category",
        "amt", "first", "last", "gender", "city", "state", "lat", "long",
        "city_pop", "job", "trans_num", "unix_time", "merch_lat", "merch_long",
        "age", "hour", "day_of_week", "is_night", "is_weekend",
        "distance_user_to_merch", "log_amt", "log_city_pop", "log_distance",
        "user_id", "tx_count_user", "amt_mean_user",
        "fraud_probability", "fraud_prediction"
    ]].copy()

    # Asegurar formato de fecha para ClickHouse
    out["trans_date_trans_time"] = pd.to_datetime(out["trans_date_trans_time"])

    # INSERCIÓN EN CLICKHOUSE
    ch_client.insert_df("transactions", out)
    
    fraudes = int(out["fraud_prediction"].sum())
    print(f"[batch {batch_id}] {len(out)} rows | fraudes={fraudes} guardados en ClickHouse")

def main():
    spark = SparkSession.builder \
        .appName("fraud-detection-streaming") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[main] Cargando modelo desde {MODEL_PATH}...")
    model = joblib.load(MODEL_PATH)

    import clickhouse_connect
    print(f"[main] Conectando a ClickHouse en {CLICKHOUSE_HOST}...")
    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database="fraud_detection",  # <--- Base de datos correcta
        password=CLICKHOUSE_PASS
    )

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema_ddl = "idx INT, trans_date_trans_time STRING, cc_num STRING, merchant STRING, category STRING, amt DOUBLE, first STRING, last STRING, gender STRING, street STRING, city STRING, state STRING, zip STRING, lat DOUBLE, " \
    "long DOUBLE, city_pop BIGINT, job STRING, dob STRING, trans_num STRING, unix_time BIGINT, merch_lat DOUBLE, merch_long DOUBLE, is_fraud INT"

    parsed = raw.select(
        from_csv(col("value").cast("string"), schema_ddl).alias("d")
    ).select("d.*")

    query = parsed.writeStream \
        .foreachBatch(lambda df, bid: process_batch(df, bid, model, ch)) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[main] Streaming iniciado. Esperando datos...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
