"""
train_model.py — XGBoost fraud detection model training
────────────────────────────────────────────────────────
Replica exacta del notebook original de Simone Brancato.

Diferencias clave vs versión anterior:
- OneHotEncoder (no OrdinalEncoder) para variables categóricas
- GridSearchCV con StratifiedKFold (3 folds), scoring='recall'
- scale_pos_weight=10 fijo
- GPU automático si CUDA disponible, CPU si no

Requisitos:
    pip install xgboost scikit-learn pandas joblib torch

Uso:
    python model/train_model.py

Output: model/xgb_full_pipeline.pkl
"""

import joblib
import numpy as np
import pandas as pd

from math import radians, cos, sin, asin, sqrt

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import classification_report, confusion_matrix

from xgboost import XGBClassifier

TRAIN_PATH = "data/fraudTrain.csv"
TEST_PATH  = "data/fraudTest.csv"
MODEL_OUT  = "model/xgb_full_pipeline.pkl"
THRESHOLD  = 0.3

NUMERIC_COLS = [
    'age', 'hour', 'day_of_week',
    'is_night', 'is_weekend',
    'log_amt', 'log_city_pop', 'log_distance',
    'tx_count_user', 'amt_mean_user'
]
CAT_COLS = ['gender', 'category', 'state', 'job']
TARGET   = 'is_fraud'


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df['dob']                   = pd.to_datetime(df['dob'])
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])

    df['age']         = (df['trans_date_trans_time'] - df['dob']).dt.days // 365
    df['hour']        = df['trans_date_trans_time'].dt.hour
    df['day_of_week'] = df['trans_date_trans_time'].dt.dayofweek
    df['is_night']    = df['hour'].apply(lambda x: 1 if x < 6 or x >= 22 else 0)
    df['is_weekend']  = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

    df['distance_user_to_merch'] = df.apply(
        lambda row: haversine(row['lat'], row['long'], row['merch_lat'], row['merch_long']),
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

    df.drop(columns=[
        'trans_date_trans_time', 'dob', 'first', 'last', 'street',
        'trans_num', 'cc_num', 'amt', 'city_pop', 'distance_user_to_merch'
    ], inplace=True)

    return df


def detect_tree_method() -> str:
    try:
        import torch
        if torch.cuda.is_available():
            print("GPU detectada — usando device='cuda'")
            return "hist"
    except ImportError:
        pass
    print("Sin GPU — usando tree_method='hist' (CPU)")
    return "hist"


def main():
    print("Cargando datasets...")
    df_train = pd.read_csv(TRAIN_PATH)
    df_test  = pd.read_csv(TEST_PATH)

    print("Aplicando feature engineering...")
    df_train = preprocess(df_train)
    df_test  = preprocess(df_test)

    X_train = df_train[CAT_COLS + NUMERIC_COLS]
    y_train = df_train[TARGET]
    X_test  = df_test[CAT_COLS + NUMERIC_COLS]
    y_test  = df_test[TARGET]

    tree_method = detect_tree_method()

    preprocessor = ColumnTransformer(transformers=[
        ('num', StandardScaler(),                       NUMERIC_COLS),
        ('cat', OneHotEncoder(handle_unknown='ignore'), CAT_COLS),
    ])

    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('clf', XGBClassifier(
            eval_metric='logloss',
            tree_method=tree_method,
            random_state=42,
            verbosity=0
        ))
    ])

    param_grid = {
        'clf__n_estimators':     [200],
        'clf__max_depth':        [12],
        'clf__learning_rate':    [0.2],
        'clf__subsample':        [0.8],
        'clf__colsample_bytree': [1.0],
        'clf__scale_pos_weight': [10],
        'clf__reg_alpha':        [1],
        'clf__reg_lambda':       [10],
    }

    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)

    print("Entrenando con GridSearchCV (3 folds) — ~10-20 min en CPU...")
    grid_search = GridSearchCV(
        pipeline, param_grid,
        cv=cv, scoring='recall',
        n_jobs=2, verbose=1
    )
    grid_search.fit(X_train, y_train)

    print(f"\nMejores parametros: {grid_search.best_params_}")
    print(f"Mejor recall CV:    {grid_search.best_score_:.4f}")

    best_model = grid_search.best_estimator_

    y_proba = best_model.predict_proba(X_test)[:, 1]
    y_pred  = (y_proba >= THRESHOLD).astype(int)

    print(f"\n── Test set results (threshold={THRESHOLD}) ──")
    print(classification_report(y_test, y_pred,
                                target_names=['Legitimate', 'Fraud'], digits=2))
    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_pred))

    joblib.dump(best_model, MODEL_OUT)
    print(f"\nModelo guardado en {MODEL_OUT}")


if __name__ == "__main__":
    main()
