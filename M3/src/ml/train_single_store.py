import pandas as pd
import numpy as np
from pathlib import Path

from sklearn.metrics import mean_squared_error, mean_absolute_error

#  No necesarios en pipeline final (se mantienen comentados para trazabilidad)
# from sklearn.preprocessing import LabelEncoder
# from sklearn.ensemble import RandomForestRegressor

import lightgbm as lgb

# No necesario en pipeline final
# import xgboost as xgb

import joblib


def load_store_data(store_id):
    df_store = pd.read_parquet(
        "../../data/features/m5_features",
        filters=[("store_id", "==", store_id)]
    )

    df_store["date"] = pd.to_datetime(df_store["date"])

    return df_store


def temporal_split(df_store):
    train = df_store[df_store["date"] < "2015-01-01"]

    validation = df_store[
        (df_store["date"] >= "2015-01-01") &
        (df_store["date"] < "2016-01-01")
    ]

    test = df_store[df_store["date"] >= "2016-01-01"]

    return train, validation, test


def prepare_features(train, validation, test):
    target = "sales"

    features = [
        col for col in train.columns
        if col not in ["sales", "date", "id", "d"]
    ]

    X_train = train[features].copy()
    y_train = train[target].copy()

    X_val = validation[features].copy()
    y_val = validation[target].copy()

    X_test = test[features].copy()
    y_test = test[target].copy()

    return X_train, y_train, X_val, y_val, X_test, y_test, features


def encode_categoricals(X_train, X_val, X_test):
    categorical_cols = [
        "store_id", "item_id", "dept_id", "cat_id", "state_id",
        "weekday", "event_name_1", "event_type_1",
        "event_name_2", "event_type_2"
    ]

    for col in categorical_cols:
        X_train[col] = X_train[col].astype("category")
        X_val[col] = X_val[col].astype("category")
        X_test[col] = X_test[col].astype("category")

    return X_train, X_val, X_test, categorical_cols


def train_lightgbm(X_train, y_train, X_val, y_val, categorical_cols):
    model = lgb.LGBMRegressor(
        n_estimators=100,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        categorical_feature=categorical_cols
    )

    return model


# BLOQUES DE MODELOS ALTERNATIVOS (se mantienen comentados para documentación)

# def encode_for_tree_models(X_train, X_val, X_test):
#     categorical_cols = X_train.select_dtypes(include="category").columns
#
#     for col in categorical_cols:
#         le = LabelEncoder()
#         X_train[col] = le.fit_transform(X_train[col].astype(str))
#         X_val[col] = le.transform(X_val[col].astype(str))
#         X_test[col] = le.transform(X_test[col].astype(str))
#
#     return X_train, X_val, X_test


# def train_random_forest(X_train, y_train):
#     model = RandomForestRegressor(
#         n_estimators=50,
#         max_depth=10,
#         n_jobs=-1,
#         random_state=42
#     )
#     model.fit(X_train, y_train)
#     return model


# def train_xgboost(X_train, y_train):
#     model = xgb.XGBRegressor(
#         n_estimators=100,
#         learning_rate=0.05,
#         max_depth=6,
#         n_jobs=-1,
#         random_state=42
#     )
#     model.fit(X_train, y_train)
#     return model


def evaluate(y_true, y_pred):
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)

    return rmse, mae


def run_modeling_for_store(store_id):

    df_store = load_store_data(store_id)

    train, val, test = temporal_split(df_store)

    X_train, y_train, X_val, y_val, X_test, y_test, features = prepare_features(
        train, val, test
    )

    X_train, X_val, X_test, categorical_cols = encode_categoricals(
        X_train, X_val, X_test
    )

    #  Modelo final seleccionado
    lgb_model = train_lightgbm(
        X_train, y_train, X_val, y_val, categorical_cols
    )

    y_pred_lgb = lgb_model.predict(X_test)
    rmse_lgb, mae_lgb = evaluate(y_test, y_pred_lgb)

    #  BLOQUES ELIMINADOS (RF y XGB) – ya evaluados en fase experimental

    # X_train_rf, X_val_rf, X_test_rf = encode_for_tree_models(
    #     X_train.copy(), X_val.copy(), X_test.copy()
    # )

    # rf_model = train_random_forest(X_train_rf, y_train)
    # y_pred_rf = rf_model.predict(X_test_rf)
    # rmse_rf, mae_rf = evaluate(y_test, y_pred_rf)

    # xgb_model = train_xgboost(X_train_rf, y_train)
    # y_pred_xgb = xgb_model.predict(X_test_rf)
    # rmse_xgb, mae_xgb = evaluate(y_test, y_pred_xgb)

    #  Resultados finales (solo modelo seleccionado)
    results = pd.DataFrame({
        "Model": ["LightGBM"],
        "RMSE": [rmse_lgb],
        "MAE": [mae_lgb],
        "store_id": store_id
    })

    model_path = Path("../../models")
    model_path.mkdir(parents=True, exist_ok=True)

    # Guardado solo del modelo final
    joblib.dump(lgb_model, model_path / f"lgbm_store_{store_id}.pkl")

    # ❌ Eliminado guardado de RF y XGB
    # joblib.dump(rf_model, ...)
    # joblib.dump(xgb_model, ...)

    joblib.dump(features, model_path / f"features_store_{store_id}.pkl")

    return results