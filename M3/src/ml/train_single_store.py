import pandas as pd
import numpy as np
from pathlib import Path

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor

import lightgbm as lgb
import xgboost as xgb
import joblib


def load_store_data(store_id):
    """
    Carga del subconjunto de datos correspondiente a una única tienda.

    Se utiliza filtrado en la lectura del parquet para evitar cargar el
    dataset completo en memoria, lo que mejora la eficiencia del pipeline.
    """
    df_store = pd.read_parquet(
        "../../data/features/m5_features",
        filters=[("store_id", "==", store_id)]
    )

    df_store["date"] = pd.to_datetime(df_store["date"])

    return df_store


def temporal_split(df_store):
    """
    División temporal del dataset en conjuntos de entrenamiento,
    validación y prueba.

    Se evita el uso de particiones aleatorias para prevenir data leakage.
    """
    train = df_store[df_store["date"] < "2015-01-01"]

    validation = df_store[
        (df_store["date"] >= "2015-01-01") &
        (df_store["date"] < "2016-01-01")
    ]

    test = df_store[df_store["date"] >= "2016-01-01"]

    return train, validation, test


def prepare_features(train, validation, test):
    """
    Separación de variables predictoras (features) y variable objetivo.

    La variable objetivo es 'sales'. Se excluyen variables identificadoras
    y temporales no informativas para el modelo.
    """
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
    """
    Conversión de variables categóricas al tipo 'category'.

    LightGBM permite trabajar directamente con este tipo, mejorando
    eficiencia y evitando codificaciones manuales.
    """
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
    """
    Entrenamiento del modelo LightGBM.

    Se selecciona como modelo principal por su eficiencia en datasets
    de gran tamaño y su capacidad para manejar variables categóricas.
    """
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


def encode_for_tree_models(X_train, X_val, X_test):
    """
    Codificación numérica de variables categóricas para modelos que
    no soportan directamente categorías (Random Forest y XGBoost).
    """
    categorical_cols = X_train.select_dtypes(include="category").columns

    for col in categorical_cols:
        le = LabelEncoder()

        X_train[col] = le.fit_transform(X_train[col].astype(str))
        X_val[col] = le.transform(X_val[col].astype(str))
        X_test[col] = le.transform(X_test[col].astype(str))

    return X_train, X_val, X_test


def train_random_forest(X_train, y_train):
    """
    Entrenamiento del modelo Random Forest como baseline.
    """
    model = RandomForestRegressor(
        n_estimators=50,
        max_depth=10,
        n_jobs=-1,
        random_state=42
    )

    model.fit(X_train, y_train)

    return model


def train_xgboost(X_train, y_train):
    """
    Entrenamiento del modelo XGBoost.

    Permite comparar técnicas de boosting frente a LightGBM.
    """
    model = xgb.XGBRegressor(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=6,
        n_jobs=-1,
        random_state=42
    )

    model.fit(X_train, y_train)

    return model


def evaluate(y_true, y_pred):
    """
    Cálculo de métricas de evaluación.

    RMSE penaliza errores grandes.
    MAE proporciona una medida interpretable del error medio.
    """
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)

    return rmse, mae


def run_modeling_for_store(store_id):
    """
    Ejecución completa del pipeline de modelado para una tienda.

    Incluye:
    - carga de datos
    - división temporal
    - preparación de variables
    - entrenamiento de modelos
    - evaluación
    - almacenamiento de resultados
    """

    df_store = load_store_data(store_id)

    train, val, test = temporal_split(df_store)

    X_train, y_train, X_val, y_val, X_test, y_test, features = prepare_features(
        train, val, test
    )

    X_train, X_val, X_test, categorical_cols = encode_categoricals(
        X_train, X_val, X_test
    )

    # Modelo principal
    lgb_model = train_lightgbm(
        X_train, y_train, X_val, y_val, categorical_cols
    )

    y_pred_lgb = lgb_model.predict(X_test)
    rmse_lgb, mae_lgb = evaluate(y_test, y_pred_lgb)

    # Preparación para modelos alternativos
    X_train_rf, X_val_rf, X_test_rf = encode_for_tree_models(
        X_train.copy(), X_val.copy(), X_test.copy()
    )

    # Random Forest
    rf_model = train_random_forest(X_train_rf, y_train)
    y_pred_rf = rf_model.predict(X_test_rf)
    rmse_rf, mae_rf = evaluate(y_test, y_pred_rf)

    # XGBoost
    xgb_model = train_xgboost(X_train_rf, y_train)
    y_pred_xgb = xgb_model.predict(X_test_rf)
    rmse_xgb, mae_xgb = evaluate(y_test, y_pred_xgb)

    # Consolidación de resultados
    results = pd.DataFrame({
        "Model": ["LightGBM", "Random Forest", "XGBoost"],
        "RMSE": [rmse_lgb, rmse_rf, rmse_xgb],
        "MAE": [mae_lgb, mae_rf, mae_xgb],
        "store_id": store_id
    })

    # Persistencia de modelos
    model_path = Path("../../models")
    model_path.mkdir(parents=True, exist_ok=True)

    joblib.dump(lgb_model, model_path / f"lgbm_store_{store_id}.pkl")
    joblib.dump(rf_model, model_path / f"rf_store_{store_id}.pkl")
    joblib.dump(xgb_model, model_path / f"xgb_store_{store_id}.pkl")
    joblib.dump(features, model_path / f"features_store_{store_id}.pkl")

    return results