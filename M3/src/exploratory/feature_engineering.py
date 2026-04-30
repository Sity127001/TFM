import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import LabelEncoder

# ============================================================
# 1. MERGE DE TABLAS (sales + calendar + prices)
# ============================================================

def merge_m5_tables(sales, calendar, prices):
    """
    Realiza el merge de las tres tablas principales del M5:
    - sales_train_validation
    - calendar
    - sell_prices

    Retorna un DataFrame largo (long format) listo para feature engineering.
    """

    # Convertir sales a formato largo
    sales_long = sales.melt(
        id_vars=sales.columns[:6],
        var_name="d",
        value_name="sales"
    )

    # Merge con calendar
    df = sales_long.merge(calendar, on="d", how="left")

    # Merge con precios
    df = df.merge(
        prices,
        on=["store_id", "item_id", "wm_yr_wk"],
        how="left"
    )

    # Convertir fecha
    df["date"] = pd.to_datetime(df["date"])

    # Ordenar por serie temporal
    df = df.sort_values(["item_id", "store_id", "date"], kind="mergesort")

    return df


# ============================================================
# 2. GENERACIÓN DE LAGS
# ============================================================

def create_lags(df, lags=[1, 7, 28]):
    """
    Genera variables lag para capturar dependencias temporales.
    """
    for lag in lags:
        df[f"lag_{lag}"] = (
            df.groupby(["item_id", "store_id"])["sales"]
              .shift(lag)
        )
    return df


# ============================================================
# 3. GENERACIÓN DE ROLLING WINDOWS
# ============================================================

def create_rolling_features(df, windows=[7, 28, 56]):
    """
    Genera medias móviles para capturar tendencias y estacionalidad.
    Se aplica shift(1) para evitar fuga de información.
    """
    for w in windows:
        df[f"rolling_mean_{w}"] = (
            df.groupby(["item_id", "store_id"])["sales"]
              .shift(1)
              .rolling(w)
              .mean()
        )
        df[f"rolling_std_{w}"] = (
            df.groupby(["item_id", "store_id"])["sales"]
              .shift(1)
              .rolling(w)
              .std()
        )
    return df


# ============================================================
# 4. CODIFICACIÓN CATEGÓRICA
# ============================================================

def encode_categoricals(df, cols=["item_id", "dept_id", "cat_id", "store_id", "state_id"]):
    """
    Aplica Label Encoding a las variables categóricas del M5.
    """
    for col in cols:
        df[col] = LabelEncoder().fit_transform(df[col])
    return df


# ============================================================
# 5. PIPELINE COMPLETO (DATASET ENTERO)
# ============================================================

def generate_features(sales, calendar, prices):
    """
    Pipeline completo para el dataset entero:
    - merge de tablas
    - generación de lags
    - rolling windows
    - codificación categórica
    """

    df = merge_m5_tables(sales, calendar, prices)
    df = create_lags(df)
    df = create_rolling_features(df)
    df = encode_categoricals(df)

    return df


# ============================================================
# 6. PIPELINE PARA SAMPLE (YA MERGEADO)
# ============================================================

def generate_features_from_df(df):
    """
    Genera features a partir de un dataframe YA mergeado
    (como m5_clean_sample.parquet).
    No hace merges, solo aplica lags, rolling y encoding.
    """

    df = df.copy()

    # Lags
    df = create_lags(df)

    # Rolling windows
    df = create_rolling_features(df)

    # Encoding
    df = encode_categoricals(df)

    return df


# ============================================================
# 7. GUARDAR FEATURES
# ============================================================

def save_features(df, path="data/features/m5_features.parquet"):
    """
    Guarda el dataset procesado en formato parquet.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Features guardadas en: {path}")
