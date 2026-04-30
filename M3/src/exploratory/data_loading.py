# src/data_loading.py

import pandas as pd
import numpy as np
import os

# ============================================================
# 1. OPTIMIZACIÓN DE MEMORIA (DOWNCASTING)
# ============================================================


def downcast(df):
    """
    Reduce el consumo de memoria del DataFrame
    aplicando conversión automática de tipos
    numéricos y categóricos.
    """
    for col in df.columns:
        col_type = df[col].dtype

        # ENTEROS → usar el más pequeño posible
        if pd.api.types.is_integer_dtype(col_type):
            df[col] = pd.to_numeric(df[col], downcast="integer")

        # FLOATS → float32 o incluso float16 si no pierdes precisión
        elif pd.api.types.is_float_dtype(col_type):
            df[col] = pd.to_numeric(df[col], downcast="float")

        # STRINGS → SOLO si baja cardinalidad
        elif pd.api.types.is_object_dtype(col_type):

            num_unique = df[col].nunique()
            num_total = len(df[col])

            # Solo convertir a category si merece la pena
            if num_unique / num_total < 0.5:
                df[col] = df[col].astype("category")

    return df


# ============================================================
# 2. CARGA DE DATOS DEL M5 (CON DOWNCASTING)
# ============================================================

def load_m5_data(path="data/raw/"):
    """
    Carga los ficheros del M5 y aplica downcasting.
    """

    sales_path = os.path.join(path, "sales_train_validation.csv")
    calendar_path = os.path.join(path, "calendar.csv")
    prices_path = os.path.join(path, "sell_prices.csv")

    for p in [sales_path, calendar_path, prices_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Archivo no encontrado: {p}")

    sales = pd.read_csv(sales_path)
    calendar = pd.read_csv(calendar_path)
    prices = pd.read_csv(prices_path)

    # Downcasting
    sales = downcast(sales)
    calendar = downcast(calendar)
    prices = downcast(prices)

    # Convertir fecha
    calendar["date"] = pd.to_datetime(calendar["date"])

    return sales, calendar, prices