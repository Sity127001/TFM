import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import shap
import time

start_total = time.time()
# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_PATH = BASE_DIR / "data" / "features" / "m5_features"
MODEL_PATH = BASE_DIR / "models"
OUTPUT_PATH = BASE_DIR / "post_analysis"
PRED_PATH = OUTPUT_PATH / "predictions"

PRED_PATH.mkdir(parents=True, exist_ok=True)

# =========================
# LOAD MODEL
# =========================

model = joblib.load(MODEL_PATH / "lgbm_global.pkl")
features = joblib.load(MODEL_PATH / "features_global.pkl")

# =========================
# STORES
# =========================

stores = pd.read_parquet(
    DATA_PATH,
    columns=["store_id"]
)["store_id"].unique().tolist()

# =========================
# FUNCTIONS
# =========================

def load_store(store):
    df = pd.read_parquet(
        DATA_PATH,
        filters=[("store_id", "==", store)]
    )

    df["date"] = pd.to_datetime(df["date"])
    return df


def prepare_X(df):
    X = df[features].copy()

    categorical_cols = [
        "state_id",
        "store_id",
        "item_id",
        "dept_id",
        "cat_id",
        "weekday",
        "event_name_1",
        "event_type_1",
        "event_name_2",
        "event_type_2"
    ]

    for col in categorical_cols:
        if col in X.columns:
            X[col] = X[col].astype("category")

    return X


def compute_shap(model, X):
    X_sample = X.sample(n=50000, random_state=42)

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_sample)

    shap_df = pd.DataFrame({
        "feature": X_sample.columns,
        "mean_abs_shap": np.abs(shap_values).mean(axis=0)
    }).sort_values("mean_abs_shap", ascending=False)

    return shap_df


# =========================
# MAIN
# =========================

all_shap = []

start_total = time.time()

for i, store in enumerate(stores):

    print(f"\nProcessing {store}")

    df = load_store(store)

    X = prepare_X(df)

    # =========================
    # PREDICTIONS
    # =========================

    df["prediction"] = model.predict(X)

    # TODAS LAS COLUMNAS
    pred = df.copy()

    output_file = PRED_PATH / f"pred_{store}.parquet"
    pred.to_parquet(output_file, index=False)

    # =========================
    # SHAP 
    # =========================

    if i == 0:
        shap_df = compute_shap(model, X)
        shap_df["store_id"] = "GLOBAL"
        all_shap.append(shap_df)


# =========================
# SAVE
# =========================


shap_final = pd.concat(all_shap, ignore_index=True)



shap_final.to_parquet(
    OUTPUT_PATH / "shap_values.parquet",
    index=False
)

end_total = time.time()

print(f"\nTotal time: {(end_total - start_total)/60:.2f} min")

print("Post-analysis global completed")