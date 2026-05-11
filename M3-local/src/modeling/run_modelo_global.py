import pandas as pd
import lightgbm as lgb
import joblib
from pathlib import Path
import time

start_total = time.time()

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_PATH = BASE_DIR / "data" / "features" / "m5_features"
MODEL_PATH = BASE_DIR / "models"

MODEL_PATH.mkdir(parents=True, exist_ok=True)

# obtener stores dinámicamente
stores = pd.read_parquet(
    DATA_PATH,
    columns=["store_id"]
)["store_id"].unique().tolist()

model = None

categorical_cols = [
    "store_id","item_id","dept_id","cat_id","state_id",
    "weekday","event_name_1","event_type_1",
    "event_name_2","event_type_2"
]

start_total = time.time()

for i, store in enumerate(stores):

    print(f"\nTraining on {store}")

    df = pd.read_parquet(
        DATA_PATH,
        filters=[("store_id", "==", store)]
    )

    df["date"] = pd.to_datetime(df["date"])

    train = df[df["date"] < "2015-01-01"]
    val = df[(df["date"] >= "2015-01-01") & (df["date"] < "2016-01-01")]

    target = "sales"

    features = [
        col for col in df.columns
        if col not in ["sales", "date", "id", "d"]
    ]

    X_train = train[features].copy()
    y_train = train[target].copy()

    X_val = val[features].copy()
    y_val = val[target].copy()

    for col in categorical_cols:
        X_train[col] = X_train[col].astype("category")
        X_val[col] = X_val[col].astype("category")

    if model is None:
        model = lgb.LGBMRegressor(
            n_estimators=100,
            learning_rate=0.05,
            num_leaves=31,
            n_jobs=-1,
            random_state=42
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            categorical_feature=categorical_cols
        )

    else:
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            categorical_feature=categorical_cols,
            init_model=model  # 🔥 clave
        )



joblib.dump(model, MODEL_PATH / "lgbm_global.pkl")
joblib.dump(features, MODEL_PATH / "features_global.pkl")

end_total = time.time()

print(f"\nTotal training time: {(end_total - start_total)/60:.2f} min")
print("Global model saved")