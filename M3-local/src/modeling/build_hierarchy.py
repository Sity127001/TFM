import glob
import pandas as pd
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT = BASE_DIR / "post_analysis"
OUTPUT.mkdir(parents=True, exist_ok=True)

start_time = time.time()

PRED_PATH = BASE_DIR / "post_analysis" / "predictions"

files = glob.glob(str(PRED_PATH / "pred_*.parquet"))

print(f"Found {len(files)} files")

if len(files) == 0:
    raise ValueError("No prediction files found in predictions/")

# =========================
# LOAD
# =========================
print("Loading predictions...")

# =========================
# FUNCTION
# =========================

def build_level(data, group_cols, level_name):
    g = (
        data
        .groupby(group_cols + ["date"], observed=True)[["prediction", "sales"]]
        .sum()
        .reset_index()
    )

    g["level"] = level_name

    g[group_cols] = g[group_cols].astype(str)

    if len(group_cols) == 1:
        g["series_id"] = g[group_cols[0]]
    else:
        g["series_id"] = g[group_cols[0]]
        for col in group_cols[1:]:
            g["series_id"] += "_" + g[col]

    return g[["series_id", "date", "prediction", "sales", "level"]]

# =========================
# LEVELS
# =========================

levels = []

for f in files:

    df = pd.read_parquet(f)

    print(df.head())

    # convertir tipos (evita errores categorical)
    for col in ["store_id", "item_id", "dept_id", "cat_id"]:
        df[col] = df[col].astype(str)

    df["date"] = pd.to_datetime(df["date"])

    levels.append(build_level(df, ["store_id", "item_id"], "item"))
    levels.append(build_level(df, ["store_id", "dept_id"], "dept"))
    levels.append(build_level(df, ["store_id", "cat_id"], "cat"))
    levels.append(build_level(df, ["store_id"], "store"))
    levels.append(build_level(df, ["state_id"], "state"))
    levels.append(build_level(df, ["state_id", "cat_id"], "state_cat"))
    levels.append(build_level(df, ["state_id", "dept_id"], "state_dept"))

    # TOTAL por store
    total = (
        df.groupby(["date"], observed=True)[["prediction", "sales"]]
        .sum()
        .reset_index()
    )

    total["level"] = "total"
    total["series_id"] = "total"

    levels.append(total[["series_id", "date", "prediction", "sales", "level"]])

# =========================
# CONCAT
# =========================

hierarchy = pd.concat(levels, ignore_index=True)

# =========================
# SAVE
# =========================

OUTPUT_FILE = OUTPUT / "hierarchy_predictions.parquet"

hierarchy.to_parquet(OUTPUT_FILE, index=False)

end_time = time.time()
print(f"Time: {(end_time - start_time)/60:.2f} minutes")
print("Hierarchy built successfully")
print(f"Rows: {hierarchy.shape[0]}")