import pandas as pd
import numpy as np
from pathlib import Path
import time

start_total = time.time()

DATA_PATH = "../../data/raw"
OUTPUT_PATH = Path("../../post_analysis")
OUTPUT_PATH.mkdir(exist_ok=True)

print("Loading M5 raw data...")

sales = pd.read_csv(DATA_PATH + "/sales_train_validation.csv")
calendar = pd.read_csv(DATA_PATH + "/calendar.csv")
prices = pd.read_csv(DATA_PATH + "/sell_prices.csv")

# =========================
# LONG FORMAT
# =========================

d_cols = [c for c in sales.columns if c.startswith("d_")]

df = sales.melt(
    id_vars=["item_id","dept_id","cat_id","store_id","state_id"],
    value_vars=d_cols,
    var_name="d",
    value_name="sales"
)

df = df.merge(calendar[["d","date","wm_yr_wk"]], on="d")
df = df.merge(prices, on=["store_id","item_id","wm_yr_wk"], how="left")

df["revenue"] = df["sales"] * df["sell_price"]
df["date"] = pd.to_datetime(df["date"])

# =========================
# TRAIN SPLIT
# =========================

train = df[df["date"] < "2016-01-01"]

# =========================
# SCALE (RMSSE denominator)
# =========================

print("Computing RMSSE scale...")

scale = (
    train.groupby(["item_id","store_id"])["sales"]
    .apply(lambda x: np.mean(np.diff(x.values) ** 2) + 1e-8)
    .reset_index()
)

#  ORDEN CORRECTO 
scale["series_id"] = (
    scale["store_id"].astype(str)
    + "_"
    + scale["item_id"].astype(str)
)

#  NOMBRE CORRECTO PARA WRMSSE
scale = scale.rename(columns={"sales": "scale"})

scale.to_parquet(OUTPUT_PATH / "scale.parquet", index=False)

# =========================
# WEIGHTS (revenue)
# =========================

print("Computing weights...")

weights = (
    train.groupby(["item_id","store_id"])["revenue"]
    .sum()
    .reset_index()
)

weights["series_id"] = (
    weights["store_id"].astype(str)
    + "_"
    + weights["item_id"].astype(str)
)

weights["weight"] = weights["revenue"] / weights["revenue"].sum()

weights.to_parquet(OUTPUT_PATH / "weights.parquet", index=False)

end_total = time.time()

print(f"\nTotal Evaluation data built time: {(end_total - start_total)/60:.2f} min")
print("Evaluation data built")