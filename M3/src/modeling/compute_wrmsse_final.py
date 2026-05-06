import pandas as pd
import numpy as np
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

start_time = time.time()

print("Loading data...")

pred = pd.read_parquet(
    "../../post_analysis/mint_predictions.parquet",
    columns=["series_id", "date", "prediction_mint"]
)

true = pd.read_parquet(
    "../../post_analysis/hierarchy_predictions.parquet",
    columns=["series_id", "date", "sales"]
)

scale = pd.read_parquet("../../post_analysis/scale.parquet")
weights = pd.read_parquet("../../post_analysis/weights.parquet")

scale_map = scale.set_index("series_id")["scale"].to_dict()

# =========================
# MERGE y_true + y_pred
# =========================

df = pred.merge(
    true[["series_id","date","sales"]],
    on=["series_id","date"],
    how="inner"
)

# =========================
# RMSSE
# =========================

def rmsse(y_true, y_pred, scale):
    if scale == 0 or np.isnan(scale):
        return np.nan
    return np.sqrt(np.mean((y_true - y_pred) ** 2) / scale)

results = []

for sid, g in df.groupby("series_id"):
    
    y_true = g["sales"].values
    y_pred = g["prediction_mint"].values
    
    sc = scale_map.get(sid)
    
    if sc is None:
        continue
    
    val = rmsse(y_true, y_pred, sc)
    
    results.append((sid, val))

rmsse_df = pd.DataFrame(results, columns=["series_id","rmsse"])

# =========================
# WRMSSE
# =========================

final = rmsse_df.merge(weights, on="series_id", how="inner")

wrmsse = np.sum(final["rmsse"] * final["weight"])

# Desglose individual WRMSSE
final["wrmsse_component"] = final["rmsse"] * final["weight"]
mean_rmsse = final["rmsse"].mean()

result_df = pd.DataFrame({
    "metric": ["WRMSSE"],
    "value": [wrmsse]
})

result_df.to_parquet(
    BASE_DIR / "post_analysis" / "wrmsse_result.parquet",
    index=False
)

final.to_parquet(
    BASE_DIR / "post_analysis" / "wrmsse_details.parquet",
    index=False
)

end_time = time.time()

print(f"WRMSSE calculated in {(end_time - start_time)/60:.2f} minutes")
print("\nFINAL WRMSSE:", wrmsse)