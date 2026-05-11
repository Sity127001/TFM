import pandas as pd
import numpy as np
from pathlib import Path
import time

start_time = time.time()

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT = BASE_DIR / "post_analysis" / "hierarchy_predictions.parquet"
OUTPUT = BASE_DIR / "post_analysis"

print("Loading hierarchy...")

# cargar solo lo necesario
df = pd.read_parquet(INPUT, columns=["date", "series_id", "prediction", "level"])

df["date"] = pd.to_datetime(df["date"])
df["series_id"] = df["series_id"].astype("category")

# =========================
# PIVOT
# =========================

pivot = df.pivot_table(
    index="date",
    columns="series_id",
    values="prediction",
    observed=True
).fillna(0)

Y_hat = pivot.values

# =========================
# COVARIANCE
# =========================

print("Computing diagonal covariance...")

# varianza por serie
series_var = np.var(Y_hat, axis=0)

# evitar división por 0
series_var[series_var == 0] = 1e-8

# pesos diagonales
weights = (1 / series_var)

# normalizar
weights = weights / weights.sum()

# =========================
# MinT
# =========================

print("Applying MinT...")

Y_rec = Y_hat * weights

# =========================
# BACK
# =========================

out = pd.DataFrame(
    Y_rec,
    index=pivot.index,
    columns=pivot.columns
).reset_index().melt(
    id_vars="date",
    var_name="series_id",
    value_name="prediction_mint"
)

out.to_parquet(OUTPUT / "mint_predictions.parquet", index=False)

end_time = time.time()

print(f"MinT completed in {(end_time - start_time)/60:.2f} min")