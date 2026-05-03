from train_single_store import run_modeling_for_store
from pyspark.sql import SparkSession
import pandas as pd
import time

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("../../data/features/m5_features")

stores = [row["store_id"] for row in df.select("store_id").distinct().collect()]

import time

all_results = []

total_start = time.time()

for store in stores:
    print(f"\nProcessing store: {store}")

    start = time.time()

    results = run_modeling_for_store(store)

    end = time.time()
    elapsed = end - start

    print(f"Time for {store}: {elapsed:.2f} seconds")

    results["time_sec"] = elapsed

    all_results.append(results)

final_results = pd.concat(all_results)

total_end = time.time()
print(f"\nTotal time: {(total_end - total_start)/60:.2f} minutes")

print(final_results)

final_results.to_parquet("../../models/results_all_stores.parquet", index=False)