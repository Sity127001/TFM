import subprocess
import sys
import time

PYTHONPATH = "PYTHONPATH=/kaggle/working"

start = time.time()

print("\n=== SPARK FEATURE ENGINEERING ===")
result = subprocess.run(
    f"{PYTHONPATH} python /kaggle/working/src/spark/run_spark_pipeline.py",
    shell=True
)

end = time.time()

minutes = (end - start) / 60
print(f"\nSPARK FEATURE ENGINEERING time: {minutes:.2f} min")

if result.returncode != 0:
    print("\nERROR en SPARK FEATURE ENGINEERING")
    sys.exit(1)

print("\n=== SPARK PIPELINE COMPLETED ===")
