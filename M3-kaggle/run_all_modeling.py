import subprocess
import sys
import time

PYTHONPATH = "PYTHONPATH=/kaggle/working"

timings = []

def run_step(cmd, name):
    print(f"\n=== {name} ===")

    start = time.time()
    result = subprocess.run(f"{PYTHONPATH} {cmd}", shell=True)
    end = time.time()

    minutes = (end - start) / 60
    timings.append((name, minutes))

    print(f"\n{name} time: {minutes:.2f} min")

    if result.returncode != 0:
        print(f"\nERROR en {name}")
        sys.exit(1)

run_step("python /kaggle/working/src/modeling/run_modelo_global.py", "MODEL TRAINING")
run_step("python /kaggle/working/src/modeling/run_post_analysis.py", "POST ANALYSIS")
run_step("python /kaggle/working/src/modeling/build_hierarchy.py", "BUILD HIERARCHY")
run_step("python /kaggle/working/src/modeling/build_m5_evaluation.py", "BUILD M5 EVALUATION")
run_step("python /kaggle/working/src/modeling/run_mint_top_revenue.py", "MINT TOP REVENUE")
run_step("python /kaggle/working/src/modeling/compute_wrmsse_final.py", "COMPUTE WRMSSE")


print("\n=== MODELING PIPELINE COMPLETED ===")
print("\n=== TIMING SUMMARY ===")

total = 0
for name, minutes in timings:
    total += minutes
    print(f"{name}: {minutes:.2f} min")

print(f"TOTAL MODELING TIME: {total:.2f} min")
