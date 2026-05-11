import os
import sys

def run_step(cmd, name):
    print(f"\n=== {name} ===")
    result = os.system(cmd)
    if result != 0:
        print(f"\n ERROR en {name}")
        sys.exit(1)

run_step("python src/modeling/run_modelo_global.py", "MODEL TRAINING")
run_step("python src/modeling/run_post_analysis.py", "POST ANALYSIS")
run_step("python src/modeling/build_hierarchy.py", "BUILD HIERARCHY")
run_step("python src/modeling/run_mint_reconciliation_vectorizada.py","MINT RECONCILIATION")

run_step("python src/modeling/build_m5_evaluation.py", "BUILD M5 EVALUATION")

run_step("python src/modeling/compute_wrmsse_final.py", "COMPUTE WRMSSE")


print("\n=== PIPELINE COMPLETED ===")