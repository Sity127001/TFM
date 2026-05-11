
import time
import subprocess

total_start = time.time()


def run_step(command, name):

    start = time.time()

    print(f"\n=== {name} ===\n")

    result = subprocess.run(command, shell=True)

    end = time.time()

    elapsed_min = (end - start) / 60

    print(f"\n{name} time: {elapsed_min:.2f} min")

    return elapsed_min


lstm_baseline_time = run_step(
    "python /kaggle/working/src/modeling/run_lstm_baseline.py",
    "LSTM BASELINE"
)

lstm_store_time = run_step(
    "python /kaggle/working/src/modeling/run_lstm_store_baseline.py",
    "LSTM STORE BASELINE"
)

total_end = time.time()

total_time = (total_end - total_start) / 60

print("\n=== ALL LSTM PIPELINES COMPLETED ===")
print(f"Total execution time: {total_time:.2f} min")
