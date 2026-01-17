import time
import random
import os
import shutil
from src.storage_engine.engine import StorageEngine


# Configuration
NUM_RECORDS = 100_000
KEY_SIZE = 16 # bytes
VAL_SIZE = 100 # bytes
DB_PATH = "benchmark_data"

def run_benchmark():
    print(f"--- Iron-Python Storage Engine Benchmark ---")
    print(f"Records: {NUM_RECORDS}")
    print(f"Payload: {KEY_SIZE} byte keys, {VAL_SIZE} byte values")

    # Clean up previous runs
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)

    engine = StorageEngine(DB_PATH, memtable_max_size=1000)

    # 1. WRITE BENCHMARK
    print("\n[Phase 1] Writing Data...")
    start_time = time.time()

    for i in range(NUM_RECORDS):
        key = f"user:{i:010d}" # padding ensures consistent size
        val = "x" * VAL_SIZE
        engine.put(key, val)

        if i % 10000 == 0:
            print(f"  ... inserted {i} records")
    
    end_time = time.time()
    duration = end_time - start_time
    writes_per_sec = NUM_RECORDS / duration
    print(f"-> Write Result: {writes_per_sec:.2f} ops/sec")

    # 2. READ BENCHMARK (Random Access)
    print("\n[Phase 2] Reading Data (Random Access)...")
    # We'll read 1000 random keys to sample latency
    sample_size = 1000
    random_indices = [random.randint(0, NUM_RECORDS-1) for _ in range(sample_size)]

    start_time = time.time()
    found_count = 0

    for i in random_indices:
        key = f"user:{i:010d}"
        if engine.get(key):
            found_count += 1
    
    end_time = time.time()
    duration = end_time - start_time
    latency_ms = (duration / sample_size) * 1000

    print(f"-> Read Result: {latency_ms:.4f} ms/op (avg)")
    print(f"-> Hit Rate: {found_count}/{sample_size}")

    engine.close()

if __name__ == "__main__":
    run_benchmark()
