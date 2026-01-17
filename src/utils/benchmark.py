import pandas as pd
import duckdb
import time
import os
import glob
import numpy as np

def run_benchmark():
    print("🚀 Starting Performance Benchmark: Pandas vs DuckDB")
    print("-" * 50)
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base_dir, 'data')
    parquet_files = glob.glob(os.path.join(data_dir, "telemetry_valid_*.parquet"))
    
    if not parquet_files:
        print("No data to benchmark.")
        return

    # 1. Pandas Benchmark
    start_time = time.time()
    df_pandas = pd.read_parquet(parquet_files) # Load all
    
    # Complex Aggregation: Avg Speed per Vehicle
    res_pandas = df_pandas.groupby('vehicle_id')['speed_kmph'].mean()
    
    pandas_duration = time.time() - start_time
    print(f"🐼 Pandas: Loaded & Aggregated {len(df_pandas)} rows in {pandas_duration:.4f} seconds.")
    
    # 2. DuckDB Benchmark
    con = duckdb.connect()
    start_time = time.time()
    
    # Zero-copy query directly on parquet files
    query = f"""
    SELECT vehicle_id, AVG(speed_kmph) 
    FROM read_parquet('{os.path.join(data_dir, "telemetry_valid_*.parquet")}')
    GROUP BY vehicle_id
    """
    res_duck = con.execute(query).df()
    
    duck_duration = time.time() - start_time
    print(f"🦆 DuckDB: Aggregated (Zero-Copy) in {duck_duration:.4f} seconds.")
    
    # Conclusion
    speedup = pandas_duration / duck_duration
    print("-" * 50)
    print(f"⚡ Speedup Factor: {speedup:.2f}x")
    if speedup > 1.5:
        print("✅ DuckDB serves as a highly scalable engine for this workload.")
    else:
        print("⚠️ Dataset might be too small to see massive DuckDB gains, but architecture scales.")

if __name__ == "__main__":
    run_benchmark()
