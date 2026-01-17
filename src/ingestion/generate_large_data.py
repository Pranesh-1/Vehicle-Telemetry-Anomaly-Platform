import pandas as pd
import os
import sys
from datetime import datetime

# Fix path to import generator
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from ingestion.generator import TelemetryGenerator

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def generate_million_rows():
    print("🚀 Starting Large Scale Data Generation (Target: 1,000,000 rows)...")
    
    # Simulate a larger fleet
    vehicle_ids = [f'V{i:03d}' for i in range(1, 101)] # 100 Vehicles
    gen = TelemetryGenerator(vehicle_ids)
    
    chunk_size = 50000
    total_rows = 1000000
    start_time = datetime.now()
    
    for i in range(0, total_rows, chunk_size):
        print(f"   Generating chunk {i // chunk_size + 1} / {total_rows // chunk_size}...")
        df = gen.generate_batch(start_time, chunk_size)
        
        # Save each chunk as a separate parquet file (Simulates partitioning)
        file_path = os.path.join(DATA_DIR, f"telemetry_large_part_{i // chunk_size}.parquet")
        df.to_parquet(file_path, index=False)
        
    print(f"✅ Generated {total_rows} rows in {len(os.listdir(DATA_DIR))} files.")
    print(f"📂 Data stored in: {DATA_DIR}")

if __name__ == "__main__":
    generate_million_rows()
