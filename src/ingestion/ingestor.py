import pandas as pd
import logging
from datetime import datetime
import os
from generator import TelemetryGenerator

# Setup Logging based on script location
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DATA_DIR = os.path.join(BASE_DIR, 'data')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'ingestion.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def validate_schema(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separates valid data from invalid data based on Motorq-aligned rules.
    Returns (valid_df, invalid_df)
    """
    # 1. Check for Missing Timestamps or Vehicle IDs (Critical)
    critical_errors = df[df['timestamp'].isnull() | df['vehicle_id'].isnull()]
    
    # 2. Check for Physics Violations (Speed < 0, Speed > 250, RPM < 0)
    physics_errors = df[
        (df['speed_kmph'] < 0) | 
        (df['speed_kmph'] > 250) | 
        (df['rpm'] < 0)
    ]
    
    # Combine all invalid indices
    invalid_indices = critical_errors.index.union(physics_errors.index)
    
    valid_df = df.drop(invalid_indices)
    invalid_df = df.loc[invalid_indices].copy()
    
    if not invalid_df.empty:
        # Add reason for rejection (simple mapping for demo)
        invalid_df['rejection_reason'] = 'Schema/Physics Violation'
        
    return valid_df, invalid_df

def run_ingestion_pipeline(num_records=1000):
    logging.info(f"Starting ingestion batch of {num_records} records...")
    
    # 1. Generate Data (Simulating Stream)
    gen = TelemetryGenerator(['V001', 'V002', 'V003', 'V004', 'V005'])
    raw_df = gen.generate_batch(datetime.utcnow(), num_records)
    
    # 2. Validation Layer
    valid_df, invalid_df = validate_schema(raw_df)
    
    # 3. Storage (Simulating Bronze Layer)
    # Storing as Parquet for efficiency (better than CSV)
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if not valid_df.empty:
        valid_path = os.path.join(DATA_DIR, f'telemetry_valid_{timestamp_str}.parquet')
        valid_df.to_parquet(valid_path, index=False)
        logging.info(f"Successfully ingested {len(valid_df)} records to {valid_path}")
        print(f"✅ Ingested {len(valid_df)} records.")
        
    if not invalid_df.empty:
        invalid_path = os.path.join(DATA_DIR, f'quarantine_{timestamp_str}.csv')
        invalid_df.to_csv(invalid_path, index=False)
        logging.warning(f"Quarantined {len(invalid_df)} records to {invalid_path}")
        print(f"⚠️ Quarantined {len(invalid_df)} records (See logs).")

if __name__ == "__main__":
    run_ingestion_pipeline(5000)
