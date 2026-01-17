import duckdb
import os
import logging

class DatabaseManager:
    def __init__(self, db_path=':memory:'):
        self.con = duckdb.connect(db_path)
        self._setup()

    def _setup(self):
        """Initial setup."""
        # We can implement persistent schema here if using a file-based DB
        pass

    def load_data(self, data_dir='../../data'):
        """
        Loads all valid parquet files from data/ directory into a DuckDB view.
        Using a VIEW for zero-copy query over Parquet files (High Performance).
        """
        try:
            parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.parquet')]
            if not parquet_files:
                logging.warning("No parquet files found to load.")
                return
            
            # DuckDB generic glob pattern for loading multiple files
            # escaping backslashes for Windows if needed, but simple forward slash works in duckdb usually
            glob_pattern = os.path.join(data_dir, "telemetry_*.parquet")
            
            query = f"""
            CREATE OR REPLACE VIEW telemetry AS 
            SELECT * FROM read_parquet('{glob_pattern}');
            """
            self.con.execute(query)
            logging.info("Data loaded into 'telemetry' view.")
            
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            raise e

    def execute_query(self, query: str):
        """Executes a raw SQL query and returns a Pandas DataFrame."""
        return self.con.execute(query).df()
    
    def get_summary(self):
         return self.execute_query("SELECT COUNT(*) as total_records, COUNT(DISTINCT vehicle_id) as vehicles FROM telemetry")

if __name__ == "__main__":
    # Test
    db = DatabaseManager()
    db.load_data('../../data')
    print(db.get_summary())
