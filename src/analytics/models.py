import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
import os
import logging

class AnomalyDetector:
    def __init__(self, contamination=0.03):
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.features = ['speed_kmph', 'rpm', 'fuel_rate', 'engine_temp']
        
    def train(self, df: pd.DataFrame):
        """
        Trains the Isolation Forest model on specific features.
        In production, this would be an offline process.
        """
        logging.info("Training Isolation Forest model...")
        X = df[self.features].fillna(0)
        self.model.fit(X)
        self.is_trained = True
        
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns the DataFrame with an 'is_anomaly' column (-1 for anomaly, 1 for normal).
        """
        X = df[self.features].fillna(0)
        predictions = self.model.predict(X)
        
        results = df.copy()
        results['is_anomaly_ml'] = predictions
        
        # Filter only anomalies for easier viewing
        anomalies = results[results['is_anomaly_ml'] == -1]
        
        if not anomalies.empty:
            logging.info(f"ML Detected {len(anomalies)} anomalies.")
            
        return anomalies

    def save_model(self, path='../../models/iso_forest.pkl'):
        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(self.model, path)
        
    def load_model(self, path='../../models/iso_forest.pkl'):
        if os.path.exists(path):
            self.model = joblib.load(path)
            self.is_trained = True
        else:
            logging.warning("Model file not found.")

if __name__ == "__main__":
    # Test
    # Load some data
    import duckdb
    con = duckdb.connect()
    # Assuming relative path from src/analytics to data/
    df = con.query("SELECT * FROM read_parquet('../../data/telemetry_valid_*.parquet')").df()
    
    detector = AnomalyDetector()
    detector.train(df)
    anomalies = detector.predict(df)
    print(anomalies.head())
