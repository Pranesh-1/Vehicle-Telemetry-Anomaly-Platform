import pandas as pd
import numpy as np
import random
import time
from datetime import datetime, timedelta

class TelemetryGenerator:
    """
    Generates synthetic vehicle telemetry data with realistic physics and controlled anomalies.
    """
    def __init__(self, vehicle_ids: list):
        self.vehicle_ids = vehicle_ids
        self.data_buffer = []
        
        # Initialize vehicle state (Simulation of current physics)
        self.state = {
            vid: {
                'speed': 0.0,
                'rpm': 800.0,
                'engine_temp': 70.0,
                'fuel_rate': 2.0,
                'battery_voltage': 13.5,
                'lat': 37.7749, # SF default
                'lon': -122.4194,
                'odometer': 10000.0
            }
            for vid in vehicle_ids
        }

    def _update_state(self, vid: str):
        """Evolve the state of the vehicle using random walk logic."""
        s = self.state[vid]
        
        # Speed change (acceleration/deceleration)
        accel = np.random.normal(0, 2) # Random accel
        s['speed'] = max(0, min(220, s['speed'] + accel))
        
        # RPM correlates with speed but has noise (and idle state)
        if s['speed'] == 0:
            s['rpm'] = max(600, min(1000, s['rpm'] + np.random.normal(0, 20))) # Idle RPM
        else:
            s['rpm'] = (s['speed'] * 30) + np.random.normal(0, 100) # Base gear ratio
            
        # Engine Temp (slowly rises, cools if stopped)
        if s['speed'] > 0:
            s['engine_temp'] += np.random.normal(0.1, 0.05)
        else:
            s['engine_temp'] -= 0.1
        s['engine_temp'] = max(20, min(110, s['engine_temp']))

        # Fuel Rate (L/hr)
        s['fuel_rate'] = (s['rpm'] / 2000) * 1.5 + np.random.normal(0, 0.1)
        
        # Battery Voltage (Alternator Simulation)
        # Normal is 13.5-14.5V when running, 12.0-12.6V when off. 
        # We simulate "running" mostly.
        s['battery_voltage'] = 13.5 + np.random.normal(0, 0.1)

        # GPS Drift
        s['lat'] += np.random.normal(0, 0.0001)
        s['lon'] += np.random.normal(0, 0.0001)
        
        self.state[vid] = s
        return s

    def _inject_anomaly(self, packet: dict):
        """Randomly injects different types of anomalies for detection testing."""
        roll = random.random()
        
        # 1. Impossible Speed (1% prob) - Data Integrity Check
        if roll < 0.01:
            packet['speed_kmph'] = -10 if random.random() < 0.5 else 300
            
        # 2. Engine Overheat (2% prob) - Rule Based
        elif roll < 0.03:
            packet['engine_temp'] = 115 + random.uniform(0, 10)
            
        # 3. Voltage Drop (Simulate bad alternator) (2% prob) - Trend/Rule
        elif roll < 0.05:
            packet['battery_voltage'] = 11.5 - random.uniform(0, 2)
            
        # 4. High Idle (High Fuel Consumption at 0 Speed) (3% prob) - Business Insight
        elif roll < 0.08:
            packet['speed_kmph'] = 0
            packet['rpm'] = 2500 # Revving engine while stopped
            packet['fuel_rate'] = 5.0

        return packet

    def generate_batch(self, start_time: datetime, num_records: int) -> pd.DataFrame:
        """
        Generates a batch of telemetry data.
        """
        records = []
        current_time = start_time
        
        for _ in range(num_records // len(self.vehicle_ids)):
            for vid in self.vehicle_ids:
                state = self._update_state(vid)
                
                packet = {
                    'vehicle_id': vid,
                    'timestamp': current_time,
                    'speed_kmph': round(state['speed'], 2),
                    'rpm': int(state['rpm']),
                    'engine_temp': round(state['engine_temp'], 1),
                    'fuel_rate': round(state['fuel_rate'], 2),
                    'battery_voltage': round(state['battery_voltage'], 2),
                    'lat': state['lat'],
                    'lon': state['lon']
                }
                
                # Inject anomaly
                packet = self._inject_anomaly(packet)
                records.append(packet)
            
            # Increment time (e.g. 1 second)
            current_time += timedelta(seconds=1)
            
        return pd.DataFrame(records)

if __name__ == "__main__":
    # Test generation
    gen = TelemetryGenerator(['V001', 'V002', 'V003'])
    df = gen.generate_batch(datetime.now(), 100)
    print(df.head())
    print(f"Generated {len(df)} records.")
