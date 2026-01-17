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
        
        profiles = ['Normal', 'Aggressive', 'Eco', 'Malfunctioning']
        self.vehicle_profiles = {vid: random.choice(profiles) for vid in vehicle_ids}
        
        if len(vehicle_ids) >= 4:
            self.vehicle_profiles[vehicle_ids[0]] = 'Aggressive'
            self.vehicle_profiles[vehicle_ids[1]] = 'Eco'
            self.vehicle_profiles[vehicle_ids[2]] = 'Malfunctioning'
        
        self.state = {
            vid: {
                'speed': 0.0,
                'rpm': 800.0,
                'engine_temp': 70.0,
                'fuel_rate': 2.0,
                'battery_voltage': 13.5,
                'lat': 37.7749,
                'lon': -122.4194,
                'odometer': 10000.0
            }
            for vid in vehicle_ids
        }

    def _update_state(self, vid: str):
        """Evolve the state of the vehicle using random walk logic, biased by profile."""
        s = self.state[vid]
        profile = self.vehicle_profiles[vid]
        
        accel_bias = 0
        speed_cap = 220
        rpm_noise = 100
        
        if profile == 'Aggressive':
            accel_bias = 0.5 # Tends to accelerate
            accel_variance = 4 # Jerky driving
        elif profile == 'Eco':
            accel_bias = -0.1 # Tends to coast
            accel_variance = 1 # Smooth driving
            speed_cap = 120
        else:
            accel_variance = 2
            
        # Speed change (acceleration/deceleration)
        accel = np.random.normal(accel_bias, accel_variance) 
        s['speed'] = max(0, min(speed_cap, s['speed'] + accel))
        
        # RPM logic
        if s['speed'] == 0:
            if profile == 'Malfunctioning' and random.random() < 0.3:
                s['rpm'] = np.random.normal(1500, 200) 
            else:
                s['rpm'] = max(600, min(1000, s['rpm'] + np.random.normal(0, 20)))
        else:
            ratio = 30 if profile != 'Aggressive' else 40
            s['rpm'] = (s['speed'] * ratio) + np.random.normal(0, rpm_noise)
            
        if profile == 'Malfunctioning':
            s['engine_temp'] += np.random.normal(0.2, 0.1) # Overheats faster
        elif s['speed'] > 0:
            s['engine_temp'] += np.random.normal(0.1, 0.05)
        else:
            s['engine_temp'] -= 0.1
        s['engine_temp'] = max(20, min(120, s['engine_temp']))

        factor = 1.5 if profile != 'Aggressive' else 2.2
        s['fuel_rate'] = (s['rpm'] / 2000) * factor + np.random.normal(0, 0.1)
        
        if profile == 'Malfunctioning' and random.random() < 0.1:
             s['battery_voltage'] = 12.0 + np.random.normal(0, 0.5) # Alternator failing
        else:
             s['battery_voltage'] = 13.5 + np.random.normal(0, 0.1)
        
        s['lat'] += np.random.normal(0, 0.0001)
        s['lon'] += np.random.normal(0, 0.0001)
        
        self.state[vid] = s
        return s

    def _inject_anomaly(self, packet: dict):
        """Randomly injects different types of anomalies for detection testing."""
        roll = random.random()
        
        if roll < 0.01:
            packet['speed_kmph'] = -10 if random.random() < 0.5 else 300
            
        elif roll < 0.03:
            packet['engine_temp'] = 115 + random.uniform(0, 10)
            
        elif roll < 0.05:
            packet['battery_voltage'] = 11.5 - random.uniform(0, 2)
            
        elif roll < 0.08:
            packet['speed_kmph'] = 0
            packet['rpm'] = 2500 
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
                
                packet = self._inject_anomaly(packet)
                records.append(packet)
            
            current_time += timedelta(seconds=1)
            
        return pd.DataFrame(records)

if __name__ == "__main__":
    gen = TelemetryGenerator(['V001', 'V002', 'V003'])
    df = gen.generate_batch(datetime.now(), 100)
    print(df.head())
    print(f"Generated {len(df)} records.")
