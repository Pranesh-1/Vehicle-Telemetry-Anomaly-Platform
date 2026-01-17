import pandas as pd
import logging

class FleetInsights:
    def __init__(self):
        pass

    def calculate_health_score(self, aggregated_metrics: dict) -> int:
        """
        Calculates a 0-100 Health Score based on weighted risks.
        Start at 100.
        - Deduct 10 for Battery Risk.
        - Deduct 20 for Safety Risk.
        - Deduct 15 for Fuel Risk.
        """
        score = 100
        if aggregated_metrics.get('safety_risk'):
            score -= 20
        if aggregated_metrics.get('battery_risk'):
            score -= 10
        if aggregated_metrics.get('fuel_risk'):
            score -= 15
            
        return max(0, score)

    def generate_vehicle_insights(self, vehicle_id: str, df: pd.DataFrame) -> dict:
        """
        Analyzes a specific vehicle's history to generate key business tags.
        """
        insights = {
            'vehicle_id': vehicle_id,
            'risks': [],
            'action_items': []
        }
        
        # 1. Fuel Efficiency Analysis (High Idle Time)
        # Definition: Speed = 0, RPM > 0
        idle_data = df[(df['speed_kmph'] == 0) & (df['rpm'] > 0)]
        idle_time_pct = len(idle_data) / len(df) if len(df) > 0 else 0
        
        if idle_time_pct > 0.15: # >15% time idling
            insights['risks'].append("High Fuel Waste")
            insights['action_items'].append(f"Driver coaching needed: {round(idle_time_pct*100, 1)}% idle time detected.")
            insights['fuel_risk'] = True

        # 2. Battery Integrity Analysis (Voltage Drop Trend)
        avg_voltage = df['battery_voltage'].mean()
        if avg_voltage < 12.8:
            insights['risks'].append("Battery Failure Imminent")
            insights['action_items'].append("Schedule battery replacement.")
            insights['battery_risk'] = True

        # 3. Safety Compliance (Overspeeding)
        overspeed_events = len(df[df['speed_kmph'] > 120])
        if overspeed_events > 5:
            insights['risks'].append("Safety Compliance Violation")
            insights['action_items'].append("Flag for safety review.")
            insights['safety_risk'] = True
            
        insights['health_score'] = self.calculate_health_score(insights)
        
        return insights

    def generate_fleet_summary(self, df: pd.DataFrame):
        """
        Aggregates insights across the entire fleet.
        """
        unique_vehicles = df['vehicle_id'].unique()
        fleet_report = []
        
        for vid in unique_vehicles:
            v_df = df[df['vehicle_id'] == vid]
            insight = self.generate_vehicle_insights(vid, v_df)
            fleet_report.append(insight)
            
        return fleet_report

if __name__ == "__main__":
    # Test
    # Mock DF
    data = {
        'vehicle_id': ['V1'] * 100,
        'speed_kmph': [0]*20 + [100]*80,
        'rpm': [800]*20 + [2000]*80,
        'battery_voltage': [13.5]*100,
        'fuel_rate': [1]*100
    }
    df = pd.DataFrame(data)
    
    ins = FleetInsights()
    print(ins.generate_vehicle_insights('V1', df))
