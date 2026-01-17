-- 1. Rolling Average Engine Temp (Trend Detection)
-- Detects if a vehicle is heating up over time (10-row rolling window)
SELECT 
    vehicle_id,
    timestamp,
    engine_temp,
    AVG(engine_temp) OVER (
        PARTITION BY vehicle_id 
        ORDER BY timestamp 
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) as rolling_avg_temp
FROM telemetry
ORDER BY vehicle_id, timestamp;

-- 2. Idle Time Aggregation (Business Insight: Idling = Fuel Waste)
-- Counts minutes where speed is 0 but engine is running (RPM > 500)
SELECT 
    vehicle_id, 
    DATE(timestamp) as log_date,
    COUNT(*) / 60.0 as idle_hours,
    SUM(fuel_rate) / 3600.0 as wasted_fuel_liters
FROM telemetry
WHERE speed_kmph = 0 AND rpm > 500
GROUP BY 1, 2
ORDER BY wasted_fuel_liters DESC;

-- 3. Rate of Change Detection (Sudden Events)
-- Detects harsh braking or sudden acceleration using LAG
WITH calc AS (
    SELECT 
        vehicle_id,
        timestamp,
        speed_kmph,
        LAG(speed_kmph) OVER (PARTITION BY vehicle_id ORDER BY timestamp) as prev_speed,
        (speed_kmph - LAG(speed_kmph) OVER (PARTITION BY vehicle_id ORDER BY timestamp)) as speed_delta
    FROM telemetry
)
SELECT * 
FROM calc
WHERE ABS(speed_delta) > 20 -- >20 kmph change in 1 second is harsh
ORDER BY timestamp DESC;

-- 4. Fleet Health Overview
SELECT
    vehicle_id,
    AVG(battery_voltage) as avg_voltage,
    MAX(engine_temp) as max_temp,
    AVG(fuel_rate) as avg_fuel_rate,
    MAX(speed_kmph) as max_speed
FROM telemetry
GROUP BY vehicle_id;
