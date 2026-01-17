# 🚗 Vehicle Telemetry Analytics & Anomaly Detection

**Production-Grade Analytics Platform for Connected Vehicles**

![Project Status](https://img.shields.io/badge/Status-Complete-green) ![Stack](https://img.shields.io/badge/Stack-Python%20|%20DuckDB%20|%20Streamlit-blue)

## 🎯 Project Overview
This project simulates a high-frequency vehicle telemetry ingestion and analytics pipeline. It processes sensor data (Speed, RPM, Voltage, Fuel), ensures data integrity through strict schema validation, detects anomalies using both Rule-Based logic and Unsupervised Learning (Isolation Forests), and serves business insights via an interactive dashboard.

**Key Features:**
- **Data Integrity**: Validates `physics` constraints (e.g., negative speed, impossibly high RPM).
- **Hybrid Anomaly Detection**:
    - **Rules**: Catch obvious failures (Overheating, Voltage Drops).
    - **ML (Isolation Forest)**: Detects subtle multivariate outliers (e.g., High Fuel Rate while Idling).
- **Business Insights**: Translates technical signals into operational risks (Safety, Maintenance, Fuel Efficiency).
- **Scalable Architecture**: Uses **DuckDB** for zero-copy high-performance OLAP queries on Parquet files.

## 🏗️ Architecture
`Telemetry Stream (Synthetic)` → `Ingestion (Validation + Quarantine)` → `Bronze Layer (Parquet)` → `DuckDB (OLAP)` → `Streamlit (Viz)`

## 🚀 How to Run

1. **Setup Environment**
   ```bash
   pip install -r requirements.txt
   ```

2. **Generate & Ingest Data**
   ```bash
   cd src/ingestion
   python ingestor.py
   # Generates 5000 records, validates them, and saves to data/
   ```

3. **Launch Dashboard**
   ```bash
   cd src/dashboard
   streamlit run app.py
   ```

## 📊 Analytics & Insights
We focus on **Time-Series** patterns:
- **Rolling Averages**: To detect engine temperature trends before overheating occurs.
- **Idling Analysis**: Calculates fuel wasted when `Velocity = 0` but `RPM > 0`.
- **Battery Health**: Tracks voltage decay over time to predict alternator failures.

## 🧠 Approach: Why Rules + ML?
We use a **Hybrid Approach**:
1. **Rules** are used for known failure modes (Safety limits, Physics bounds). They are instant and explainable.
2. **ML (Isolation Forest)** is used for *Unknown Unknowns*. It finds data points that are technically "valid" but operationally "weird" relative to the fleet's distribution.

## 🤖 GenAI Scope
GenAI is used **strictly for summarization** of the final analytical report. No critical safety or operational decisions are made by the LLM. It serves as a translator from "Data" to "Executive Summary".
**GenAI is used strictly for summarization and never affects anomaly detection or scoring.**

## ⚡ Performance
We benchmarked **Pandas vs DuckDB**.
DuckDB provides a significant speedup for aggregation queries on larger datasets due to its columnar vectorized execution engine, simulating how Motorq handles millions of rows.
