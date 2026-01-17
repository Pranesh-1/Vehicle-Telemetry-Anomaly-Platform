import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.db_manager import DatabaseManager
from analytics.models import AnomalyDetector
from analytics.insights import FleetInsights

st.set_page_config(layout="wide", page_title="Motorq Telemetry Analytics")

st.markdown("""
<style>
    /* Global Font and Background */
    .stApp {
        background-color: #0E1117;
        font-family: 'Inter', sans-serif;
    }
    
    /* Metrics Cards */
    .stMetric {
        background-color: #161B22;
        border: 1px solid #30363D;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #E6EDF3 !important;
        font-weight: 600;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        color: #8B949E;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: transparent;
        color: #58A6FF;
        border-bottom: 2px solid #58A6FF;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_db():
    # Calculate absolute path to data directory
    # src/dashboard/app.py -> src/dashboard -> src -> project_root -> data
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_dir = os.path.join(base_dir, 'data')
    
    # Cloud Deployment Check: If no data exists, generate seed data
    if not os.path.exists(data_dir) or not any(f.endswith('.parquet') for f in os.listdir(data_dir)):
        st.warning("⚠️ No data found (Cloud Deployment Detected). Generating seed data... Please wait.")
        try:
            # Import dynamically to avoid top-level path issues
            sys.path.append(os.path.join(base_dir, 'src'))
            from ingestion.ingestor import run_ingestion_pipeline
            run_ingestion_pipeline(num_records=5000) # Generate 5k records for the cloud demo
            st.success("✅ Data generated successfully!")
        except Exception as e:
            st.error(f"Failed to generate data: {e}")
            st.stop() # Stop execution if data generation fails
            
    # Double check if data directory exists now
    if not os.path.exists(data_dir):
        st.error(f"Critical Error: Data Directory {data_dir} could not be created.")
        st.stop()
            
    db = DatabaseManager()
    db.load_data(data_dir)
    return db

@st.cache_resource
def get_insights_engine():
    return FleetInsights()

@st.cache_resource
def get_anomaly_detector():
    return AnomalyDetector()

def main():
    st.title("Vehicle Telemetry & Anomaly Platform")
    st.markdown("### Production-Grade Fleet Analytics System")

    db = get_db()
    
    # --- Sidebar ---
    st.sidebar.header("Filter Options")
    
    # Load Data View
    try:
        df = db.execute_query("SELECT * FROM telemetry")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    vehicles = df['vehicle_id'].unique()
    selected_vehicle = st.sidebar.selectbox("Select Vehicle", ["All"] + list(vehicles))
    
    st.sidebar.markdown("---")
    st.sidebar.caption("Data shown is synthetic but statistically realistic.")
    
    # --- Main Dashboard ---
    
    # Tab Structure
    tab1, tab2, tab3, tab4 = st.tabs(["Fleet Overview", "Vehicle Drill-Down", "Anomaly Detection", "GenAI Insights"])
    
    with tab1:
        st.subheader("Fleet Health Overview")
        
        # High Level KPIs
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Vehicles", len(vehicles))
        col2.metric("Total Data Points", len(df))
        
        # Idle Time Analysis (Business Insight)
        idle_df = db.execute_query("SELECT SUM(CASE WHEN speed_kmph=0 AND rpm>0 THEN 1 ELSE 0 END) as idle_count FROM telemetry")
        col3.metric("Total Idle Events", idle_df['idle_count'][0])
        
        avg_speed = df['speed_kmph'].mean()
        col4.metric("Fleet Avg Speed", f"{avg_speed:.1f} km/h")
        
        # Visuals
        st.markdown("#### Real-time Speed Distribution")
        fig_hist = px.histogram(df, x="speed_kmph", color="vehicle_id", nbins=50, title="Speed Distribution by Vehicle")
        fig_hist.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
        st.plotly_chart(fig_hist, use_container_width=True)

    with tab2:
        if selected_vehicle == 'All':
            st.info("Please select a specific vehicle from the sidebar to view details.")
        else:
            st.subheader(f"Deep Dive: {selected_vehicle}")
            
            v_df = df[df['vehicle_id'] == selected_vehicle]
            
            # Time Series Chart
            fig_ts = go.Figure()
            fig_ts.add_trace(go.Scatter(x=v_df['timestamp'], y=v_df['speed_kmph'], name='Speed (km/h)'))
            fig_ts.add_trace(go.Scatter(x=v_df['timestamp'], y=v_df['rpm'] / 10, name='RPM (x10)', line=dict(dash='dot')))
            fig_ts.update_layout(title="Speed vs RPM Correlated View", hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig_ts, use_container_width=True)
            
            # Business Risk Check
            insights_engine = get_insights_engine()
            vehicle_risk = insights_engine.generate_vehicle_insights(selected_vehicle, v_df)
            
            st.markdown("#### Vehicle Risk Profile")
            col_r1, col_r2 = st.columns([1, 2])
            
            with col_r1:
                st.metric("Health Score", f"{vehicle_risk['health_score']}/100")
            
            with col_r2:
                if vehicle_risk['risks']:
                    for risk in vehicle_risk['risks']:
                        st.error(f"Risk Detected: {risk}")
                    for action in vehicle_risk['action_items']:
                        st.info(f"Action: {action}")
                else:
                    st.success("No critical risks detected.")

    with tab3:
        st.subheader("Anomaly Detection (Isolation Forest)")
        
        # Train/Predict on the fly for demo (In prod this is pre-computed)
        detector = get_anomaly_detector()
        detector.train(df)
        anomalies = detector.predict(df)
        
        st.warning(f"Detected {len(anomalies)} anomalies across the fleet.")
        
        if not anomalies.empty:
            st.dataframe(anomalies[['vehicle_id', 'timestamp', 'speed_kmph', 'rpm', 'fuel_rate', 'is_anomaly_ml']].head(50))
            
            # Scatter Plot of Anomalies
            fig_anom = px.scatter(anomalies, x="speed_kmph", y="rpm", color="vehicle_id", title="Anomalous Operational Points")
            fig_anom.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig_anom, use_container_width=True)

    with tab4:
        st.subheader("Executive Summary (Powered by Groq)")
        
        # --- CONFIGURATION ---
        GROQ_API_KEY = "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" 
        # ---------------------
        
        if st.button("Generate Insight"):
            from utils.ai_summarizer import AISummarizer
            
            # Gather Real Metrics for the AI
            metrics = {
                'total_vehicles': len(vehicles),
                'avg_speed': avg_speed,
                'idle_events': idle_df['idle_count'][0]
            }
            # Get top anomalies
            top_anomalies = anomalies['vehicle_id'].unique().tolist() if not anomalies.empty else []
            
            with st.spinner("Analyzing fleet telemetry with Groq (Llama3)..."):
                summarizer = AISummarizer(api_key=GROQ_API_KEY)
                summary = summarizer.generate_summary(metrics, top_anomalies)
                
            st.markdown("### Assistant Report")
            st.markdown(summary)
            
            if not GROQ_API_KEY:
                st.warning("Please provide a valid Groq API Key.")

if __name__ == "__main__":
    main()
