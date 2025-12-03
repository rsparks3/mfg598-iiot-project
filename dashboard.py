import streamlit as st
import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'telemetry_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data(ttl=5)
def get_all_machines():
    """Get list of all unique machine IDs."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT machine_id FROM telemetry ORDER BY machine_id")
    machines = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return machines

@st.cache_data(ttl=5)
def get_telemetry_records(machine_id=None):
    """Get all telemetry records, optionally filtered by machine_id."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    if machine_id:
        cur.execute("""
            SELECT id, machine_id, timestep, temperatures, power_consumption, 
                   vibration, received_at, min_temp, max_temp, mean_temp, std_temp
            FROM telemetry
            WHERE machine_id = %s
            ORDER BY received_at ASC
        """, (machine_id,))
    else:
        cur.execute("""
            SELECT id, machine_id, timestep, temperatures, power_consumption, 
                   vibration, received_at, min_temp, max_temp, mean_temp, std_temp
            FROM telemetry
            ORDER BY received_at ASC
        """)
    
    records = cur.fetchall()
    cur.close()
    conn.close()
    return records

def create_temperature_heatmap(temp_array, title="Temperature Heatmap"):
    """Create a Plotly heatmap from temperature array."""
    fig = go.Figure(data=go.Heatmap(
        z=temp_array,
        colorscale='RdBu_r',  # Red for hot, Blue for cold
        colorbar=dict(title="Temperature (°C)"),
        hoverongaps=False,
        hovertemplate='Row: %{y}<br>Col: %{x}<br>Temp: %{z:.2f}°C<extra></extra>'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Column",
        yaxis_title="Row",
        height=600,
        width=700,
        yaxis=dict(autorange='reversed')  # Start from top
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="DMLS Printing Supervisory Dashboard",
        page_icon="🌡️",
        layout="wide"
    )
    
    st.title("DMLS Printing Supervisory Dashboard")
    st.markdown("Real-time temperature monitoring and analysis")
    
    # Sidebar controls
    st.sidebar.header("Controls")
    
    # Get available machines
    machines = get_all_machines()
    
    if not machines:
        st.warning("No telemetry data available in the database.")
        st.info("Make sure the cloud-device.py server is running and receiving data.")
        return
    
    # Machine selector dropdown
    selected_machine = st.sidebar.selectbox(
        "Select Machine",
        options=machines,
        index=0
    )
    
    # Get telemetry records for selected machine
    records = get_telemetry_records(selected_machine)
    
    if not records:
        st.warning(f"No data available for machine {selected_machine}")
        return
    
    # Time slider
    st.sidebar.markdown("---")
    st.sidebar.subheader("Time Navigation")
    
    record_index = st.sidebar.slider(
        "Record Index",
        min_value=0,
        max_value=len(records) - 1,
        value=len(records) - 1,  # Start at most recent
        step=1,
        help="Slide to view historical data"
    )
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (every 5s)", value=False)
    
    if auto_refresh:
        st.sidebar.info("Dashboard will refresh every 5 seconds")
        # This will trigger a rerun every 5 seconds
        import time
        time.sleep(5)
        st.rerun()
    
    # Stop button (placeholder functionality)
    st.sidebar.markdown("---")
    if st.sidebar.button("🛑 STOP", type="primary", use_container_width=True):
        st.sidebar.error("Stop button pressed!")
        st.sidebar.info("TBD: Stop functionality to be implemented")
        # Placeholder for stop functionality
        # Could be used to stop data collection, pause monitoring, etc.
    
    # Get selected record
    record = records[record_index]
    
    # Parse temperature array
    temp_array = np.array(record['temperatures'])
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Temperature heatmap
        st.subheader(f"Temperature Grid - {selected_machine}")
        
        heatmap_title = f"Timestep: {record['timestep']} | Received: {record['received_at'].strftime('%Y-%m-%d %H:%M:%S')}"
        fig = create_temperature_heatmap(temp_array, heatmap_title)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Statistics and metadata
        st.subheader("Statistics")
        
        # Temperature stats
        st.metric("Min Temperature", f"{record['min_temp']:.2f} °C")
        st.metric("Max Temperature", f"{record['max_temp']:.2f} °C")
        st.metric("Mean Temperature", f"{record['mean_temp']:.2f} °C")
        st.metric("Std Deviation", f"{record['std_temp']:.2f} °C")
        
        st.markdown("---")
        
        # Other sensor data
        st.subheader("Sensor Data")
        st.metric("Power Consumption", f"{record['power_consumption']:.2f} kW")
        st.metric("Vibration", f"{record['vibration']:.2f} mm/s")
        
        st.markdown("---")
        
        # Record info
        st.subheader("Record Info")
        st.text(f"Record ID: {record['id']}")
        st.text(f"Machine: {record['machine_id']}")
        st.text(f"Timestep: {record['timestep']}")
        st.text(f"Received: {record['received_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        st.text(f"Record {record_index + 1} of {len(records)}")
    
    # Timeline chart showing all records for this machine
    st.markdown("---")
    st.subheader("Historical Trends")
    
    # Create dataframe for plotting
    df = pd.DataFrame([
        {
            'timestamp': r['received_at'],
            'mean_temp': r['mean_temp'],
            'power': r['power_consumption'],
            'vibration': r['vibration']
        } for r in records
    ])
    
    # Plot trends
    col3, col4, col5 = st.columns(3)
    
    with col3:
        st.line_chart(df.set_index('timestamp')['mean_temp'], height=200)
        st.caption("Mean Temperature Over Time")
    
    with col4:
        st.line_chart(df.set_index('timestamp')['power'], height=200)
        st.caption("Power Consumption Over Time")
    
    with col5:
        st.line_chart(df.set_index('timestamp')['vibration'], height=200)
        st.caption("Vibration Over Time")
    
    # Data table at the bottom (collapsible)
    with st.expander("View Raw Data Table"):
        display_df = pd.DataFrame([
            {
                'ID': r['id'],
                'Machine': r['machine_id'],
                'Timestep': r['timestep'],
                'Min Temp': f"{r['min_temp']:.2f}",
                'Max Temp': f"{r['max_temp']:.2f}",
                'Mean Temp': f"{r['mean_temp']:.2f}",
                'Power (kW)': f"{r['power_consumption']:.2f}",
                'Vibration (mm/s)': f"{r['vibration']:.2f}",
                'Received': r['received_at'].strftime('%Y-%m-%d %H:%M:%S')
            } for r in records
        ])
        st.dataframe(display_df, use_container_width=True, height=300)

if __name__ == "__main__":
    main()
