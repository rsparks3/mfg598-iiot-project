import streamlit as st
import requests
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import os
import json
from dotenv import load_dotenv

load_dotenv()

# API configuration
API_BASE_URL = os.getenv('API_BASE_URL', 'http://localhost:8067')

# Load nodes.json for position data
@st.cache_data
def load_nodes():
    """Load node positions from nodes.json file."""
    try:
        nodes_path = os.path.join(os.path.dirname(__file__), 'results', 'nodes.json')
        with open(nodes_path, 'r') as f:
            nodes = json.load(f)
        return nodes
    except Exception as e:
        st.error(f"Failed to load nodes.json: {str(e)}")
        return None

@st.cache_data(ttl=5)
def get_all_machines():
    """Get list of all unique machine IDs from API."""
    try:
        response = requests.get(f"{API_BASE_URL}/machines")
        response.raise_for_status()
        data = response.json()
        return data.get('machines', [])
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch machines: {str(e)}")
        return []

@st.cache_data(ttl=5)
def get_telemetry_records(machine_id=None):
    """Get all telemetry records from API, optionally filtered by machine_id."""
    try:
        params = {'machine_id': machine_id} if machine_id else {}
        response = requests.get(f"{API_BASE_URL}/telemetry", params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert API response to match expected format
        records = []
        for item in data.get('data', []):
            records.append({
                'id': item['id'],
                'machine_id': item['machine_id'],
                'timestep': item['timestep'],
                'simulation_time': item.get('simulation_time', ''),
                'num_nodes': item.get('num_nodes', 1581),
                'temperatures': item['temperatures'],
                'power_consumption': item['power_consumption'],
                'received_at': datetime.fromisoformat(item['received_at']),
                'min_temp': item['stats']['min'],
                'max_temp': item['stats']['max'],
                'mean_temp': item['stats']['mean'],
                'std_temp': item['stats']['std']
            })
        return records
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch telemetry data: {str(e)}")
        return []

def create_temperature_scatter(nodes, temp_array, title="Temperature Distribution"):
    """Create a Plotly scatter plot from temperature array using node positions."""
    if nodes is None:
        st.error("Cannot create plot: nodes.json not loaded")
        return None
    
    x_coords = nodes['x']
    y_coords = nodes['y']
    
    # Ensure temperature array is 1D
    if isinstance(temp_array, list) and isinstance(temp_array[0], list):
        # Flatten if it's 2D
        temp_array = [item for sublist in temp_array for item in sublist]
    
    # Convert to numpy array
    temps = np.array(temp_array)
    
    # Validate data
    if len(temps) != len(x_coords) or len(temps) != len(y_coords):
        st.warning(f"Data mismatch: {len(temps)} temps, {len(x_coords)} x coords, {len(y_coords)} y coords")
        # Trim to shortest length
        min_len = min(len(temps), len(x_coords), len(y_coords))
        temps = temps[:min_len]
        x_coords = x_coords[:min_len]
        y_coords = y_coords[:min_len]
    
    # Filter nodes based on x and y coordinate ranges
    # Keep only nodes where: 1.5 <= x <= 4.5 and 1.5 <= y <= 8.5
    filtered_indices = [
        i for i in range(len(x_coords))
        if 1.5 <= x_coords[i] <= 4.5 and 1.5 <= y_coords[i] <= 8.5
    ]
    
    # Apply filter to all arrays
    x_coords = [x_coords[i] for i in filtered_indices]
    y_coords = [y_coords[i] for i in filtered_indices]
    temps = temps[filtered_indices]
    
    fig = go.Figure(data=go.Scatter(
        x=x_coords,
        y=y_coords,
        mode='markers',
        marker=dict(
            size=10, 
            color=temps,
            colorscale='RdBu_r',  # Red for hot, Blue for cold
            colorbar=dict(title="Temperature (K)"),
            cmin=np.min(temps),
            cmax=np.max(temps),
            showscale=True,
            symbol='square'  # Use squares instead of circles
        ),
        text=[f"Temp: {t:.2f}K" for t in temps],
        hovertemplate='X: %{x:.2f}<br>Y: %{y:.2f}<br>%{text}<extra></extra>'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="X Position (mm)",
        yaxis_title="Y Position (mm)",
        height=600,
        width=700,
        showlegend=False,
        yaxis=dict(
            scaleanchor="x",  # Link y-axis scale to x-axis
            scaleratio=1,     # 1:1 aspect ratio
        ),
        xaxis=dict(
            constrain='domain'
        )
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
    
    if not records or len(records) == 0:
        st.warning(f"No data available for machine {selected_machine}")
        st.info("Waiting for telemetry data to arrive...")
        return
    
    # Time slider
    st.sidebar.markdown("---")
    st.sidebar.subheader("Time Navigation")
    
    # Only show slider if there are multiple records
    if len(records) > 1:
        record_index = st.sidebar.slider(
            "Record Index",
            min_value=0,
            max_value=len(records) - 1,
            value=len(records) - 1,  # Start at most recent
            step=1,
            help="Slide to view historical data"
        )
    else:
        record_index = 0
        st.sidebar.info("Only 1 record available")
    
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
    
    # Load nodes data
    nodes = load_nodes()
    if nodes is None:
        st.error("Cannot display temperature visualization without nodes.json")
        return
    
    # Get selected record
    record = records[record_index]
    
    # Parse temperature array
    temp_array = record['temperatures']
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Temperature scatter plot
        st.subheader(f"Temperature Distribution - {selected_machine}")
        
        plot_title = f"Timestep: {record['timestep']} | Sim Time: {record['simulation_time']} | Received: {record['received_at'].strftime('%Y-%m-%d %H:%M:%S')}"
        fig = create_temperature_scatter(nodes, temp_array, plot_title)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Statistics and metadata
        st.subheader("Statistics")
        
        # Temperature stats (convert from Kelvin to Celsius for display)
        st.metric("Min Temperature", f"{record['min_temp']:.2f} K ({record['min_temp']-273.15:.2f} °C)")
        st.metric("Max Temperature", f"{record['max_temp']:.2f} K ({record['max_temp']-273.15:.2f} °C)")
        st.metric("Mean Temperature", f"{record['mean_temp']:.2f} K ({record['mean_temp']-273.15:.2f} °C)")
        st.metric("Std Deviation", f"{record['std_temp']:.2f} K")
        
        st.markdown("---")
        
        # Other sensor data
        st.subheader("Sensor Data")
        st.metric("Power Consumption", f"{record['power_consumption']:.2f} W")
        st.metric("Number of Nodes", f"{record['num_nodes']}")
        
        st.markdown("---")
        
        # Record info
        st.subheader("Record Info")
        st.text(f"Record ID: {record['id']}")
        st.text(f"Machine: {record['machine_id']}")
        st.text(f"Timestep: {record['timestep']}")
        st.text(f"Simulation Time: {record['simulation_time']}")
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
            'power': r['power_consumption']
        } for r in records
    ])
    
    # Plot trends
    col3, col4 = st.columns(2)
    
    with col3:
        st.line_chart(df.set_index('timestamp')['mean_temp'], height=200)
        st.caption("Mean Temperature Over Time (K)")
    
    with col4:
        st.line_chart(df.set_index('timestamp')['power'], height=200)
        st.caption("Power Consumption Over Time (W)")
    
    # Data table at the bottom (collapsible)
    with st.expander("View Raw Data Table"):
        display_df = pd.DataFrame([
            {
                'ID': r['id'],
                'Machine': r['machine_id'],
                'Timestep': r['timestep'],
                'Sim Time': r['simulation_time'],
                'Nodes': r['num_nodes'],
                'Min Temp (K)': f"{r['min_temp']:.2f}",
                'Max Temp (K)': f"{r['max_temp']:.2f}",
                'Mean Temp (K)': f"{r['mean_temp']:.2f}",
                'Power (W)': f"{r['power_consumption']:.2f}",
                'Received': r['received_at'].strftime('%Y-%m-%d %H:%M:%S')
            } for r in records
        ])
        st.dataframe(display_df, use_container_width=True, height=300)

if __name__ == "__main__":
    main()
