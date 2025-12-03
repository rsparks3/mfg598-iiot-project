# Data Format Migration Summary

## Changes Completed

All code has been updated to use your actual data format from the `results/` folder.

### New Data Format

**nodes.json** (static reference):
- `num_nodes`: 1581
- `x`: array of 1581 x-coordinates
- `y`: array of 1581 y-coordinates

**snapshot_*.json** (telemetry data):
- `machine_id`: "MACHINE_001"
- `timestamp`: ISO timestamp
- `simulation_time`: e.g., "0.04"
- `num_nodes`: 1581
- `temperatures`: array of 1581 temperature values (in Kelvin)
- `power_consumption`: watts
- **NO vibration field**

### Files Modified

1. **cloud-device.py**
   - Updated database schema: removed `vibration`, added `simulation_time` and `num_nodes`
   - Changed from 10,000 to 1581 temperatures
   - Accepts temperature array (not comma-separated string)
   - All API endpoints updated

2. **edge-device.py**
   - Updated OPC UA variables: added `SimulationTime`, `NumNodes`, removed `Vibration`
   - Changed buffering/averaging logic for 1581 values
   - Handles temperature arrays (JSON format)

3. **physical-device.py**
   - Reads actual `snapshot_*.json` files from `results/` folder
   - Sends real data instead of generating random values
   - Iterates through all available snapshot files

4. **dashboard.py**
   - Loads `nodes.json` for node positions
   - Changed from 2D heatmap to scatter plot visualization
   - Uses x, y coordinates from nodes.json
   - Displays temperatures in Kelvin (with Celsius conversion)
   - Removed vibration metrics
   - Added simulation_time display

## Next Steps to Run the Demo

### 1. **Drop and Recreate Database**

The database schema has changed. You need to recreate it:

```powershell
# Drop the old database
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -h localhost -c "DROP DATABASE IF EXISTS telemetry_db;"

# Create new database
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -h localhost -c "CREATE DATABASE telemetry_db;"
```

### 2. **Start Services in Order**

**Terminal 1 - Cloud Device:**
```powershell
python .\cloud-device.py
```
Wait for: `Running on http://127.0.0.1:8067`

**Terminal 2 - Edge Device:**
```powershell
python .\edge-device.py
```
Wait for: `🚀 OPC UA Telemetry Server started`

**Terminal 3 - Physical Device (Data Sender):**
```powershell
python .\physical-device.py
```
This will read all `snapshot_*.json` files from `results/` and send them through the pipeline.

**Terminal 4 - Dashboard:**
```powershell
streamlit run .\dashboard.py
```
Opens browser at `http://localhost:8501`

### 3. **What to Expect**

- Physical device will find and send all snapshot files from `results/` folder
- Edge device will buffer and average every 4 data points
- Cloud device will store in PostgreSQL with 1581 temperatures per record
- Dashboard will show:
  - Scatter plot of temperature distribution using node positions
  - Temperatures in Kelvin (with Celsius conversion)
  - Power consumption in Watts
  - Simulation time for each snapshot
  - No vibration data

### 4. **Verify Data Flow**

Check cloud device received data:
```powershell
Invoke-RestMethod -Uri http://localhost:8067/health | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri http://localhost:8067/machines | ConvertTo-Json -Depth 5
```

## Troubleshooting

**If you see "No snapshot files found":**
- Verify `results/` folder exists in project root
- Check for files named `snapshot_*.json`
- Ensure they have the correct format

**If cloud device errors on database insert:**
- Make sure you dropped and recreated the database
- The schema has changed significantly

**If dashboard shows "Cannot load nodes.json":**
- Verify `results/nodes.json` exists
- Check file has `x`, `y`, and `num_nodes` fields

**If temperatures look wrong:**
- Data is in Kelvin (295.15K = 22°C)
- Dashboard shows both Kelvin and Celsius

## Data Validation

Current snapshot files in `results/`:
- `snapshot_00000002.json`
- `snapshot_00000003.json`
- `snapshot_00000004.json`

Each should have:
- 1581 temperature values
- Valid machine_id, timestamp, simulation_time
- Power consumption value

## Key Differences from Old System

| Aspect | Old | New |
|--------|-----|-----|
| Temperature count | 10,000 | 1,581 |
| Temperature format | Comma-string | JSON array |
| Visualization | 2D heatmap | Scatter plot |
| Position data | Implicit grid | Explicit x,y from nodes.json |
| Vibration | Included | **Removed** |
| Additional fields | None | simulation_time, num_nodes |
| Temperature unit | °C | Kelvin |
| Power unit | kW | Watts |

All changes are complete and ready to test!
