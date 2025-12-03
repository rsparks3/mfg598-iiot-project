import asyncio
import glob
import json
import os
from datetime import datetime
from asyncua import Client

# Configuration
MACHINE_ID = "MACHINE_001"  # Change this to your machine identifier
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

async def send_single_telemetry(client, iteration, snapshot_file):
    """
    Send a single telemetry data packet via OPC UA from snapshot JSON file.
    """
    # Load snapshot data from JSON file
    with open(snapshot_file, 'r') as f:
        snapshot_data = json.load(f)
    
    # Extract data from snapshot
    machine_id = snapshot_data.get('machine_id', MACHINE_ID)
    timestamp = snapshot_data.get('timestamp', datetime.now().isoformat())
    simulation_time = snapshot_data.get('simulation_time', '')
    power_consumption = snapshot_data.get('power_consumption', 0.0)
    num_nodes = snapshot_data.get('num_nodes', 1581)
    temperatures = snapshot_data.get('temperatures', [])
    
    # Validate data
    if len(temperatures) != 1581:
        print(f"⚠️  Warning: Expected 1581 temperatures, got {len(temperatures)}")
    
    # Create the request payload
    payload = {
        "machine_id": machine_id,
        "timestep": timestamp,
        "simulation_time": simulation_time,
        "num_nodes": num_nodes,
        "temperatures": temperatures,
        "power_consumption": power_consumption
    }
    
    # Create output directory if it doesn't exist
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "test-data", "telemetry-requests")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save payload to JSON file
    output_filename = os.path.join(output_dir, f"telemetry_request_opcua_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(output_filename, 'w') as f:
        json.dump(payload, f, indent=2)
    print(f"✅ [{iteration}] Request payload saved to: {output_filename}")
    print()
    
    print(f"[{iteration}] Sending telemetry data with {len(temperatures)} temperature values via OPC UA...")
    print(f"Machine ID: {payload['machine_id']}")
    print(f"Timestamp: {payload['timestep']}")
    print(f"Simulation Time: {payload['simulation_time']}")
    print(f"Temperature sample (first 5): {temperatures[:5]}")
    print(f"Total values: {len(temperatures)}")
    print(f"Power Consumption: {payload['power_consumption']} W")
    print()
    
    try:
        # Get the root node
        root = client.nodes.root
        
        # Navigate to TelemetryObject
        objects = await root.get_child(["0:Objects"])
        telemetry_obj = await objects.get_child(["2:TelemetryObject"])
        
        # Get the variable nodes
        machine_id_var = await telemetry_obj.get_child(["2:MachineID"])
        timestep_var = await telemetry_obj.get_child(["2:Timestep"])
        simulation_time_var = await telemetry_obj.get_child(["2:SimulationTime"])
        num_nodes_var = await telemetry_obj.get_child(["2:NumNodes"])
        temperatures_var = await telemetry_obj.get_child(["2:Temperatures"])
        power_var = await telemetry_obj.get_child(["2:PowerConsumption"])
        trigger_var = await telemetry_obj.get_child(["2:TriggerStorage"])
        result_var = await telemetry_obj.get_child(["2:LastRecordID"])
        
        # Write telemetry data to variables
        print("📤 Writing telemetry data to OPC UA variables...")
        await machine_id_var.write_value(payload['machine_id'])
        await timestep_var.write_value(payload['timestep'])
        await simulation_time_var.write_value(payload['simulation_time'])
        await num_nodes_var.write_value(payload['num_nodes'])
        await temperatures_var.write_value(json.dumps(payload['temperatures']))
        await power_var.write_value(payload['power_consumption'])
        
        # Trigger storage by setting trigger to True
        print("📤 Triggering storage...")
        await trigger_var.write_value(True)
        
        # Wait a moment for processing
        await asyncio.sleep(0.5)
        
        # Read the result
        result = await result_var.read_value()
        
        print(f"\n✅ [{iteration}/10] Telemetry sent successfully!")
        print(f"Record ID: {result}\n")
        
    except Exception as e:
        print(f"❌ [{iteration}/10] Error: {e}")
        import traceback
        traceback.print_exc()
        raise


async def send_telemetry():
    """
    Send telemetry data packets via OPC UA from snapshot JSON files.
    """
    # Find all snapshot JSON files in results directory
    snapshot_files = sorted(glob.glob(os.path.join(RESULTS_DIR, "snapshot_*.json")))
    
    if not snapshot_files:
        print(f"❌ No snapshot files found in {RESULTS_DIR}")
        print("Expected files like: snapshot_00000002.json, snapshot_00000003.json, etc.")
        return
    
    print(f"📁 Found {len(snapshot_files)} snapshot files in {RESULTS_DIR}")
    print(f"📋 Files: {[os.path.basename(f) for f in snapshot_files]}\n")
    
    # Send telemetry via OPC UA
    url = "opc.tcp://localhost:4840/telemetry/server/"
    
    try:
        # Create OPC UA client
        client = Client(url=url)
        await client.connect()
        print(f"✅ Connected to OPC UA server at {url}\n")
        
        # Send telemetry for each snapshot file
        for i, snapshot_file in enumerate(snapshot_files, 1):
            print(f"\n[{i}/{len(snapshot_files)}] Processing {os.path.basename(snapshot_file)}")
            await send_single_telemetry(client, i, snapshot_file)
            
            # Delay between messages (except after the last one)
            if i < len(snapshot_files):
                await asyncio.sleep(0.5)  # 500ms delay
        
        await client.disconnect()
        print("✅ Disconnected from OPC UA server")
        print(f"\n🎉 Successfully sent all {len(snapshot_files)} telemetry messages!")
        
    except ConnectionRefusedError:
        print("❌ Error: Could not connect to the OPC UA server.")
        print("Make sure the OPC UA server is running on opc.tcp://localhost:4840/telemetry/server/")
        print("Run: python edge-device-db-opcua.py")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(send_telemetry())
