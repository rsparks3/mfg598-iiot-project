import asyncio
import random
import json
import os
from datetime import datetime
from asyncua import Client

# Configuration
MACHINE_ID = "MACHINE_001"  # Change this to your machine identifier

async def send_single_telemetry(client, iteration):
    """
    Send a single telemetry data packet via OPC UA.
    """
    # Generate 10,000 random temperature values (simulating a 100x100 sensor array)
    # Temperature range: 20°C to 80°C
    temperatures = [round(random.uniform(20.0, 80.0), 2) for _ in range(10000)]
    
    # Convert to comma-separated string
    temperatures_str = ','.join(map(str, temperatures))
    
    # Generate random power consumption (in kW) and vibration (in mm/s)
    power_consumption = round(random.uniform(10.0, 50.0), 2)
    vibration = round(random.uniform(0.5, 5.0), 2)
    
    # Create the request payload
    payload = {
        "machine_id": MACHINE_ID,
        "timestep": datetime.now().isoformat(),
        "temperatures": temperatures_str,
        "power_consumption": power_consumption,
        "vibration": vibration
    }
    
    # Create output directory if it doesn't exist
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "telemetry-requests")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save payload to JSON file
    output_filename = os.path.join(output_dir, f"telemetry_request_opcua_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(output_filename, 'w') as f:
        json.dump(payload, f, indent=2)
    print(f"✅ [{iteration}/10] Request payload saved to: {output_filename}")
    print()
    
    print(f"[{iteration}/10] Sending telemetry data with 10,000 temperature values via OPC UA...")
    print(f"Machine ID: {payload['machine_id']}")
    print(f"Timestep: {payload['timestep']}")
    print(f"Temperature sample (first 10): {temperatures[:10]}")
    print(f"Total values: {len(temperatures)}")
    print(f"Power Consumption: {payload['power_consumption']} kW")
    print(f"Vibration: {payload['vibration']} mm/s")
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
        temperatures_var = await telemetry_obj.get_child(["2:Temperatures"])
        power_var = await telemetry_obj.get_child(["2:PowerConsumption"])
        vibration_var = await telemetry_obj.get_child(["2:Vibration"])
        trigger_var = await telemetry_obj.get_child(["2:TriggerStorage"])
        result_var = await telemetry_obj.get_child(["2:LastRecordID"])
        
        # Write telemetry data to variables
        print("📤 Writing telemetry data to OPC UA variables...")
        await machine_id_var.write_value(payload['machine_id'])
        await timestep_var.write_value(payload['timestep'])
        await temperatures_var.write_value(payload['temperatures'])
        await power_var.write_value(payload['power_consumption'])
        await vibration_var.write_value(payload['vibration'])
        
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
    Send 10 telemetry data packets via OPC UA with a 200ms delay between them.
    """
    # Create output directory if it doesn't exist
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "telemetry-requests")
    os.makedirs(output_dir, exist_ok=True)
    
    # Send telemetry via OPC UA
    url = "opc.tcp://localhost:4840/telemetry/server/"
    
    try:
        # Create OPC UA client
        client = Client(url=url)
        await client.connect()
        print(f"✅ Connected to OPC UA server at {url}\n")
        
        # Send 10 telemetry messages
        for i in range(1, 11):
            await send_single_telemetry(client, i)
            
            # Delay between messages (except after the last one)
            if i < 10:
                await asyncio.sleep(0.2)  # 200ms delay
        
        await client.disconnect()
        print("✅ Disconnected from OPC UA server")
        print(f"\n🎉 Successfully sent all 10 telemetry messages!")
        
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
