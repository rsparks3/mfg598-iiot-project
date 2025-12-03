import asyncio
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
import json
import requests
from asyncua import Server, ua

load_dotenv()

# Cloud device endpoint
CLOUD_DEVICE_URL = os.getenv('CLOUD_DEVICE_URL', 'http://localhost:8067/telemetry')

# Buffer to store telemetry data points
telemetry_buffer = []
BUFFER_SIZE = 4

def send_to_cloud(machine_id, timestep, simulation_time, num_nodes, temperatures, power_consumption):
    """
    Send averaged telemetry data to cloud device via HTTP POST.
    """
    try:
        payload = {
            "machine_id": machine_id,
            "timestep": timestep,
            "simulation_time": simulation_time,
            "num_nodes": num_nodes,
            "temperatures": temperatures,
            "power_consumption": power_consumption
        }
        
        response = requests.post(CLOUD_DEVICE_URL, json=payload, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Data sent to cloud device successfully")
        print(f"   Record ID: {result.get('record_id')}")
        print(f"   Machine: {machine_id}, Timestep: {timestep}")
        print(f"   Stats - Min: {result['stats']['min']:.2f}, Max: {result['stats']['max']:.2f}, Mean: {result['stats']['mean']:.2f}, Std: {result['stats']['std']:.2f}")
        
        return result.get('record_id')
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error sending data to cloud device: {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return None

def process_telemetry(machine_id, timestep, simulation_time, num_nodes, temperatures, power_consumption):
    """
    Collect telemetry data and average every BUFFER_SIZE points before sending to cloud.
    """
    global telemetry_buffer
    
    try:
        # Parse temperatures if it's a JSON string
        if isinstance(temperatures, str):
            temp_list = json.loads(temperatures)
        else:
            temp_list = temperatures
        
        # Validate that we have exactly 1581 values
        if len(temp_list) != 1581:
            print(f"Invalid array size. Expected 1581 values, got {len(temp_list)}")
            return None
        
        # Convert to numpy array
        temp_array = np.array(temp_list)
        
        # Add to buffer
        telemetry_buffer.append({
            'machine_id': machine_id,
            'timestep': timestep,
            'simulation_time': simulation_time,
            'num_nodes': num_nodes,
            'temperatures': temp_array,
            'power_consumption': power_consumption
        })
        
        print(f"📊 Buffered telemetry point {len(telemetry_buffer)}/{BUFFER_SIZE} from {machine_id}")
        
        # If buffer is full, average and send
        if len(telemetry_buffer) >= BUFFER_SIZE:
            print(f"\n🔄 Averaging {BUFFER_SIZE} telemetry points...")
            
            # Average the temperature arrays
            avg_temperatures = np.mean([item['temperatures'] for item in telemetry_buffer], axis=0)
            
            # Average power consumption
            avg_power = np.mean([item['power_consumption'] for item in telemetry_buffer])
            
            # Use the most recent machine_id, timestep, simulation_time, and num_nodes
            final_machine_id = telemetry_buffer[-1]['machine_id']
            final_timestep = telemetry_buffer[-1]['timestep']
            final_simulation_time = telemetry_buffer[-1]['simulation_time']
            final_num_nodes = telemetry_buffer[-1]['num_nodes']
            
            # Convert averaged temperatures to list
            temp_list = avg_temperatures.tolist()
            
            # Send to cloud
            record_id = send_to_cloud(final_machine_id, final_timestep, final_simulation_time, final_num_nodes, temp_list, avg_power)
            
            # Clear buffer
            telemetry_buffer = []
            
            return record_id
        
        return None
        
    except Exception as e:
        print(f"❌ Error processing telemetry: {str(e)}")
        return None

async def main():
    """
    Main function to set up and run the OPC UA server.
    """
    print(f"🌐 Cloud device endpoint: {CLOUD_DEVICE_URL}")
    print(f"📦 Buffer size: {BUFFER_SIZE} points\n")
    
    # Create OPC UA server
    server = Server()
    await server.init()
    
    server.set_endpoint("opc.tcp://0.0.0.0:4840/telemetry/server/")
    server.set_server_name("Telemetry OPC UA Server")
    
    # Setup namespace
    uri = "http://telemetry.opcua.server"
    idx = await server.register_namespace(uri)
    
    # Create object node for telemetry
    objects = server.nodes.objects
    telemetry_object = await objects.add_object(idx, "TelemetryObject")
    
    # Create variables for telemetry data
    machine_id_var = await telemetry_object.add_variable(idx, "MachineID", "")
    timestep_var = await telemetry_object.add_variable(idx, "Timestep", "")
    simulation_time_var = await telemetry_object.add_variable(idx, "SimulationTime", "")
    num_nodes_var = await telemetry_object.add_variable(idx, "NumNodes", 0)
    temperatures_var = await telemetry_object.add_variable(idx, "Temperatures", "")
    power_consumption_var = await telemetry_object.add_variable(idx, "PowerConsumption", 0.0)
    trigger_var = await telemetry_object.add_variable(idx, "TriggerStorage", False)
    result_var = await telemetry_object.add_variable(idx, "LastRecordID", 0)
    
    # Make variables writable
    await machine_id_var.set_writable()
    await timestep_var.set_writable()
    await simulation_time_var.set_writable()
    await num_nodes_var.set_writable()
    await temperatures_var.set_writable()
    await power_consumption_var.set_writable()
    await trigger_var.set_writable()
    
    # Subscribe to trigger variable changes
    class TriggerHandler:
        async def datachange_notification(self, node, val, data):
            if val:  # When trigger is set to True
                # Read all the values
                machine_id = await machine_id_var.read_value()
                timestep = await timestep_var.read_value()
                simulation_time = await simulation_time_var.read_value()
                num_nodes = await num_nodes_var.read_value()
                temperatures = await temperatures_var.read_value()
                power_consumption = await power_consumption_var.read_value()
                
                print(f"\n📥 Received telemetry data from {machine_id}")
                record_id = process_telemetry(machine_id, timestep, simulation_time, num_nodes, temperatures, power_consumption)
                
                # Write result back
                await result_var.write_value(record_id if record_id else 0)
                
                # Reset trigger
                await trigger_var.write_value(False)
    
    handler = TriggerHandler()
    sub = await server.create_subscription(100, handler)
    await sub.subscribe_data_change(trigger_var)
    
    print("\n🚀 OPC UA Telemetry Server started")
    print("   Endpoint: opc.tcp://0.0.0.0:4840/telemetry/server/")
    print("   Namespace: " + uri)
    print("   Variables: MachineID, Timestep, SimulationTime, NumNodes, Temperatures, PowerConsumption")
    print("   Trigger: TriggerStorage (set to True to store data)")
    print("   Result: LastRecordID (read after trigger)")
    print("\nWaiting for telemetry data...")
    
    async with server:
        while True:
            await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped")
