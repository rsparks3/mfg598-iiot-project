import asyncio
import numpy as np
from datetime import datetime
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import json
from asyncua import Server, ua

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

def init_db():
    """Initialize the database schema."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create telemetry table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telemetry (
            id SERIAL PRIMARY KEY,
            machine_id VARCHAR(100) NOT NULL,
            timestep VARCHAR(100) NOT NULL,
            temperatures JSONB NOT NULL,
            power_consumption FLOAT NOT NULL,
            vibration FLOAT NOT NULL,
            received_at TIMESTAMP NOT NULL,
            min_temp FLOAT,
            max_temp FLOAT,
            mean_temp FLOAT,
            std_temp FLOAT
        )
    """)
    
    # Create index on machine_id and timestep for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_machine_timestep 
        ON telemetry(machine_id, timestep)
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def store_telemetry(machine_id, timestep, temperatures, power_consumption, vibration):
    """
    Store telemetry data in PostgreSQL database.
    """
    try:
        # Parse comma-separated temperature values
        temp_list = [float(temp.strip()) for temp in temperatures.split(',')]
        
        # Validate that we have exactly 10,000 values (100x100)
        if len(temp_list) != 10000:
            print(f"Invalid array size. Expected 10,000 values (100x100), got {len(temp_list)}")
            return None
        
        # Convert to numpy array and reshape to 100x100
        temp_array = np.array(temp_list).reshape(100, 100)
        
        # Calculate statistics
        stats = {
            "min": float(np.min(temp_array)),
            "max": float(np.max(temp_array)),
            "mean": float(np.mean(temp_array)),
            "std": float(np.std(temp_array))
        }
        
        # Store the data in PostgreSQL
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO telemetry 
            (machine_id, timestep, temperatures, power_consumption, vibration, 
             received_at, min_temp, max_temp, mean_temp, std_temp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            machine_id,
            str(timestep),
            json.dumps(temp_array.tolist()),
            power_consumption,
            vibration,
            datetime.now(),
            stats["min"],
            stats["max"],
            stats["mean"],
            stats["std"]
        ))
        
        record_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Telemetry data stored with ID: {record_id}")
        print(f"   Machine: {machine_id}, Timestep: {timestep}")
        print(f"   Stats - Min: {stats['min']:.2f}, Max: {stats['max']:.2f}, Mean: {stats['mean']:.2f}, Std: {stats['std']:.2f}")
        
        return record_id
        
    except Exception as e:
        print(f"❌ Error storing telemetry: {str(e)}")
        return None

async def main():
    """
    Main function to set up and run the OPC UA server.
    """
    # Initialize database
    init_db()
    print("✅ Database initialized")
    
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
    temperatures_var = await telemetry_object.add_variable(idx, "Temperatures", "")
    power_consumption_var = await telemetry_object.add_variable(idx, "PowerConsumption", 0.0)
    vibration_var = await telemetry_object.add_variable(idx, "Vibration", 0.0)
    trigger_var = await telemetry_object.add_variable(idx, "TriggerStorage", False)
    result_var = await telemetry_object.add_variable(idx, "LastRecordID", 0)
    
    # Make variables writable
    await machine_id_var.set_writable()
    await timestep_var.set_writable()
    await temperatures_var.set_writable()
    await power_consumption_var.set_writable()
    await vibration_var.set_writable()
    await trigger_var.set_writable()
    
    # Subscribe to trigger variable changes
    class TriggerHandler:
        async def datachange_notification(self, node, val, data):
            if val:  # When trigger is set to True
                # Read all the values
                machine_id = await machine_id_var.read_value()
                timestep = await timestep_var.read_value()
                temperatures = await temperatures_var.read_value()
                power_consumption = await power_consumption_var.read_value()
                vibration = await vibration_var.read_value()
                
                print(f"\n📥 Received telemetry data from {machine_id}")
                record_id = store_telemetry(machine_id, timestep, temperatures, power_consumption, vibration)
                
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
    print("   Variables: MachineID, Timestep, Temperatures, PowerConsumption, Vibration")
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
