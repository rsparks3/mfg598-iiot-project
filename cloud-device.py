from flask import Flask, request, jsonify
import numpy as np
from datetime import datetime
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import json

load_dotenv()

app = Flask(__name__)

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

# Initialize database on startup
init_db()

@app.route('/telemetry', methods=['POST'])
def receive_telemetry():
    """
    Endpoint to receive telemetry data with a 100x100 temperature array.
    
    Expected JSON format:
    {
        "machine_id": "MACHINE_001",
        "timestep": "2025-12-01T10:30:00",  # or integer timestep
        "temperatures": "temp1,temp2,temp3,...",  # comma-separated values (10,000 values)
        "power_consumption": 25.5,  # in kW
        "vibration": 2.3  # in mm/s
    }
    """
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        # Extract fields
        machine_id = data.get('machine_id')
        timestep = data.get('timestep')
        temperatures_str = data.get('temperatures')
        power_consumption = data.get('power_consumption')
        vibration = data.get('vibration')
        
        if machine_id is None:
            return jsonify({"error": "Missing 'machine_id' field"}), 400
        
        if timestep is None:
            return jsonify({"error": "Missing 'timestep' field"}), 400
        
        if temperatures_str is None:
            return jsonify({"error": "Missing 'temperatures' field"}), 400
        
        if power_consumption is None:
            return jsonify({"error": "Missing 'power_consumption' field"}), 400
        
        if vibration is None:
            return jsonify({"error": "Missing 'vibration' field"}), 400
        
        # Parse comma-separated temperature values
        try:
            temperatures = [float(temp.strip()) for temp in temperatures_str.split(',')]
        except ValueError:
            return jsonify({"error": "Invalid temperature values. Must be numeric."}), 400
        
        # Validate that we have exactly 10,000 values (100x100)
        if len(temperatures) != 10000:
            return jsonify({
                "error": f"Invalid array size. Expected 10,000 values (100x100), got {len(temperatures)}"
            }), 400
        
        # Convert to numpy array and reshape to 100x100
        temp_array = np.array(temperatures).reshape(100, 100)
        
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
        
        return jsonify({
            "status": "success",
            "message": "Telemetry data received and stored",
            "record_id": record_id,
            "machine_id": machine_id,
            "timestep": timestep,
            "array_shape": [100, 100],
            "power_consumption": power_consumption,
            "vibration": vibration,
            "stats": stats
        }), 201
        
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route('/machines', methods=['GET'])
def get_machines():
    """
    Endpoint to retrieve all unique machine IDs.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT DISTINCT machine_id FROM telemetry ORDER BY machine_id")
        machines = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            "machines": machines,
            "total": len(machines)
        }), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve machines: {str(e)}"}), 500


@app.route('/telemetry', methods=['GET'])
def get_telemetry():
    """
    Endpoint to retrieve all stored telemetry data, optionally filtered by machine_id.
    Query parameters:
        - machine_id: Filter by specific machine (optional)
    """
    try:
        machine_id = request.args.get('machine_id')
        
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
                ORDER BY received_at DESC
            """)
        
        records = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert records to list of dicts
        data = []
        for record in records:
            data.append({
                "id": record["id"],
                "machine_id": record["machine_id"],
                "timestep": record["timestep"],
                "temperatures": record["temperatures"],
                "power_consumption": record["power_consumption"],
                "vibration": record["vibration"],
                "received_at": record["received_at"].isoformat(),
                "stats": {
                    "min": record["min_temp"],
                    "max": record["max_temp"],
                    "mean": record["mean_temp"],
                    "std": record["std_temp"]
                }
            })
        
        return jsonify({
            "total_records": len(data),
            "machine_id": machine_id,
            "data": data
        }), 200
    except Exception as e:
        return jsonify({"error": f"Failed to retrieve telemetry: {str(e)}"}), 500


@app.route('/telemetry/<int:index>', methods=['GET'])
def get_telemetry_by_index(index):
    """
    Endpoint to retrieve a specific telemetry record by ID.
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT id, machine_id, timestep, temperatures, power_consumption, 
               vibration, received_at, min_temp, max_temp, mean_temp, std_temp
        FROM telemetry
        WHERE id = %s
    """, (index,))
    
    record = cur.fetchone()
    cur.close()
    conn.close()
    
    if record:
        data = {
            "id": record["id"],
            "machine_id": record["machine_id"],
            "timestep": record["timestep"],
            "temperatures": record["temperatures"],
            "power_consumption": record["power_consumption"],
            "vibration": record["vibration"],
            "received_at": record["received_at"].isoformat(),
            "stats": {
                "min": record["min_temp"],
                "max": record["max_temp"],
                "mean": record["mean_temp"],
                "std": record["std_temp"]
            }
        }
        return jsonify(data), 200
    else:
        return jsonify({"error": "Record not found"}), 404


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM telemetry")
        total_records = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "total_records": total_records
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 503


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8067, debug=True)
