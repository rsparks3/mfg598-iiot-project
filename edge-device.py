from flask import Flask, request, jsonify
import numpy as np
from datetime import datetime

app = Flask(__name__)

# Store telemetry data (in-memory storage for now)
telemetry_data = []

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
        
        # Store the data
        telemetry_entry = {
            "machine_id": machine_id,
            "timestep": timestep,
            "temperatures": temp_array.tolist(),  # Convert to list for JSON serialization
            "power_consumption": power_consumption,
            "vibration": vibration,
            "received_at": datetime.now().isoformat(),
            "stats": {
                "min": float(np.min(temp_array)),
                "max": float(np.max(temp_array)),
                "mean": float(np.mean(temp_array)),
                "std": float(np.std(temp_array))
            }
        }
        
        telemetry_data.append(telemetry_entry)
        
        return jsonify({
            "status": "success",
            "message": "Telemetry data received",
            "machine_id": machine_id,
            "timestep": timestep,
            "array_shape": [100, 100],
            "power_consumption": power_consumption,
            "vibration": vibration,
            "total_records": len(telemetry_data),
            "stats": telemetry_entry["stats"]
        }), 201
        
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route('/telemetry', methods=['GET'])
def get_telemetry():
    """
    Endpoint to retrieve all stored telemetry data.
    """
    return jsonify({
        "total_records": len(telemetry_data),
        "data": telemetry_data
    }), 200


@app.route('/telemetry/<int:index>', methods=['GET'])
def get_telemetry_by_index(index):
    """
    Endpoint to retrieve a specific telemetry record by index.
    """
    if 0 <= index < len(telemetry_data):
        return jsonify(telemetry_data[index]), 200
    else:
        return jsonify({"error": "Index out of range"}), 404


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.
    """
    return jsonify({
        "status": "healthy",
        "total_records": len(telemetry_data)
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
