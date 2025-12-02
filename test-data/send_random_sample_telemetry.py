import requests
import random
import json
import os
from datetime import datetime

# Configuration
MACHINE_ID = "MACHINE_001"  # Change this to your machine identifier

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
output_filename = os.path.join(output_dir, f"telemetry_request_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
with open(output_filename, 'w') as f:
    json.dump(payload, f, indent=2)
print(f"✅ Request payload saved to: {output_filename}")
print()

# Send POST request to the telemetry endpoint
url = "http://localhost:8000/telemetry"

print("Sending telemetry data with 10,000 temperature values...")
print(f"Machine ID: {payload['machine_id']}")
print(f"Timestep: {payload['timestep']}")
print(f"Temperature sample (first 10): {temperatures[:10]}")
print(f"Total values: {len(temperatures)}")
print(f"Power Consumption: {payload['power_consumption']} kW")
print(f"Vibration: {payload['vibration']} mm/s")
print()

try:
    response = requests.post(url, json=payload)
    
    print(f"Status Code: {response.status_code}")
    print("Response:")
    print(response.json())
    
except requests.exceptions.ConnectionError:
    print("❌ Error: Could not connect to the server.")
    print("Make sure the Flask app is running on http://localhost:8000")
    print("Run: python app.py")
except Exception as e:
    print(f"❌ Error: {e}")
