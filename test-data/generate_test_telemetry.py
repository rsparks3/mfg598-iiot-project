import requests
import random
import json
from datetime import datetime

# Generate 10,000 random temperature values (simulating a 100x100 sensor array)
# Temperature range: 20°C to 80°C
temperatures = [round(random.uniform(20.0, 80.0), 2) for _ in range(10000)]

# Convert to comma-separated string
temperatures_str = ','.join(map(str, temperatures))

# Create the request payload
payload = {
    "timestep": datetime.now().isoformat(),
    "temperatures": temperatures_str
}

# Save payload to JSON file
output_filename = f"telemetry_request_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(output_filename, 'w') as f:
    json.dump(payload, f, indent=2)
print(f"✅ Request payload saved to: {output_filename}")
print()

# Send POST request to the telemetry endpoint
url = "http://localhost:8000/telemetry"

print("Sending telemetry data with 10,000 temperature values...")
print(f"Timestep: {payload['timestep']}")
print(f"Temperature sample (first 10): {temperatures[:10]}")
print(f"Total values: {len(temperatures)}")
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
