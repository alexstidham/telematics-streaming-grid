import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Connect to our local Kafka broker running in Docker
# Force a strict API version mapping to skip the auto-detect probe error
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    api_version=(7, 5, 2),  # Force a specific API version to avoid auto-detect issues
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'vehicle-telemetry'
VEHICLE_IDS = [f"TRUCK-{i:03d}" for i in range(1, 51)]

print("Starting telemetry simulator. Press Ctrl+C to stop...")

try:
    while True:
        # Pick a random vehicle to transmit data
        vehicle_id = random.choice(VEHICLE_IDS)

        # Simulate normal running metrics with occasional anomalies
        is_faulty = random.random() < 0.03  # 3% chance a truck starts failing

        base_rpm = 3500 if is_faulty else 2200
        base_vibration = random.uniform(2.5, 4.5) if is_faulty else random.uniform(0.5, 1.2)

        packet = {
            "packet_id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "firmware_version": "v2.4.1",
            "timestamps": {
                "event_time": datetime.now(timezone.utc).isoformat(),
                "ingest_time": None # Will be filled by the backend server
            },
            "telemetry": {
                "engine_rpm": int(random.uniform(base_rpm - 200, base_rpm + 200)),
                "coolant_temp_c": round(random.uniform(85.0, 105.0 if is_faulty else 95.0), 2),
                "vibration_amplitude_g": round(base_vibration, 2),
                "gps": {
                    "latitude": round(random.uniform(42.25, 42.35), 6),  # Simulating driving around Ann Arbor area
                    "longitude": round(random.uniform(-83.79, -83.69), 6)
                }
            }
        }

        # Send the payload to Kafka
        producer.send(TOPIC, value=packet)
        print(f"📡 Sent packet for {vehicle_id} | Vib: {packet['telemetry']['vibration_amplitude_g']}g")

        # Stream high-velocity data (100ms pause between total fleet transmissions)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nStopping simulator safely.")
    producer.close()