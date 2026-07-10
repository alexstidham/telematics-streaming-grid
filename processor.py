import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import redis
import psycopg2

# Pick up internal Docker paths or fallback to localhost
host_db = os.getenv('DB_HOST', 'localhost')
host_redis = os.getenv('REDIS_HOST', 'localhost')
host_kafka = os.getenv('KAFKA_BROKER', 'localhost:9092')

print(f"🔄 Initializing Stream Processor Connectors... (DB: {host_db}, Redis: {host_redis}, Kafka: {host_kafka})")

# 1. RESILIENT REDIS CONNECTION LOOP
while True:
    try:
        r = redis.Redis(host=host_redis, port=6379, decode_responses=True)
        r.ping()
        print("✅ Connected to Redis Hot Tier Cache.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Redis to wake up... ({e})")
        time.sleep(2)

# 2. RESILIENT TIMESCALEDB CONNECTION & SCHEMA INITIALIZATION LOOP
db_conn = None
while True:
    try:
        db_conn = psycopg2.connect(
            host=host_db, database="telematics_db",
            user="telematics_admin", password="telematics_secure_pass", port="5432"
        )
        db_cursor = db_conn.cursor()

        # Enforce structural hypertable tables are ready
        db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_telemetry_logs (
            packet_id UUID NOT NULL,
            vehicle_id VARCHAR(50) NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            ingest_time TIMESTAMPTZ NOT NULL,
            engine_rpm INT,
            coolant_temp_c NUMERIC,
            vibration_amplitude_g NUMERIC,
            latitude NUMERIC,
            longitude NUMERIC
        );
        """)
        try:
            db_cursor.execute("SELECT create_hypertable('vehicle_telemetry_logs', 'event_time', if_not_exists => TRUE);")
        except Exception:
            pass  # Table is already converted to hypertable
        db_conn.commit()
        print("✅ Connected to TimescaleDB and verified Schema Tables.")
        break
    except Exception as e:
        print(f"⏳ TimescaleDB is still initializing. Retrying connection in 2 seconds... ({e})")
        time.sleep(2)

# 3. RESILIENT KAFKA CONNECTION LOOP
while True:
    try:
        consumer = KafkaConsumer(
            'vehicle-telemetry',
            bootstrap_servers=[host_kafka],
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print("📡 Connected to Kafka Cluster Bus.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Kafka Broker to become available... ({e})")
        time.sleep(2)

vibration_windows = {}
print("⚡ Stream Engine Online with Real-Time Analytics Alerting...")

try:
    for message in consumer:
        packet = message.value
        vehicle_id = packet['vehicle_id']
        current_vibration = packet['telemetry']['vibration_amplitude_g']

        packet['timestamps']['ingest_time'] = datetime.now(timezone.utc).isoformat()

        # Stateful moving variance window (CEP)
        if vehicle_id not in vibration_windows:
            vibration_windows[vehicle_id] = []

        vibration_windows[vehicle_id].append(current_vibration)
        if len(vibration_windows[vehicle_id]) > 5:
            vibration_windows[vehicle_id].pop(0)

        moving_avg_vibration = sum(vibration_windows[vehicle_id]) / len(vibration_windows[vehicle_id])

        # State decision mapping
        if moving_avg_vibration > 1.2 and len(vibration_windows[vehicle_id]) >= 5:
            r.hset(f"vehicle:status:{vehicle_id}", "status", "CRITICAL_MAINTENANCE_REQUIRED")
        else:
            r.hset(f"vehicle:status:{vehicle_id}", "status", "HEALTHY")

        # Live Cache Tier state dump
        r.hset(f"vehicle:status:{vehicle_id}", mapping={
            "last_seen": packet['timestamps']['ingest_time'],
            "rpm": packet['telemetry']['engine_rpm'],
            "temp": packet['telemetry']['coolant_temp_c'],
            "vibration": current_vibration,
            "lat": packet['telemetry']['gps']['latitude'],
            "lng": packet['telemetry']['gps']['longitude']
        })

        # Hard Cold Tier structural archiving
        insert_query = """
        INSERT INTO vehicle_telemetry_logs (
            packet_id, vehicle_id, event_time, ingest_time,
            engine_rpm, coolant_temp_c, vibration_amplitude_g, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        db_cursor.execute(insert_query, (
            packet['packet_id'], packet['vehicle_id'],
            packet['timestamps']['event_time'], packet['timestamps']['ingest_time'],
            packet['telemetry']['engine_rpm'], packet['telemetry']['coolant_temp_c'],
            current_vibration, packet['telemetry']['gps']['latitude'], packet['telemetry']['gps']['longitude']
        ))
        db_conn.commit()
        print(f"⚙️ Processed & Stored Packet for {vehicle_id}")

except KeyboardInterrupt:
    print("\nShutting down safely.")
    db_cursor.close()
    db_conn.close()