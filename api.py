import os
import time
import redis
import psycopg2
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Real-Time Telematics API Gateway")

host_db = os.getenv('DB_HOST', 'localhost')
host_redis = os.getenv('REDIS_HOST', 'localhost')

# Connection stabilizer loop for Redis and TimescaleDB only (No Kafka!)
db_conn = None
while True:
    try:
        r = redis.Redis(host=host_redis, port=6379, decode_responses=True)
        r.ping()

        db_conn = psycopg2.connect(
            host=host_db, database="telematics_db",
            user="telematics_admin", password="telematics_secure_pass", port="5432"
        )
        print("🚀 API Gateway initialized database connections cleanly.")
        break
    except Exception as e:
        print(f"⏳ API Gateway waiting for storage layers... Retrying: {e}")
        time.sleep(2)

@app.get("/api/fleet/live/{vehicle_id}")
def get_vehicle_live_status(vehicle_id: str):
    """Fetches sub-second real-time state from the Redis Hot Tier Cache"""
    redis_key = f"vehicle:status:{vehicle_id}"
    data = r.hgetall(redis_key)

    if not data:
        raise HTTPException(status_code=404, detail=f"Vehicle {vehicle_id} not found in active stream mesh.")

    return {
        "vehicle_id": vehicle_id,
        "status": data.get("status"),
        "telemetry": {
            "last_seen": data.get("last_seen"),
            "engine_rpm": int(data.get("rpm", 0)),
            "coolant_temp_c": float(data.get("temp", 0.0)),
            "vibration_amplitude_g": float(data.get("vibration", 0.0)),
            "location": {
                "latitude": float(data.get("lat", 0.0)),
                "longitude": float(data.get("lng", 0.0))
            }
        }
    }

@app.get("/api/fleet/predictive-report")
def get_predictive_maintenance_report():
    """Queries TimescaleDB Cold Tier for long-term structural degradation trends"""
    query = """
    SELECT
        vehicle_id,
        ROUND(AVG(vibration_amplitude_g), 2) as fleet_avg_vibration,
        ROUND(AVG(coolant_temp_c), 2) as fleet_avg_temp,
        COUNT(*) as total_logged_signals
    FROM vehicle_telemetry_logs
    GROUP BY vehicle_id
    HAVING AVG(vibration_amplitude_g) > 0.8
    ORDER BY fleet_avg_vibration DESC;
    """
    try:
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            report = cursor.fetchall()
        return {"timestamp": "live", "anomalous_assets": report}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database aggregation failed: {e}")