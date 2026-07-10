import psycopg2
import pandas as pd

# Connect to the cold tier time-series database
try:
    db_conn = psycopg2.connect(
        host="localhost",
        database="telematics_db",
        user="telematics_admin",
        password="telematics_secure_pass",
        port="5432"
    )
    print("📊 Connected to TimescaleDB Analytics Engine.\n")
except Exception as e:
    print(f"❌ Connection error: {e}")
    exit(1)

def run_fleet_health_report():
    print("=== REPORT 1: 1-Minute Fleet Telematics Rollups ===")
    print("Computing rolling averages using TimescaleDB time_bucket()...\n")

    # Query 1: Use time_bucket to group messy high-frequency data into clean 1-minute blocks
    query_1 = """
    SELECT
        time_bucket('1 minute', event_time) AS minute_bucket,
        vehicle_id,
        AVG(engine_rpm) AS avg_rpm,
        AVG(coolant_temp_c) AS avg_temp,
        MAX(vibration_amplitude_g) AS max_vibration,
        COUNT(*) AS packets_received
    FROM vehicle_telemetry_logs
    GROUP BY minute_bucket, vehicle_id
    ORDER BY minute_bucket DESC, max_vibration DESC
    LIMIT 10;
    """

    df_rollups = pd.read_sql_query(query_1, db_conn)
    print(df_rollups.to_string(index=False))
    print("\n" + "="*60 + "\n")


def run_predictive_anomaly_report():
    print("=== REPORT 2: Predictive Maintenance Flag ===")
    print("Identifying assets with sustained high temperature & vibration drift...\n")

    # Query 2: Find vehicles whose average vibration over their history is abnormally high
    # This is the exact kind of query an MLOps pipeline uses to trigger a model retraining sequence
    query_2 = """
    SELECT
        vehicle_id,
        ROUND(AVG(vibration_amplitude_g), 2) as fleet_avg_vibration,
        ROUND(AVG(coolant_temp_c), 2) as fleet_avg_temp,
        COUNT(*) as total_logged_signals
    FROM vehicle_telemetry_logs
    GROUP BY vehicle_id
    HAVING AVG(vibration_amplitude_g) > 1.5
    ORDER BY fleet_avg_vibration DESC;
    """

    df_anomalies = pd.read_sql_query(query_2, db_conn)
    if df_anomalies.empty:
        print("✅ No sustained long-term anomalies detected yet. Fleet operating within nominal specs.")
    else:
        print("⚠️ PREDICTIVE REPAIR REQUIRED FOR THE FOLLOWING ASSETS:")
        print(df_anomalies.to_string(index=False))
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    run_fleet_health_report()
    run_predictive_anomaly_report()
    db_conn.close()