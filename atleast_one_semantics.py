import json
import random
import signal
import sys
import time
import threading
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
 
# Shared cluster configuration
BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9094', 'localhost:9096']
 
# Graceful shutdown flag
shutdown_event = threading.Event()
 
 
def create_air_quality_producer():
    """Create producer with strict at-least-once (idempotent) semantics."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',                              # Wait for full replication confirmation
        retries=2147483647,                      # Effectively infinite retries
        enable_idempotence=True,                 # Prevent duplicates on retry
        max_in_flight_requests_per_connection=1  # Prevent out-of-order delivery during retry
    )
 
 
def create_bus_gps_producer():
    """Create producer optimized for high-throughput spatial streams."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        acks=1,                  # Leader-only ack for lower latency
        linger_ms=20,            # Batch for 20ms before sending
        compression_type='gzip'  # Compress batches to reduce network usage
    )
 
 
# --------------------------------------------------------------------------------
# 1. AIR QUALITY PRODUCER (Strict At-Least-Once / Idempotent Semantics)
# --------------------------------------------------------------------------------
def run_air_quality_producer(producer):
    print("🚀 Initializing Air Quality Producer [At-Least-Once / Idempotent]...")
    sensor_ids = [f"AQI-SENSOR-{i}" for i in range(1, 101)]
    zones = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D"]
 
    try:
        while not shutdown_event.is_set():
            sensor_id = random.choice(sensor_ids)
            zone = random.choice(zones)
 
            # Simulated Network Fault: 5% of entries generate null values
            is_faulty = random.random() < 0.05
            aqi_value = None if is_faulty else random.randint(30, 450)
 
            payload = {
                "sensor_id": sensor_id,
                "zone": zone,
                "pm25": None if is_faulty else round(random.uniform(5.0, 180.0), 2),
                "pm10": None if is_faulty else round(random.uniform(15.0, 280.0), 2),
                "no2": None if is_faulty else round(random.uniform(2.0, 65.0), 2),
                "aqi": aqi_value,
                "timestamp": datetime.utcnow().isoformat()
            }
 
            try:
                # Log anomalies locally before offloading downstream
                if payload["aqi"] is None:
                    print(f"⚠️ [PRODUCER FAULT DETECTION] Null AQI from {sensor_id}. Emitting for DLQ evaluation.")
 
                future = producer.send('urbanpulse.air_quality', key=sensor_id, value=payload)
                record_metadata = future.get(timeout=10)  # Synchronous block for strict assurance
            except KafkaError as e:
                print(f"❌ Permanent write exception for {sensor_id}: {e}")
 
            time.sleep(0.5)
    finally:
        producer.flush()
        producer.close()
        print("✅ Air Quality Producer shut down cleanly.")
 
 
# --------------------------------------------------------------------------------
# 2. BUS GPS PRODUCER (Route-ID Keyed Ordering)
# --------------------------------------------------------------------------------
def run_bus_gps_producer(producer):
    print("🚀 Initializing Bus GPS Producer [Keyed By Route ID]...")
    bus_ids = [f"BEST-BUS-{i}" for i in range(1000, 1200)]
    route_ids = ["ROUTE-101", "ROUTE-202", "ROUTE-303", "ROUTE-404"]
 
    try:
        while not shutdown_event.is_set():
            bus_id = random.choice(bus_ids)
            route_id = random.choice(route_ids)
 
            payload = {
                "bus_id": bus_id,
                "route_id": route_id,
                "lat": round(18.5204 + random.uniform(-0.08, 0.08), 6),
                "lon": round(73.8567 + random.uniform(-0.08, 0.08), 6),
                "speed_kmh": random.randint(0, 55),
                "occupancy_pct": random.randint(5, 95),
                "timestamp": datetime.utcnow().isoformat()
            }
 
            # Partition assignment dictated by route_id to preserve spatial order
            producer.send('urbanpulse.bus_gps', key=route_id, value=payload)
            time.sleep(0.05)
    finally:
        producer.flush()
        producer.close()
        print("✅ Bus GPS Producer shut down cleanly.")
 
 
# --------------------------------------------------------------------------------
# MAIN ENTRY POINT
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    # Handle Ctrl+C and SIGTERM gracefully
    def handle_shutdown(signum, frame):
        print("\n🛑 Shutdown signal received. Stopping producers...")
        shutdown_event.set()
 
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
 
    # Create producers only when actually running
    aqi_producer = create_air_quality_producer()
    bus_producer = create_bus_gps_producer()
 
    # Launch both producers in daemon threads
    t1 = threading.Thread(target=run_air_quality_producer, args=(aqi_producer,), daemon=True)
    t2 = threading.Thread(target=run_bus_gps_producer, args=(bus_producer,), daemon=True)
 
    t1.start()
    t2.start()
 
    # Wait for shutdown signal
    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown_event.set()
 
    # Wait for threads to finish cleanup
    t1.join(timeout=10)
    t2.join(timeout=10)
 
    print("🏁 All producers stopped. Exiting.")
    sys.exit(0)
 
