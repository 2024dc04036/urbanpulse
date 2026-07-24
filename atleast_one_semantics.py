import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Shared cluster configuration
BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9094', 'localhost:9096']

# --------------------------------------------------------------------------------
# 1. AIR QUALITY PRODUCER (Strict At-Least-Once Semantics)
# --------------------------------------------------------------------------------
aqi_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',               # Wait for full replication confirmation
    retries=5,                # Robust automatic cluster retry loop
    max_in_flight_requests_per_connection=1 # Prevent out-of-order delivery during retry
)

def run_air_quality_producer():
    print("🚀 Initializing Air Quality Producer [At-Least-Once]...")
    sensor_ids = [f"AQI-SENSOR-{i}" for i in range(1, 101)]
    zones = ["ZONE-A", "ZONE-B", "ZONE-C", "ZONE-D"]
    
    while True:
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
            # Inline Validation: Log anomalies locally before offloading down stream
            if payload["aqi"] is None:
                print(f"⚠️ [PRODUCER FAULT DETECTION] Local validation caught null AQI from {sensor_id}. Emitting to stream for DLQ evaluation.")
            
            future = aqi_producer.send('urbanpulse.air_quality', key=sensor_id, value=payload)
            record_metadata = future.get(timeout=10) # Synchronous block for strict assurance
            
        except KafkaError as e:
            print(f"❌ Permanent write exception encountered for {sensor_id}: {e}")
            
        time.sleep(0.5) # Emulation pacing

# --------------------------------------------------------------------------------
# 2. BUS GPS PRODUCER (Route-ID Keyed Ordering)
# --------------------------------------------------------------------------------
bus_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks=1, # Performance optimized for spatial streams
    linger_ms=20,
    compression_type='gzip'
)

def run_bus_gps_producer():
    print("🚀 Initializing Bus GPS Producer [Keyed By Route ID]...")
    bus_ids = [f"BEST-BUS-{i}" for i in range(1000, 1200)]
    route_ids = [f"ROUTE-101", f"ROUTE-202", f"ROUTE-303", f"ROUTE-404"]
    
    while True:
        bus_id = random.choice(bus_ids)
        route_id = random.choice(route_ids) # Key constraint
        
        payload = {
            "bus_id": bus_id,
            "route_id": route_id,
            "lat": round(18.5204 + random.uniform(-0.08, 0.08), 6),
            "lon": round(73.8567 + random.uniform(-0.08, 0.08), 6),
            "speed_kmh": random.randint(0, 55),
            "occupancy_pct": random.randint(5, 95),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Partition assignment is dictated explicitly by route_id to preserve spatial order
        bus_producer.send('urbanpulse.bus_gps', key=route_id, value=payload)
        time.sleep(0.05)
