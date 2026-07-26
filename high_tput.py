import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    linger_ms=5, # Batching for high throughput
    compression_type='lz4' # Essential for 4000+ msgs/sec
)

def get_timestamp():
    return datetime.utcnow().isoformat()

def emit_bus_gps():
    data = {
        "bus_id": f"BUS-{random.randint(1000, 13000)}",
        "route_id": f"RTE-{random.randint(1, 150)}",
        "lat": round(18.5204 + random.uniform(-0.1, 0.1), 6),
        "lon": round(73.8567 + random.uniform(-0.1, 0.1), 6),
        "speed_kmh": random.randint(0, 60),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": get_timestamp()
    }
    producer.send('urbanpulse.bus_gps', key=data['bus_id'], value=data)

def emit_traffic_signal():
    data = {
        "junction_id": f"JCT-{random.randint(1, 3800)}",
        "zone": f"ZONE-{random.randint(1, 20)}",
        "vehicle_count": random.randint(5, 150),
        "avg_wait_sec": random.randint(10, 120),
        "signal_phase": random.choice(["RED", "GREEN", "YELLOW"]),
        "timestamp": get_timestamp()
    }
    producer.send('urbanpulse.traffic_signals', key=data['junction_id'], value=data)

def emit_air_quality():
    data = {
        "sensor_id": f"AQI-{random.randint(1, 600)}",
        "zone": f"ZONE-{random.randint(1, 20)}",
        "pm25": round(random.uniform(10.0, 200.0), 2),
        "pm10": round(random.uniform(20.0, 300.0), 2),
        "no2": round(random.uniform(5.0, 80.0), 2),
        "aqi": random.randint(30, 400),
        "timestamp": get_timestamp()
    }
    producer.send('urbanpulse.air_quality', key=data['sensor_id'], value=data)

def emit_smart_meter():
    data = {
        "meter_id": f"MTR-{random.randint(1, 1100000)}",
        "ward_id": f"WARD-{random.randint(1, 50)}",
        "kwh_reading": round(random.uniform(1000, 5000), 2),
        "voltage": round(random.uniform(210, 240), 1),
        "power_factor": round(random.uniform(0.85, 0.99), 2),
        "timestamp": get_timestamp()
    }
    producer.send('urbanpulse.smart_meters', key=data['meter_id'], value=data)

print("Starting UrbanPulse High-Throughput Emitter...")

try:
    while True:
        # Proportionate distribution based on provided rates
        # Bus: ~60%, Meter: ~28%, Traffic: ~10%, AQI: ~2%
        roll = random.random()
        if roll < 0.60:
            emit_bus_gps()
        elif roll < 0.88:
            emit_smart_meter()
        elif roll < 0.98:
            emit_traffic_signal()
        else:
            emit_air_quality()
            
except KeyboardInterrupt:
    print("Shutting down emitter...")
finally:
    producer.close()
