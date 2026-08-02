import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
 
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    linger_ms=5, # Batching for high throughput
    batch_size=32768, # 32KB batch size for better batching efficiency
    buffer_memory=67108864, # 64MB buffer to absorb bursts
    compression_type='lz4' # Essential for 4000+ msgs/sec
)
 
# Target throughput: ~4000 msgs/sec
TARGET_MSGS_PER_SEC = 4000
BATCH_SIZE = 100  # Send in batches then sleep to approximate target rate
SLEEP_INTERVAL = BATCH_SIZE / TARGET_MSGS_PER_SEC
 
def get_timestamp():
    return datetime.now(timezone.utc).isoformat()
 
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
        "kwh_reading": round(random.uniform(0.5, 8.0), 2),
        "voltage": round(random.uniform(210, 240), 1),
        "power_factor": round(random.uniform(0.85, 0.99), 2),
        "timestamp": get_timestamp()
    }
    producer.send('urbanpulse.smart_meters', key=data['meter_id'], value=data)
 
print("Starting UrbanPulse High-Throughput Emitter...")
print(f"Target rate: ~{TARGET_MSGS_PER_SEC} msgs/sec")
 
try:
    total_sent = 0
    start_time = time.time()
    last_report_time = start_time
 
    while True:
        # Send a batch of messages
        for _ in range(BATCH_SIZE):
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
            total_sent += 1
 
        # Throttle to approximate target throughput
        time.sleep(SLEEP_INTERVAL)
 
        # Report throughput every 5 seconds
        now = time.time()
        if now - last_report_time >= 5.0:
            elapsed = now - start_time
            rate = total_sent / elapsed
            print(f"  [{elapsed:.0f}s] Sent {total_sent:,} messages | Current rate: {rate:,.0f} msgs/sec")
            last_report_time = now
 
except KeyboardInterrupt:
    print("\nShutting down emitter...")
finally:
    producer.flush(timeout=10)
    producer.close()
    elapsed = time.time() - start_time
    if elapsed > 0:
        print(f"Total sent: {total_sent:,} messages in {elapsed:.1f}s ({total_sent/elapsed:,.0f} msgs/sec avg)")
 
