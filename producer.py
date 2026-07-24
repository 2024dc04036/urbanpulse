import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def generate_bus_telemetry():
    bus_id = f"BUS-{random.randint(1000, 12000)}"
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "bus_id": bus_id,
        "lat": round(18.5204 + random.uniform(-0.1, 0.1), 6), # Approx Pune coordinates for realism
        "lon": round(73.8567 + random.uniform(-0.1, 0.1), 6),
        "speed_kmh": random.randint(0, 60),
        "status": random.choice(["ON_ROUTE", "ON_ROUTE", "DELAYED", "STOPPED"])
    }

def generate_aqi_telemetry():
    sensor_id = f"AQI-{random.randint(1, 600)}"
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "sensor_id": sensor_id,
        "pm25": random.uniform(10.0, 150.0), # Micrograms per cubic meter
        "status": "ACTIVE"
    }

print("Starting UrbanPulse Telemetry Emitter...")

try:
    while True:
        # Emit Bus Data (High Frequency)
        for _ in range(5): 
            bus_data = generate_bus_telemetry()
            producer.send(
                'urbanpulse.transit.bus-gps', 
                key=bus_data['bus_id'], 
                value=bus_data
            )
        
        # Emit AQI Data (Lower Frequency)
        if random.random() > 0.7:
            aqi_data = generate_aqi_telemetry()
            producer.send(
                'urbanpulse.env.aqi', 
                key=aqi_data['sensor_id'], 
                value=aqi_data
            )
            
        producer.flush()
        print(f"Emitted batch at {datetime.utcnow().strftime('%H:%M:%S')}")
        time.sleep(1) # Simulate real-time streaming intervals

except KeyboardInterrupt:
    print("Telemetry Emitter Stopped.")
finally:
    producer.close()
