import json
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'urbanpulse.bus_gps',
    'urbanpulse.traffic_signals',
    'urbanpulse.air_quality',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuration for SLA targets
AQI_CRITICAL_THRESHOLD = 300
WAIT_TIME_CRITICAL_SEC = 90

print("UrbanPulse SLA Processor Active...")

for message in consumer:
    data = message.value
    topic = message.topic
    
    # ---------------------------------------------------------
    # SLA TARGET 3: Sub-2m AQI Alerting
    # ---------------------------------------------------------
    if topic == 'urbanpulse.air_quality':
        if data['aqi'] > AQI_CRITICAL_THRESHOLD:
            alert = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "SEVERE_SMOG_ALERT",
                "zone": data['zone'],
                "sensor_id": data['sensor_id'],
                "aqi_reading": data['aqi'],
                "pollutants": {"pm25": data['pm25'], "pm10": data['pm10']}
            }
            producer.send('urbanpulse.alerts.incidents', value=alert)
            print(f"🚨 [AQI ALERT] Zone {data['zone']} AQI hit {data['aqi']}!")

    # ---------------------------------------------------------
    # SLA TARGET 1: Sub-90s Adaptive Signals
    # ---------------------------------------------------------
    elif topic == 'urbanpulse.traffic_signals':
        # Direct congestion detection using intersection wait times
        if data['avg_wait_sec'] > WAIT_TIME_CRITICAL_SEC and data['signal_phase'] == 'RED':
            action = {
                "timestamp": datetime.utcnow().isoformat(),
                "junction_id": data['junction_id'],
                "zone": data['zone'],
                "action": "FORCE_GREEN",
                "reason": f"Wait time {data['avg_wait_sec']}s exceeded SLA"
            }
            producer.send('urbanpulse.traffic_signals.control', value=action)
            print(f"🚦 [SIGNAL OVERRIDE] Junction {data['junction_id']} forced GREEN.")

    # ---------------------------------------------------------
    # SLA TARGET 2: Sub-60s Bus ETA Updates
    # ---------------------------------------------------------
    elif topic == 'urbanpulse.bus_gps':
        # Route to frontend WebSocket caches immediately
        # Payload includes occupancy to notify passengers of crowded buses
        update_payload = {
            "bus_id": data['bus_id'],
            "route_id": data['route_id'],
            "lat": data['lat'],
            "lon": data['lon'],
            "status": "MOVING" if data['speed_kmh'] > 0 else "IDLE",
            "occupancy": f"{data['occupancy_pct']}%"
        }
        producer.send('urbanpulse.dashboard.bus-locations', value=update_payload)
