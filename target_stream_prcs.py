import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer


# Initialize Kafka Consumer and Producer
consumer = KafkaConsumer(
    'urbanpulse.transit.bus-gps',
    'urbanpulse.env.aqi',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# State stores for windowing and aggregations
zone_speeds = defaultdict(list)
window_start_time = datetime.utcnow()
AQI_THRESHOLD = 150.0

print("UrbanPulse SLA Processor Running...")

for message in consumer:
    data = message.value
    topic = message.topic
    current_time = datetime.utcnow()

    # ---------------------------------------------------------
    # TARGET 3: AQI Breach Alert (<2 minute SLA)
    # Action: Immediate stateless evaluation
    # ---------------------------------------------------------
    if topic == 'urbanpulse.env.aqi':
        if data['pm25'] > AQI_THRESHOLD:
            alert = {
                "timestamp": current_time.isoformat(),
                "type": "AQI_EMERGENCY",
                "sensor_id": data['sensor_id'],
                "reading": data['pm25'],
                "action": "Dispatch Health Advisory"
            }
            producer.send('urbanpulse.alerts.incidents', value=alert)
            print(f"🚨 [ALERT FIRED] AQI Breach at {data['sensor_id']}! Advisory dispatched.")

    # ---------------------------------------------------------
    # TARGET 2: Real-time ETA (<60 second refresh)
    # Action: Update state store immediately upon receipt
    # ---------------------------------------------------------
    elif topic == 'urbanpulse.transit.bus-gps':
        # In production, this pushes to a Redis cache queried by the app
        # thereby bringing the 8-12 minute lag down to sub-second.
        update_payload = {
            "bus_id": data['bus_id'],
            "lat": data['lat'],
            "lon": data['lon'],
            "speed": data['speed_kmh'],
            "updated_at": data['timestamp']
        }
        producer.send('urbanpulse.dashboard.bus-locations', value=update_payload)
        
        # ---------------------------------------------------------
        # TARGET 1: Adaptive Signal Control (<90 second SLA)
        # Action: 60-second Tumbling Window to detect congestion
        # ---------------------------------------------------------
        # Group buses by a mock "zone" based on coordinates
        zone_id = f"ZONE-{round(data['lat'], 2)}-{round(data['lon'], 2)}"
        zone_speeds[zone_id].append(data['speed_kmh'])

        # Evaluate the window every 60 seconds
        if (current_time - window_start_time).total_seconds() >= 60:
            for zone, speeds in zone_speeds.items():
                avg_speed = sum(speeds) / len(speeds)
                
                # If average speed drops, trigger adaptive signals
                if avg_speed < 10.0:
                    signal_override = {
                        "timestamp": current_time.isoformat(),
                        "zone_id": zone,
                        "action": "EXTEND_GREEN_PHASE",
                        "reason": f"Congestion detected (Avg speed: {avg_speed:.1f} km/h)"
                    }
                    producer.send('urbanpulse.traffic.signals.control', value=signal_override)
                    print(f"🚦 [SIGNAL OVERRIDE] Congestion in {zone}. Green phase extended.")
            
            # Reset window state
            zone_speeds.clear()
            window_start_time = current_time
