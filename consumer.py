import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer for multiple topics
consumer = KafkaConsumer(
    'urbanpulse.transit.bus-gps',
    'urbanpulse.env.aqi',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', # Only read new data
    enable_auto_commit=True,
    group_id='urbanpulse-incident-engine',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("UrbanPulse Incident Engine Listening...")

for message in consumer:
    topic = message.topic
    data = message.value
    
    if topic == 'urbanpulse.env.aqi':
        pm25 = data.get('pm25', 0)
        if pm25 > 120.0:
            print(f"🚨 [INCIDENT] Hazmat Alert! Sensor {data['sensor_id']} reports severe PM2.5: {pm25:.2f}")
            
    elif topic == 'urbanpulse.transit.bus-gps':
        if data['status'] == 'STOPPED' and data['speed_kmh'] == 0:
            print(f"⚠️ [TRAFFIC] Bus {data['bus_id']} is stopped. Possible congestion.")
            
    # In a full production environment, this script would produce 
    # to 'urbanpulse.alerts.incidents' rather than just printing.
