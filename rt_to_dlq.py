import json
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
 
consumer = KafkaConsumer(
    'urbanpulse.air_quality',
    'urbanpulse.bus_gps',
    bootstrap_servers=['localhost:9092'],
    group_id='DLQ_VALIDATION_ENGINE',
    value_deserializer=None,  # Process raw bytes for safe verification
)
 
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
def evaluate_and_route():
    print("\U0001f6e1\ufe0f UrbanPulse Inline Validation Shield Active...")
    for message in consumer:
        raw_val = message.value.decode('utf-8')
        topic = message.topic
        try:
            data = json.loads(raw_val)
        except json.JSONDecodeError:
            route_to_dlq(topic, raw_val, "INVALID_JSON_FORMAT")
            continue
 
        # ---------------------------------------------------------------------
        # VALIDATION BOUNDARY RULES
        # ---------------------------------------------------------------------
        if topic == 'urbanpulse.air_quality':
            # Check for structural sensor failure drops (Null values)
            if data.get('aqi') is None or data.get('pm25') is None:
                route_to_dlq(topic, data, "NULL_METRIC_VALUE")
            # Check physical boundaries
            elif data['aqi'] < 0 or data['aqi'] > 500:
                route_to_dlq(topic, data, "OUT_OF_RANGE_AQI")
 
        elif topic == 'urbanpulse.bus_gps':
            # Geographic envelope check for MetroConnect boundary
            # Real coordinates must fall within acceptable regional margins
            lat, lon = data.get('lat', 0), data.get('lon', 0)
            if not (18.0 <= lat <= 19.0) or not (73.0 <= lon <= 74.0):
                route_to_dlq(topic, data, "IMPOSSIBLE_GEO_COORDINATES")
 
def route_to_dlq(original_topic, payload, error_type):
    dlq_envelope = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "original_topic": original_topic,
        "error_reason": error_type,
        "raw_payload": payload
    }
    producer.send('urbanpulse.dlq', value=dlq_envelope)
    producer.flush()
    print(f"\U0001f4cc [DLQ ROUTE] Enqueued corrupt message into DLQ. Error type: {error_type}")
 
if __name__ == "__main__":
    try:
        evaluate_and_route()
    except KeyboardInterrupt:
        print("\nShutting down validation shield...")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
