import os
import json
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.api.common.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.util.java_utils import to_jarray

def build_flink_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.get_checkpoint_config().set_checkpoint_interval(10000) # 10s state checkpointing

    kafka_brokers = "localhost:9092,localhost:9094,localhost:9096"

    # Define Global Kafka Sink for outbound Incidents
    incident_sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_brokers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("urbanpulse.incidents")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()

    # --------------------------------------------------------------------------------
    # (a) AQI EMERGENCY DETECTION (Stateless Filter/Map, SLA < 2 minutes)
    # --------------------------------------------------------------------------------
    aqi_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_brokers) \
        .set_topics("urbanpulse.air_quality") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    aqi_stream = env.from_source(aqi_source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)), "AQI-Source") \
        .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY()) \
        .filter(lambda data: data.get('aqi', 0) > 300) \
        .map(lambda data: json.dumps({
            "timestamp": data['timestamp'],
            "incident_type": "AQI_EMERGENCY",
            "entity_id": data['sensor_id'],
            "zone": data['zone'],
            "details": f"Hazardous air quality detected: AQI {data['aqi']}"
        }), output_type=Types.STRING())
    
    aqi_stream.sink_to(incident_sink)

    # --------------------------------------------------------------------------------
    # (b) TRAFFIC GRIDLOCK DETECTION (Keyed State Process Function)
    # --------------------------------------------------------------------------------
    traffic_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_brokers) \
        .set_topics("urbanpulse.traffic_signals") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    class GridlockDetector(KeyedProcessFunction):
        def open(self, context):
            # Keeps track of consecutive high wait cycles
            self.consecutive_cycles = context.get_state(ValueStateDescriptor("cycles", Types.INT()))

        def process_element(self, value, ctx, out):
            data = json.loads(value)
            current_cycles = self.consecutive_cycles.value() or 0
            
            if data.get('avg_wait_sec', 0) > 180:
                current_cycles += 1
            else:
                current_cycles = 0
            
            self.consecutive_cycles.update(current_cycles)
            
            if current_cycles >= 3:
                out.collect(json.dumps({
                    "timestamp": data['timestamp'],
                    "incident_type": "TRAFFIC_GRIDLOCK",
                    "entity_id": data['junction_id'],
                    "zone": data['zone'],
                    "details": f"Gridlock detected: Wait time > 180s for {current_cycles} consecutive cycles"
                }))

    traffic_stream = env.from_source(traffic_source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(15)), "Traffic-Source") \
        .key_by(lambda x: json.loads(x)['junction_id']) \
        .process(GridlockDetector(), output_type=Types.STRING())

    traffic_stream.sink_to(incident_sink)

    # --------------------------------------------------------------------------------
    # (c) BUS BUNCHING DETECTION (Spatial Proximity State Process Function)
    # --------------------------------------------------------------------------------
    bus_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_brokers) \
        .set_topics("urbanpulse.bus_gps") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    class BusBunchingDetector(KeyedProcessFunction):
        def open(self, context):
            # MapState storing the latest coordinates and timestamps per bus_id
            self.bus_positions = context.get_state(ValueStateDescriptor("positions", Types.PICKLED_BYTE_ARRAY()))
            # Tracks timestamp when bunching behavior began on this route
            self.bunching_started = context.get_state(ValueStateDescriptor("bunching_ts", Types.LONG()))

        def haversine_distance(self, lat1, lon1, lat2, lon2):
            import math
            R = 6371000.0  # Earth radius in meters
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlam = math.radians(lon2 - lon1)
            a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2
            return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

        def process_element(self, value, ctx, out):
            current_bus = json.loads(value)
            route_id = current_bus['route_id']
            curr_bus_id = current_bus['bus_id']
            
            # Fetch previous state or initialize empty dict
            state_bytes = self.bus_positions.value()
            fleet = json.loads(state_bytes.decode('utf-8')) if state_bytes else {}
            
            # Update current bus location in state
            fleet[curr_bus_id] = {
                "lat": current_bus['lat'],
                "lon": current_bus['lon'],
                "ts": ctx.timestamp()
            }
            self.bus_positions.update(json.dumps(fleet).encode('utf-8'))
            
            bunching_detected = False
            bunched_bus_id = None
            
            # Evaluate spatial thresholds against all active buses on this route key
            for b_id, metrics in fleet.items():
                if b_id == curr_bus_id:
                    continue
                
                # Check if data is fresh (within last 2 minutes) to prevent testing stale buses
                if ctx.timestamp() - metrics['ts'] < 120000:
                    dist = self.haversine_distance(current_bus['lat'], current_bus['lon'], metrics['lat'], metrics['lon'])
                    if dist <= 200:
                        bunching_detected = True
                        bunched_bus_id = b_id
                        break
            
            if bunching_detected:
                start_ts = self.bunching_started.value()
                if start_ts is None:
                    self.bunching_started.update(ctx.timestamp())
                elif ctx.timestamp() - start_ts >= 300000:  # 5 minutes in milliseconds
                    out.collect(json.dumps({
                        "timestamp": current_bus['timestamp'],
                        "incident_type": "BUS_BUNCHING",
                        "entity_id": route_id,
                        "zone": "TRANSIT_NET",
                        "details": f"Buses {curr_bus_id} and {bunched_bus_id} bunched under 200m for > 5 min."
                    }))
            else:
                self.bunching_started.clear()

    bus_stream = env.from_source(bus_source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)), "Bus-Source") \
        .key_by(lambda x: json.loads(x)['route_id']) \
        .process(BusBunchingDetector(), output_type=Types.STRING())

    bus_stream.sink_to(incident_sink)
    env.execute("UrbanPulse-RealTime-Incident-Engine")

if __name__ == '__main__':
    build_flink_pipeline()
