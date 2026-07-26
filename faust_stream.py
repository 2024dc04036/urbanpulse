import faust

# Define the Streaming Application Engine
app = faust.App('urbanpulse-spatial-enrichment', broker='kafka://localhost:9092')

# --------------------------------------------------------------------------------
# DESERIALIZATION SCHEMAS
# --------------------------------------------------------------------------------
class BusGPS(faust.Record, serializer='json'):
    bus_id: str
    route_id: str
    lat: float
    lon: float
    speed_kmh: int
    occupancy_pct: int
    timestamp: str

class RouteSchedule(faust.Record, serializer='json'):
    route_name: str
    scheduled_arrival_time: str
    terminal: str

class EnrichedBusTelemetry(faust.Record, serializer='json'):
    bus_id: str
    route_id: str
    route_name: str
    lat: float
    lon: float
    speed_kmh: int
    scheduled_arrival_time: str
    terminal: str
    enriched_timestamp: str

# --------------------------------------------------------------------------------
# STREAM AND KTABLE PROVISIONING
# --------------------------------------------------------------------------------
gps_topic = app.topic('urbanpulse.bus_gps', value_type=BusGPS)
enriched_topic = app.topic('urbanpulse.transit.enriched_gps', value_type=EnrichedBusTelemetry)

# The Global KTable handles static schedule metadata lookups keyed by Route ID
route_schedule_table = app.Table('route_schedule_ktable', default=dict, value_type=RouteSchedule)

@app.task
async def seed_schedule_table_from_csv():
    """
    Simulates parsing the administrative CSV itinerary reference file 
    and populating the KTable cache at architecture runtime.
    """
    mock_csv_rows = {
        "ROUTE-101": {"route_name": "Metro-Express North", "scheduled_arrival_time": "14:45:00", "terminal": "Terminal-A"},
        "ROUTE-202": {"route_name": "South-Coast Shuttle", "scheduled_arrival_time": "15:00:00", "terminal": "Terminal-B"},
        "ROUTE-303": {"route_name": "Crosstown Link East", "scheduled_arrival_time": "14:30:00", "terminal": "Terminal-C"},
        "ROUTE-404": {"route_name": "Ward-Circular 4", "scheduled_arrival_time": "15:15:00", "terminal": "Terminal-D"}
    }
    for route_id, metadata in mock_csv_rows.items():
        route_schedule_table[route_id] = RouteSchedule(**metadata)
    print("📋 Global Route Schedule KTable successfully hydrated from storage reference.")

# --------------------------------------------------------------------------------
# STREAM JOIN ENGINE (Enrichment Topology)
# --------------------------------------------------------------------------------
@app.agent(gps_topic)
async def process_gps_stream(stream):
    async for event in stream:
        # Retrieve reference structural schedule from the KTable using the partition key (route_id)
        schedule_metadata = route_schedule_table[event.route_id]
        
        if schedule_metadata:
            # Construct the unified, enriched real-time payload
            enriched_payload = EnrichedBusTelemetry(
                bus_id=event.bus_id,
                route_id=event.route_id,
                route_name=schedule_metadata.route_name,
                lat=event.lat,
                lon=event.lon,
                speed_kmh=event.speed_kmh,
                scheduled_arrival_time=schedule_metadata.scheduled_arrival_time,
                terminal=schedule_metadata.terminal,
                enriched_timestamp=event.timestamp
            )
            # Route the enriched payload to the outbound topic to feed the ETA service
            await enriched_topic.send(key=event.route_id, value=enriched_payload)
        else:
            # Fallback handling for unmapped transit routes
            print(f"⚠️ [ENRICHMENT ERROR] No schedule reference located for Route ID: {event.route_id}")

if __name__ == '__main__':
    app.main()
