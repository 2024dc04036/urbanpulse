import json
import time
import threading
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC = 'urbanpulse.traffic_signals'

# --------------------------------------------------------------------------------
# GROUP 1: HIGH_PRIORITY (Dedicated Signal Control Interface)
# --------------------------------------------------------------------------------
def run_high_priority_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='HIGH_PRIORITY_SIGNAL_CONTROL',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("🚦 [HIGH PRIORITY] Consumer Group Online. Monitoring intersection phases...")
    
    for message in consumer:
        data = message.value
        # Sub-millisecond execution loop simulating signal runtime optimization
        start_process = time.time()
        
        # Real-time evaluation rule
        if data['avg_wait_sec'] > 75 and data['signal_phase'] == 'RED':
            print(f"⚡ [CRITICAL PATH] High Priority processing for Junction: {data['junction_id']} | Lag: 0ms | Action: Force Green Phase")
            
# --------------------------------------------------------------------------------
# GROUP 2: STANDARD_PRIORITY (Analytical Aggregation Dashboard)
# --------------------------------------------------------------------------------
def run_standard_priority_consumer(consumer_id):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='STANDARD_PRIORITY_ANALYTICS',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"📊 [STANDARD PRIORITY] Consumer Instance {consumer_id} Active.")
    
    for message in consumer:
        data = message.value
        
        # SIMULATED SLOWDOWN: Simulating complex disk-write or heavy aggregation lag
        # This causes the standard consumer group to accumulate significant offset lag
        time.sleep(0.8) 
        print(f"🐢 [ANALYTICS INSTANCE {consumer_id}] Completed ingestion processing for junction: {data['junction_id']} (Artificial delay applied).")

# Execution orchestrator demonstrating non-blocking priority streams
if __name__ == "__main__":
    # 1. Start the real-time critical node
    high_priority_thread = threading.Thread(target=run_high_priority_consumer)
    high_priority_thread.daemon = True
    high_priority_thread.start()
    
    # 2. Start the distributed slow analytical instances
    for i in range(1, 4):
        t = threading.Thread(target=run_standard_priority_consumer, args=(i,))
        t.daemon = True
        t.start()
        
    while True:
        time.sleep(1)
