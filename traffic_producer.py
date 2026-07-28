import json
import time
import random
from kafka import KafkaProducer
from cloud_config import get_kafka_producer_config

KAFKA_TOPIC = 'traffic-data'

# The exact same 5 Pune zones to ensure we can join streams later
PUNE_ZONES = {
    "Hinjewadi": {"lat": 18.5912, "lon": 73.7389, "free_flow": 50, "rush_hours": [9, 10, 18, 19]},
    "FC Road": {"lat": 18.5236, "lon": 73.8478, "free_flow": 40, "rush_hours": [11, 12, 17, 18, 19]},
    "Kothrud": {"lat": 18.5074, "lon": 73.8077, "free_flow": 45, "rush_hours": [9, 18]},
    "Hadapsar": {"lat": 18.5089, "lon": 73.9260, "free_flow": 50, "rush_hours": [8, 9, 17, 18]},
    "Shivajinagar": {"lat": 18.5308, "lon": 73.8474, "free_flow": 35, "rush_hours": [9, 10, 17, 18, 19]}
}

try:
    producer = KafkaProducer(
        **get_kafka_producer_config(
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    )
    print("✅ Traffic Kafka Producer initialized successfully.")
except Exception as e:
    print(f"❌ Kafka initialization failed: {e}")
    producer = None

def simulate_traffic(zone_name, config):
    current_hour = time.localtime().tm_hour
    free_flow = config["free_flow"]
    
    # Check if the current time matches a local congestion rush hour
    if current_hour in config["rush_hours"]:
        # Heavy traffic drop: Speed drops significantly (below 20 km/h triggers congestion flag)
        current_speed = round(random.uniform(10, 19), 1)
        # 15% chance of a localized accident/blockage causing total road closure
        road_closure = random.choice([False, False, False, False, False, False, True]) 
    else:
        # Free flowing traffic patterns
        current_speed = round(random.uniform(free_flow - 8, free_flow), 1)
        road_closure = False

    # Calculate congestion percentage: (1 - current/free_flow) * 100
    congestion_pct = round((1 - (current_speed / free_flow)) * 100, 1)
    if congestion_pct < 0: congestion_pct = 0.0

    return {
        "sensor_id": f"TRF_{zone_name.upper().replace(' ', '')}_001",
        "zone": zone_name,
        "lat": config["lat"],
        "lon": config["lon"],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "current_speed_kmph": current_speed,
        "free_flow_speed": free_flow,
        "congestion_pct": congestion_pct,
        "road_closure": road_closure,
        "data_source": "Local_IoT_Simulator"
    }

def main():
    print("🚀 Traffic Streaming Engine Activated.")
    while True:
        print(f"\n--- Emitting Traffic Window: {time.strftime('%X')} ---")
        
        for zone_name, config in PUNE_ZONES.items():
            payload = simulate_traffic(zone_name, config)
            print(f" -> Sent Traffic | Zone: {payload['zone']:13} | Speed: {payload['current_speed_kmph']} km/h | Congestion: {payload['congestion_pct']}%")
            
            if producer:
                producer.send(KAFKA_TOPIC, value=payload)
                
        if producer:
            producer.flush()
            
        # Emit updates every 15 seconds to match the ingestion velocity of your ecosystem
        time.sleep(15)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Traffic producer stopped by developer.")
