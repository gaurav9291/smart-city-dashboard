import os
import requests
import json
import time
import random
from kafka import KafkaProducer
from cloud_config import get_kafka_producer_config

# 1. Configuration & API Tokens
WAQI_TOKEN = os.getenv("WAQI_TOKEN", "demo")
KAFKA_TOPIC = 'aqi-data'

# 2. Define your 5 Pune Zones and their environmental modifiers
# This replicates real-world behaviors (e.g., Industrial/Commercial areas have worse AQI)
PUNE_ZONES = {
    "Hinjewadi": {"lat": 18.5912, "lon": 73.7389, "aqi_modifier": 1.05},     # IT Hub - slightly elevated
    "FC Road": {"lat": 18.5236, "lon": 73.8478, "aqi_modifier": 1.25},       # Commercial - heavy traffic congestion
    "Kothrud": {"lat": 18.5074, "lon": 73.8077, "aqi_modifier": 0.85},       # Residential - cleaner air
    "Hadapsar": {"lat": 18.5089, "lon": 73.9260, "aqi_modifier": 1.35},      # Industrial - high industrial emissions
    "Shivajinagar": {"lat": 18.5308, "lon": 73.8474, "aqi_modifier": 1.10}    # Mixed Use - urban center baseline
}

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        **get_kafka_producer_config(
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    )
    print("✅ Kafka Producer initialized successfully.")
except Exception as e:
    print(f"❌ Failed to initialize Kafka Producer: {e}")
    print("⚠️ Script will run in DRY-RUN mode (printing to console only without Kafka broadcasting).")
    producer = None

def fetch_live_pune_baseline():
    """Fetches the real-time baseline data for Pune from WAQI API"""
    url = "https://api.waqi.info/feed/pune/"
    try:
        response = requests.get(url, params={"token": WAQI_TOKEN}, timeout=10)
        if response.status_code == 200:
            raw_json = response.json()
            if raw_json.get("status") == "ok":
                return raw_json["data"]
        print("⚠️ Warning: Failed to fetch live data, using mock baseline.")
    except Exception as e:
        print(f"⚠️ API Fetch exception: {e}. Falling back to standard baseline.")
    
    # Static fallback baseline if API token is blocked or internet drops out
    return {
        "aqi": 42,
        "iaqi": {"pm25": {"v": 12}, "pm10": {"v": 35}, "no2": {"v": 15}, "co": {"v": 5}, "t": {"v": 31}, "h": {"v": 55}, "w": {"v": 8}},
        "time": {"iso": time.strftime("%Y-%m-%dT%H:%M:%S+05:30")}
    }

def generate_zone_stream_event(zone_name, zone_config, baseline_data):
    """Applies zone modifiers and random micro-variance to build a unique event packet"""
    # Introduce a minor fluctuating micro-variance (-3% to +3%) so data points wiggle dynamically on graphs
    micro_variance = random.uniform(0.97, 1.03)
    
    # Calculate compound multiplier
    final_multiplier = zone_config["aqi_modifier"] * micro_variance
    
    # Deep extract with defensive get logic
    iaqi = baseline_data.get("iaqi", {})
    
    def scale_metric(metric_key, default_val=0):
        val = iaqi.get(metric_key, {}).get("v")
        if val is None: return default_val
        return round(val * final_multiplier, 1)

    # Construct the final normalized real-time payload matching your target schema
    return {
        "sensor_id": f"AQI_{zone_name.upper().replace(' ', '')}_001",
        "zone": zone_name,
        "lat": zone_config["lat"],
        "lon": zone_config["lon"],
        "aqi": int(round(baseline_data.get("aqi", 50) * final_multiplier)),
        "pm25": scale_metric("pm25", 15),
        "pm10": scale_metric("pm10", 40),
        "no2": scale_metric("no2", 12),
        "co": scale_metric("co", 4),
        "temperature": scale_metric("t", 30),
        "humidity": scale_metric("h", 60),
        "wind": scale_metric("w", 5),
        "timestamp": baseline_data.get("time", {}).get("iso", time.strftime("%Y-%m-%dT%H:%M:%S+05:30"))
    }

def main_producer_loop():
    print("🚀 Smart City AQI Ingestion Pipeline Activated.")
    
    last_api_fetch_time = 0
    fetch_interval_seconds = 300  # Fetch fresh real data from WAQI once every 5 minutes
    baseline = None
    
    while True:
        current_time = time.time()
        
        # Step A: Periodically grab a real baseline payload to stay synchronized with reality
        if current_time - last_api_fetch_time > fetch_interval_seconds or baseline is None:
            print("\n🔄 Contacting WAQI API Server for refreshed Pune baseline readings...")
            baseline = fetch_live_pune_baseline()
            last_api_fetch_time = current_time
            print(f"📡 New Base Reference AQI Synchronized: {baseline.get('aqi')}")
            
        print(f"\n--- Emitting Streaming Window: {time.strftime('%X')} ---")
        
        # Step B: Loop over every target zone and blast out individual events
        for zone_name, zone_config in PUNE_ZONES.items():
            payload = generate_zone_stream_event(zone_name, zone_config, baseline)
            
            # Print to console for tracing/verification
            print(f" -> Sent Stream Event | Zone: {payload['zone']:13} | AQI: {payload['aqi']:3} | PM2.5: {payload['pm25']}")
            
            # Fire data down the Kafka pipeline channel
            if producer:
                producer.send(KAFKA_TOPIC, value=payload)
                
        # Flush batches to keep memory footprints lean
        if producer:
            producer.flush()
            
        # Stream interval delay: Adjust this window to control engine streaming velocity
        time.sleep(15)

if __name__ == "__main__":
    try:
        main_producer_loop()
    except KeyboardInterrupt:
        print("\n🛑 Producer execution terminated by developer. Exiting gracefully.")
