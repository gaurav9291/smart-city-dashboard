import requests
import json
import time
import random
from kafka import KafkaProducer
from cloud_config import get_kafka_producer_config

KAFKA_TOPIC = 'weather-data'

# The exact same 5 Pune zones to ensure we can join them later
PUNE_ZONES = {
    "Hinjewadi": {"lat": 18.5912, "lon": 73.7389},
    "FC Road": {"lat": 18.5236, "lon": 73.8478},
    "Kothrud": {"lat": 18.5074, "lon": 73.8077},
    "Hadapsar": {"lat": 18.5089, "lon": 73.9260},
    "Shivajinagar": {"lat": 18.5308, "lon": 73.8474}
}

try:
    producer = KafkaProducer(
        **get_kafka_producer_config(
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    )
    print("✅ Weather Kafka Producer initialized.")
except Exception as e:
    print(f"❌ Kafka initialization failed: {e}")
    producer = None

def get_live_weather(lat, lon):
    """Fetches real-time weather parameters directly from Open-Meteo"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': lat,
        'longitude': lon,
        'current_weather': True,
        'hourly': 'relativehumidity_2m,precipitation,surface_pressure'
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            d = r.json()
            cw = d['current_weather']
            return {
                "temperature_c": cw['temperature'],
                "wind_speed_kmph": cw['windspeed'],
                "humidity_pct": d['hourly']['relativehumidity_2m'][0],
                "rainfall_mm": d['hourly']['precipitation'][0],
                "pressure_hpa": d['hourly']['surface_pressure'][0]
            }
    except Exception as e:
        print(f"⚠️ API Fetch error: {e}")
    
    # Fallback default values if network hiccups
    return {"temperature_c": 32.0, "wind_speed_kmph": 10.0, "humidity_pct": 60, "rainfall_mm": 0.0, "pressure_hpa": 1008.0}

def main():
    print("🚀 Weather Streaming Engine Activated.")
    while True:
        print(f"\n--- Emitting Weather Window: {time.strftime('%X')} ---")
        
        for zone_name, coords in PUNE_ZONES.items():
            # 1. Get real data from API
            base_weather = get_live_weather(coords['lat'], coords['lon'])
            
            # 2. Add subtle simulation micro-variance (-1% to +1%) to show dynamic stream fluctuations
            v = random.uniform(0.99, 1.01)
            
            payload = {
                "sensor_id": f"WTR_{zone_name.upper().replace(' ', '')}_001",
                "zone": zone_name,
                "lat": coords['lat'],
                "lon": coords['lon'],
                "temperature_c": round(base_weather["temperature_c"] * v, 1),
                "wind_speed_kmph": round(base_weather["wind_speed_kmph"] * v, 1),
                "humidity_pct": int(base_weather["humidity_pct"] * v),
                "rainfall_mm": base_weather["rainfall_mm"],
                "pressure_hpa": round(base_weather["pressure_hpa"] * v, 1),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+05:30")
            }
            
            print(f" -> Sent Weather | Zone: {payload['zone']:13} | Temp: {payload['temperature_c']}°C | Humidity: {payload['humidity_pct']}%")
            
            if producer:
                producer.send(KAFKA_TOPIC, value=payload)
                
        if producer:
            producer.flush()
            
        # Poll and stream every 15 seconds to match your pipeline speed
        time.sleep(15)

if __name__ == "__main__":
    main()
