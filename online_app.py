import os
import threading

import uvicorn

from aqi_producer import main_producer_loop
from traffic_producer import main as traffic_main
from weather_producer import main as weather_main


def start_background_thread(name, target):
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def main():
    start_background_thread("aqi-producer", main_producer_loop)
    start_background_thread("traffic-producer", traffic_main)
    start_background_thread("weather-producer", weather_main)

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("dashboard_api:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
