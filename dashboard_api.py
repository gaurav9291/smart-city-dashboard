import os

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from cloud_config import (
    get_elasticsearch_auth,
    get_elasticsearch_index,
    get_elasticsearch_url,
)


ELASTICSEARCH_URL = get_elasticsearch_url()
ELASTICSEARCH_INDEX = get_elasticsearch_index()
ELASTICSEARCH_AUTH = get_elasticsearch_auth()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_DIR = os.path.join(BASE_DIR, "dashboard")

app = FastAPI(title="Smart City Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")


def elasticsearch_request(method, path, **kwargs):
    url = f"{ELASTICSEARCH_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        response = requests.request(method, url, auth=ELASTICSEARCH_AUTH, timeout=10, **kwargs)
        if response.status_code == 404 and path.startswith(f"{ELASTICSEARCH_INDEX}/"):
            return {"hits": {"hits": []}, "aggregations": {}}
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Elasticsearch request failed: {exc}") from exc


def search_documents(size=200, range_minutes=60):
    query = {
        "size": size,
        "sort": [{"synchronized_time": {"order": "desc"}}],
        "query": {
            "range": {
                "synchronized_time": {
                    "gte": f"now-{range_minutes}m",
                    "lte": "now"
                }
            }
        }
    }
    result = elasticsearch_request("POST", f"{ELASTICSEARCH_INDEX}/_search", json=query)
    return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]


def latest_by_zone(documents):
    latest = {}
    for doc in documents:
        zone = doc.get("zone")
        if zone and zone not in latest:
            latest[zone] = doc
    return list(latest.values())


def time_range_query(range_minutes):
    return {
        "range": {
            "synchronized_time": {
                "gte": f"now-{range_minutes}m",
                "lte": "now"
            }
        }
    }


def safe_value(metric):
    value = metric.get("value")
    if value is None:
        return None
    return round(value, 1)


def bucket_values(bucket):
    return {
        "zone": bucket["key"],
        "record_count": bucket["doc_count"],
        "avg_aqi": safe_value(bucket["avg_aqi"]),
        "max_aqi": safe_value(bucket["max_aqi"]),
        "avg_congestion": safe_value(bucket["avg_congestion"]),
        "max_congestion": safe_value(bucket["max_congestion"]),
        "avg_speed": safe_value(bucket["avg_speed"]),
        "avg_temperature": safe_value(bucket["avg_temperature"]),
        "avg_humidity": safe_value(bucket["avg_humidity"]),
        "total_rainfall": safe_value(bucket["total_rainfall"]),
        "pollution_trap_count": bucket["pollution_traps"]["doc_count"],
        "road_closure_count": bucket["road_closures"]["doc_count"],
    }


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(DASHBOARD_DIR, "index.html"))


@app.get("/api/health")
def health():
    result = elasticsearch_request("GET", "")
    return {
        "status": "ok",
        "elasticsearch": result.get("version", {}).get("number"),
        "index": ELASTICSEARCH_INDEX,
    }


@app.get("/api/latest")
def latest(range_minutes: int = Query(default=60, ge=1, le=1440)):
    docs = latest_by_zone(search_documents(size=500, range_minutes=range_minutes))
    return {"records": docs}


@app.get("/api/summary")
def summary(range_minutes: int = Query(default=60, ge=1, le=1440)):
    query = {
        "size": 0,
        "query": time_range_query(range_minutes),
        "aggs": {
            "avg_aqi": {"avg": {"field": "aqi"}},
            "max_aqi": {"max": {"field": "aqi"}},
            "avg_congestion": {"avg": {"field": "congestion_pct"}},
            "avg_temperature": {"avg": {"field": "temperature_c"}},
            "record_count": {"value_count": {"field": "aqi"}},
            "zones": {"cardinality": {"field": "zone.keyword"}},
            "last_updated": {"max": {"field": "synchronized_time"}},
            "alerts": {"terms": {"field": "city_alert.keyword", "size": 10}},
            "aqi_statuses": {"terms": {"field": "aqi_status.keyword", "size": 10}},
            "traffic_statuses": {"terms": {"field": "traffic_status.keyword", "size": 10}},
            "pollution_traps": {
                "filter": {"term": {"city_alert.keyword": "🔴 POLLUTION TRAP"}}
            },
            "road_closures": {
                "filter": {"term": {"road_closure": True}}
            }
        }
    }
    aggs = elasticsearch_request("POST", f"{ELASTICSEARCH_INDEX}/_search", json=query).get("aggregations", {})
    return {
        "zone_count": aggs.get("zones", {}).get("value", 0),
        "record_count": aggs.get("record_count", {}).get("value", 0),
        "avg_aqi": safe_value(aggs.get("avg_aqi", {})),
        "max_aqi": safe_value(aggs.get("max_aqi", {})),
        "avg_congestion": safe_value(aggs.get("avg_congestion", {})),
        "avg_temperature": safe_value(aggs.get("avg_temperature", {})),
        "pollution_trap_count": aggs.get("pollution_traps", {}).get("doc_count", 0),
        "road_closure_count": aggs.get("road_closures", {}).get("doc_count", 0),
        "alerts": {
            bucket["key"]: bucket["doc_count"]
            for bucket in aggs.get("alerts", {}).get("buckets", [])
        },
        "aqi_statuses": {
            bucket["key"]: bucket["doc_count"]
            for bucket in aggs.get("aqi_statuses", {}).get("buckets", [])
        },
        "traffic_statuses": {
            bucket["key"]: bucket["doc_count"]
            for bucket in aggs.get("traffic_statuses", {}).get("buckets", [])
        },
        "last_updated": aggs.get("last_updated", {}).get("value_as_string"),
    }


@app.get("/api/zone-analytics")
def zone_analytics(range_minutes: int = Query(default=60, ge=1, le=1440)):
    query = {
        "size": 0,
        "query": time_range_query(range_minutes),
        "aggs": {
            "zones": {
                "terms": {"field": "zone.keyword", "size": 20, "order": {"_key": "asc"}},
                "aggs": {
                    "avg_aqi": {"avg": {"field": "aqi"}},
                    "max_aqi": {"max": {"field": "aqi"}},
                    "avg_congestion": {"avg": {"field": "congestion_pct"}},
                    "max_congestion": {"max": {"field": "congestion_pct"}},
                    "avg_speed": {"avg": {"field": "current_speed_kmph"}},
                    "avg_temperature": {"avg": {"field": "temperature_c"}},
                    "avg_humidity": {"avg": {"field": "humidity_pct"}},
                    "total_rainfall": {"sum": {"field": "rainfall_mm"}},
                    "pollution_traps": {
                        "filter": {"term": {"city_alert.keyword": "🔴 POLLUTION TRAP"}}
                    },
                    "road_closures": {
                        "filter": {"term": {"road_closure": True}}
                    }
                }
            }
        }
    }
    buckets = elasticsearch_request("POST", f"{ELASTICSEARCH_INDEX}/_search", json=query) \
        .get("aggregations", {}) \
        .get("zones", {}) \
        .get("buckets", [])
    return {"zones": [bucket_values(bucket) for bucket in buckets]}


@app.get("/api/status-breakdown")
def status_breakdown(range_minutes: int = Query(default=60, ge=1, le=1440)):
    query = {
        "size": 0,
        "query": time_range_query(range_minutes),
        "aggs": {
            "alerts": {"terms": {"field": "city_alert.keyword", "size": 10}},
            "aqi_statuses": {"terms": {"field": "aqi_status.keyword", "size": 10}},
            "traffic_statuses": {"terms": {"field": "traffic_status.keyword", "size": 10}},
            "weather_conditions": {"terms": {"field": "weather_condition.keyword", "size": 10}},
        }
    }
    aggs = elasticsearch_request("POST", f"{ELASTICSEARCH_INDEX}/_search", json=query).get("aggregations", {})
    return {
        name: [
            {"name": bucket["key"], "value": bucket["doc_count"]}
            for bucket in agg.get("buckets", [])
        ]
        for name, agg in aggs.items()
    }


@app.get("/api/trends")
def trends(range_minutes: int = Query(default=60, ge=1, le=1440), size: int = Query(default=500, ge=50, le=2000)):
    query = {
        "size": 0,
        "query": time_range_query(range_minutes),
        "aggs": {
            "timeline": {
                "date_histogram": {
                    "field": "synchronized_time",
                    "fixed_interval": "30s",
                    "min_doc_count": 1
                },
                "aggs": {
                    "avg_aqi": {"avg": {"field": "aqi"}},
                    "avg_congestion": {"avg": {"field": "congestion_pct"}},
                    "avg_speed": {"avg": {"field": "current_speed_kmph"}},
                    "avg_temperature": {"avg": {"field": "temperature_c"}},
                    "pollution_traps": {
                        "filter": {"term": {"city_alert.keyword": "🔴 POLLUTION TRAP"}}
                    }
                }
            }
        }
    }
    buckets = elasticsearch_request("POST", f"{ELASTICSEARCH_INDEX}/_search", json=query) \
        .get("aggregations", {}) \
        .get("timeline", {}) \
        .get("buckets", [])
    return {
        "records": [
            {
                "time": bucket["key_as_string"],
                "record_count": bucket["doc_count"],
                "avg_aqi": safe_value(bucket["avg_aqi"]),
                "avg_congestion": safe_value(bucket["avg_congestion"]),
                "avg_speed": safe_value(bucket["avg_speed"]),
                "avg_temperature": safe_value(bucket["avg_temperature"]),
                "pollution_trap_count": bucket["pollution_traps"]["doc_count"],
            }
            for bucket in buckets
        ]
    }
