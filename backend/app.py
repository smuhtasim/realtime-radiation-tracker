from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import logging
import uuid
from datetime import datetime, timedelta
import redis
import threading
import time
import re
from collections import defaultdict
from functools import wraps

# Setup Flask app and CORS
app = Flask(__name__)
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
CORS(app)

# Kafka 
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPICS = ["radiation_safe", "radiation_elevated", "radiation_dangerous"]

# Redis settings
REDIS_HOST = "redis"
REDIS_PORT = 6379
GROUP_ID = "flask-cache"
API_READER_GROUP = "api_reader_group"
CACHE_TTL = 3600
STATS_CACHE_TTL = 300

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

# Redis connection
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    logger.info("Redis connected")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    redis_client = None

def background_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=10000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
        logger.info("Kafka consumer started for caching")
        for msg in consumer:
            try:
                device_id = msg.value.get("device_id", f"unknown_{uuid.uuid4().hex[:8]}")
                timestamp = msg.value.get("timestamp", datetime.now().isoformat())
                redis_key = f"sensor:{device_id}:{timestamp}"
                if redis_client:
                    redis_client.setex(redis_key, CACHE_TTL, json.dumps(msg.value))
                    logger.debug(f"Cached to Redis: {redis_key}")
                else:
                    logger.error("Redis not connected, skipping cache")
            except Exception as e:
                logger.error(f"Failed to cache message: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer failed: {e}")

def fetch_messages_from_topics(topics, offset_reset="earliest", max_records=1000):
    """Fetch messages from Kafka topics with a limit."""
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset=offset_reset,
            enable_auto_commit=False,
            group_id=API_READER_GROUP,
            consumer_timeout_ms=30000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
            fetch_min_bytes=1,
            fetch_max_wait_ms=1000,
            fetch_max_bytes=5242880,  # 5MB
            max_poll_records=1000
        )
        logger.info(f"Fetching from topics: {', '.join(topics)}")
        
        messages = []

        start_time = datetime.now()
        max_wait_seconds = 10
        empty_polls = 0
        max_empty_polls = 10
        
        while len(messages) < max_records:
            if (datetime.now() - start_time).total_seconds() > max_wait_seconds:
                logger.info(f"Timeout, fetched {len(messages)} messages")
                break
            
            batch = consumer.poll(timeout_ms=5000)
            if not batch:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    logger.info(f"No more messages, fetched {len(messages)} messages")
                    break
                continue
            
            empty_polls = 0
            for _, records in batch.items():
                for record in records:
                    messages.append(record.value)
                    if len(messages) >= max_records:
                        break
        
        consumer.close()
        logger.info(f"Fetched {len(messages)} messages")
        return {"status": "success", "data": messages}
    except Exception as e:
        logger.error(f"Failed to fetch messages: {e}")
        return {"status": "error", "message": f"Kafka error: {str(e)}"}

def validate_coordinates(lat_min, lat_max, lon_min, lon_max):
    if not (-90 <= lat_min <= 90 and -90 <= lat_max <= 90):
        return False, "Latitude must be between -90 and 90"
    if not (-180 <= lon_min <= 180 and -180 <= lon_max <= 180):
        return False, "Longitude must be between -180 and 180"
    if lat_min >= lat_max:
        return False, "lat_min must be less than lat_max"
    if lon_min >= lon_max:
        return False, "lon_min must be less than lon_max"
    return True, ""

def parse_datetime_safe(date_str):
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        return None

def get_safe_cpm_value(msg, default_cpm=0):
    try:
        cpm = msg.get("cpm", default_cpm)
        if cpm is None or cpm == "":
            return default_cpm
        return float(cpm)
    except (ValueError, TypeError):
        return default_cpm

def rate_limit():
    def decorator(f):
        @wraps(f)
        def wrapped_function(*args, **kwargs):
            if not redis_client:
                logger.warning("Redis not available, skipping rate limit")
                return f(*args, **kwargs)
            
            client_ip = request.remote_addr
            key = f"rate_limit:{client_ip}:{f.__name__}"
            requests = redis_client.get(key)
            requests = int(requests) if requests else 0
            
            if requests >= 100:
                return jsonify({"status": "error", "message": "Rate limit exceeded"}), 429
            
            redis_client.incr(key)
            redis_client.expire(key, 60)
            return f(*args, **kwargs)
        return wrapped_function
    return decorator


@app.route("/api/health", methods=["GET"])
@rate_limit()
def health_check():
    """Health check endpoint."""
    try:
        kafka_status = "connected"
        redis_status = "connected" if redis_client else "disconnected"
        
        return jsonify({
            "status": "healthy",
            "services": {
                "kafka": kafka_status,
                "redis": redis_status
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 500



@app.route("/api/radiation/custom", methods=["GET"])
@rate_limit()
def get_custom_radiation_data():
    try:
        cpm_min = float(request.args.get("cpm_min", 50))
        cpm_max = float(request.args.get("cpm_max", 100))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if cpm_min >= cpm_max:
            return jsonify({"status": "error", "message": "cpm_min must be less than cpm_max"}), 400
        if cpm_min < 0 or cpm_max < 0:
            return jsonify({"status": "error", "message": "CPM values must be positive"}), 400
        if offset_reset not in ["earliest", "latest"]:
            return jsonify({"status": "error", "message": "offset_reset must be 'earliest' or 'latest'"}), 400
        
        logger.info(f"Custom request: cpm_min={cpm_min}, cpm_max={cpm_max}, offset_reset={offset_reset}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        processed = []
        counts = {"safe": 0, "elevated": 0, "dangerous": 0}
        
        for msg in messages:
            new_msg = msg.copy()
            cpm = get_safe_cpm_value(msg, cpm_min)
            new_msg["cpm"] = cpm
            
            if cpm < cpm_min:
                new_msg["category"] = "safe"
                counts["safe"] += 1
            elif cpm_min <= cpm <= cpm_max:
                new_msg["category"] = "elevated"
                counts["elevated"] += 1
            else:
                new_msg["category"] = "dangerous"
                counts["dangerous"] += 1
            
            new_msg["cpm_source"] = "original" if msg.get("cpm") is not None else "default"
            processed.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": counts,
            "data": processed,
            "thresholds": {"cpm_min": cpm_min, "cpm_max": cpm_max},
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in custom endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@app.route("/api/initial_radiation_data", methods=["GET"])
@rate_limit()
def get_initial_radiation_data():
    """Get initial radiation data with default CPM values and generate up to 30k entries."""
    try:
        default_cpm = float(request.args.get("default_cpm", 80))
        limit = int(request.args.get("limit", 30000))  # Reduced to 30,000
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if default_cpm < 0:
            return jsonify({"status": "error", "message": "default_cpm must be positive"}), 400
        if limit > 50000:
            return jsonify({"status": "error", "message": "limit cannot exceed 50000"}), 400
        
        logger.info(f"Initial data request: default_cpm={default_cpm}, limit={limit}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        processed = []
        
        for msg in messages:
            new_msg = msg.copy()
            cpm = get_safe_cpm_value(msg, default_cpm)
            new_msg["cpm"] = cpm
            new_msg["cpm_source"] = "original" if msg.get("cpm") is not None else "default"
            
            if cpm < 50:
                new_msg["category"] = "safe"
            elif cpm > 80:
                new_msg["category"] = "dangerous"
            else:
                new_msg["category"] = "elevated"
            
            processed.append(new_msg)
        
        while len(processed) < limit:
            import random
            synthetic_msg = {
                "device_id": f"device_{random.randint(1000, 9999)}",
                "timestamp": datetime.now().isoformat(),
                "latitude": round(random.uniform(-90, 90), 6),
                "longitude": round(random.uniform(-180, 180), 6),
                "cpm": round(random.uniform(10, 200), 2),
                "cpm_source": "synthetic",
                "category": "safe" if random.random() < 0.6 else ("elevated" if random.random() < 0.7 else "dangerous")
            }
            processed.append(synthetic_msg)
        
        processed = processed[:limit]
        
        return jsonify({
            "status": "success",
            "total_count": len(processed),
            "data": processed,
            "default_cpm_used": default_cpm,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in initial endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/api/radiation/timespan", methods=["GET"])
@rate_limit()
def get_radiation_by_timespan():
    """Filter radiation data by time period."""
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if not start_date or not end_date:
            return jsonify({"status": "error", "message": "start_date and end_date are required (YYYY-MM-DD format)"}), 400
        
        start_dt = parse_datetime_safe(start_date)
        end_dt = parse_datetime_safe(end_date)
        
        if not start_dt or not end_dt:
            return jsonify({"status": "error", "message": "Invalid date format. Use YYYY-MM-DD or ISO format"}), 400
        
        if start_dt >= end_dt:
            return jsonify({"status": "error", "message": "start_date must be before end_date"}), 400
        
        logger.info(f"Timespan filter: {start_date} to {end_date}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        time_filtered = []
        
        for msg in messages:
            timestamp_str = msg.get("timestamp")
            if timestamp_str:
                msg_dt = parse_datetime_safe(timestamp_str)
                if msg_dt and start_dt <= msg_dt <= end_dt:
                    new_msg = msg.copy()
                    new_msg["time_filter_applied"] = True
                    time_filtered.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": len(time_filtered),
            "data": time_filtered,
            "timespan": {"start_date": start_date, "end_date": end_date},
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in timespan endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@app.route("/api/radiation/<level>", methods=["GET"])
@rate_limit()
def get_radiation_by_level(level):
    """Get radiation data from Redis by category."""
    try:
        if level not in ["safe", "elevated", "dangerous"]:
            return jsonify({"status": "error", "message": "Invalid level, use safe, elevated, or dangerous"}), 400
        
        if not redis_client:
            return jsonify({"status": "error", "message": "Redis not available"}), 503
        
        keys = redis_client.keys("*")
        if not keys:
            return jsonify({"status": "success", "total_count": 0, "data": [], "message": "No cached data available"})
        
        data = []
        for key in keys:
            try:
                cached_data = redis_client.get(key)
                if cached_data:
                    data.append(json.loads(cached_data))
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Failed to parse cached data for key {key}: {e}")
                continue
        
        filtered = [d for d in data if d.get("category") == level]
        
        return jsonify({
            "status": "success",
            "total_count": len(filtered),
            "data": filtered,
            "level": level,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in level endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500



@app.route("/api/radiation/filter", methods=["GET"])
@rate_limit()
def filter_radiation_by_cpm():
    """Filter radiation data by CPM value regardless of risk level."""
    try:
        cpm_limit = float(request.args.get("cpm_limit", 100))
        operator = request.args.get("operator", "greater")
        cpm_limit_max = request.args.get("cpm_limit_max", None)
        offset_reset = request.args.get("offset_reset", "earliest")
        
        valid_operators = ["greater", "less", "equal", "between"]
        if operator not in valid_operators:
            return jsonify({"status": "error", "message": f"Invalid operator. Use: {', '.join(valid_operators)}"}), 400
        
        if operator == "between" and not cpm_limit_max:
            return jsonify({"status": "error", "message": "cpm_limit_max required for 'between' operator"}), 400
        
        if operator == "between":
            cpm_limit_max = float(cpm_limit_max)
            if cpm_limit >= cpm_limit_max:
                return jsonify({"status": "error", "message": "cpm_limit must be less than cpm_limit_max"}), 400
        
        logger.info(f"Filter request: cpm_limit={cpm_limit}, operator={operator}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        filtered_data = []
        
        for msg in messages:
            cpm = get_safe_cpm_value(msg, 0)
            include = False
            
            if operator == "greater" and cpm > cpm_limit:
                include = True
            elif operator == "less" and cpm < cpm_limit:
                include = True
            elif operator == "equal" and abs(cpm - cpm_limit) < 0.001:
                include = True
            elif operator == "between" and cpm_limit <= cpm <= cpm_limit_max:
                include = True
            
            if include:
                new_msg = msg.copy()
                new_msg["cpm"] = cpm
                new_msg["filter_applied"] = f"{operator} {cpm_limit}"
                if operator == "between":
                    new_msg["filter_applied"] = f"between {cpm_limit} and {cpm_limit_max}"
                filtered_data.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": len(filtered_data),
            "data": filtered_data,
            "filter": {
                "cpm_limit": cpm_limit,
                "operator": operator,
                "cpm_limit_max": cpm_limit_max if operator == "between" else None
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in filter endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500
    
@app.route("/api/radiation/country", methods=["GET"])
@rate_limit()
def get_radiation_by_country():
    """Filter radiation data by country (Hardcoded country lats and longs)."""
    try:
        country = request.args.get("country", "").lower()
        offset_reset = request.args.get("offset_reset", "earliest")
        
        country_bounds = {
            "germany": {"lat_min": 47.3, "lat_max": 55.1, "lon_min": 5.9, "lon_max": 15.0},
            "japan": {"lat_min": 24.0, "lat_max": 46.0, "lon_min": 129.0, "lon_max": 146.0},
            "usa": {"lat_min": 25.0, "lat_max": 49.0, "lon_min": -125.0, "lon_max": -66.0},
            "united states": {"lat_min": 25.0, "lat_max": 49.0, "lon_min": -125.0, "lon_max": -66.0},
            "france": {"lat_min": 42.0, "lat_max": 51.0, "lon_min": -5.0, "lon_max": 9.0},
            "italy": {"lat_min": 35.0, "lat_max": 47.0, "lon_min": 6.0, "lon_max": 19.0},
            "spain": {"lat_min": 35.0, "lat_max": 44.0, "lon_min": -10.0, "lon_max": 5.0},
            "uk": {"lat_min": 49.0, "lat_max": 61.0, "lon_min": -8.0, "lon_max": 2.0},
            "united kingdom": {"lat_min": 49.0, "lat_max": 61.0, "lon_min": -8.0, "lon_max": 2.0},
            "canada": {"lat_min": 41.0, "lat_max": 84.0, "lon_min": -141.0, "lon_max": -52.0},
            "australia": {"lat_min": -44.0, "lat_max": -9.0, "lon_min": 112.0, "lon_max": 154.0},
            "china": {"lat_min": 18.0, "lat_max": 54.0, "lon_min": 73.0, "lon_max": 135.0},
            "russia": {"lat_min": 41.0, "lat_max": 82.0, "lon_min": 19.0, "lon_max": 180.0},
            "india": {"lat_min": 6.0, "lat_max": 37.0, "lon_min": 68.0, "lon_max": 97.0},
            "bangladesh": {"lat_min": 20.5, "lat_max": 26.6, "lon_min": 88.0, "lon_max": 92.7},
            "brazil": {"lat_min": -34.0, "lat_max": 6.0, "lon_min": -74.0, "lon_max": -34.0}
        }
        
        if country not in country_bounds:
            return jsonify({"status": "error", "message": f"Country '{country}' not supported. Available: {', '.join(country_bounds.keys())}"}), 400
        
        bounds = country_bounds[country]
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
            return jsonify(result), 503
        
        messages = result["data"]
        country_data = []
        
        for msg in messages:
            try:
                lat = float(msg.get("latitude", 0))
                lon = float(msg.get("longitude", 0))
                
                if (bounds["lat_min"] <= lat <= bounds["lat_max"] and 
                    bounds["lon_min"] <= lon <= bounds["lon_max"]):
                    new_msg = msg.copy()
                    new_msg["country"] = country
                    country_data.append(new_msg)
            except (ValueError, TypeError):
                continue
        
        return jsonify({
            "status": "success",
            "total_count": len(country_data),
            "data": country_data,
            "country": country,
            "bounds": bounds,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in country endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500
    
@app.route("/api/radiation/location", methods=["GET"])
@rate_limit()
def get_radiation_by_location():
    """Filter radiation data by geographic bounds."""
    try:
        lat_min = float(request.args.get("lat_min", -90))
        lat_max = float(request.args.get("lat_max", 90))
        lon_min = float(request.args.get("lon_min", -180))
        lon_max = float(request.args.get("lon_max", 180))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        is_valid, error_msg = validate_coordinates(lat_min, lat_max, lon_min, lon_max)
        if not is_valid:
            return jsonify({"status": "error", "message": error_msg}), 400
        
        logger.info(f"Location filter: lat({lat_min}, {lat_max}), lon({lon_min}, {lon_max})")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        location_filtered = []
        
        for msg in messages:
            try:
                lat = float(msg.get("latitude", 0))
                lon = float(msg.get("longitude", 0))
                
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    new_msg = msg.copy()
                    new_msg["location_filter_applied"] = True
                    location_filtered.append(new_msg)
            except (ValueError, TypeError):
                continue
        
        return jsonify({
            "status": "success",
            "total_count": len(location_filtered),
            "data": location_filtered,
            "bounds": {
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in location endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/api/radiation/stats", methods=["GET"])
@rate_limit()
def get_radiation_statistics():
    """Get statistical summary of radiation data with optional filters."""
    try:
        offset_reset = request.args.get("offset_reset", "earliest")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        country = request.args.get("country", "").lower()
        
        start_dt = parse_datetime_safe(start_date) if start_date else None
        end_dt = parse_datetime_safe(end_date) if end_date else None
        if start_date and end_date and (not start_dt or not end_dt or start_dt >= end_dt):
            return jsonify({"status": "error", "message": "Invalid date range"}), 400
        
        country_bounds = {
            "germany": {"lat_min": 47.3, "lat_max": 55.1, "lon_min": 5.9, "lon_max": 15.0},
            "japan": {"lat_min": 24.0, "lat_max": 46.0, "lon_min": 129.0, "lon_max": 146.0},
            "usa": {"lat_min": 25.0, "lat_max": 49.0, "lon_min": -125.0, "lon_max": -66.0},
            "france": {"lat_min": 42.0, "lat_max": 51.0, "lon_min": -5.0, "lon_max": 9.0}
        }
        if country and country not in country_bounds:
            return jsonify({"status": "error", "message": f"Country '{country}' not supported. Available: {', '.join(country_bounds.keys())}"}), 400
        
        cache_key = f"stats:{start_date or 'all'}:{end_date or 'all'}:{country or 'all'}"
        if redis_client:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Serving stats from cache: {cache_key}")
                return jsonify(json.loads(cached_data))
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset, max_records=5000)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        if not messages:
            return jsonify({
                "status": "success",
                "message": "No data available",
                "stats": {},
                "timestamp": datetime.now().isoformat()
            })
        
        filtered_messages = []
        for msg in messages:
            include = True
            if start_date and end_date:
                timestamp_str = msg.get("timestamp")
                if timestamp_str:
                    msg_dt = parse_datetime_safe(timestamp_str)
                    if not msg_dt or not (start_dt <= msg_dt <= end_dt):
                        include = False
            if country:
                try:
                    lat = float(msg.get("latitude", 0))
                    lon = float(msg.get("longitude", 0))
                    bounds = country_bounds[country]
                    if not (bounds["lat_min"] <= lat <= bounds["lat_max"] and
                            bounds["lon_min"] <= lon <= bounds["lon_max"]):
                        include = False
                except (ValueError, TypeError):
                    include = False
            if include:
                filtered_messages.append(msg)
        
        cpm_values = []
        valid_coords = 0
        devices = set()
        time_range = {"earliest": None, "latest": None}
        location_cpm = defaultdict(list)
        categories = {"safe": 0, "elevated": 0, "dangerous": 0}
        
        for msg in filtered_messages:
            cpm = get_safe_cpm_value(msg, 0)
            if cpm > 0:
                cpm_values.append(cpm)
                category = msg.get("category", "unknown")
                if category in categories:
                    categories[category] += 1
            
            try:
                lat = float(msg.get("latitude", 0))
                lon = float(msg.get("longitude", 0))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    valid_coords += 1
                    location_cpm[f"{lat:.2f},{lon:.2f}"].append(cpm)
            except (ValueError, TypeError):
                pass
            
            device_id = msg.get("device_id")
            if device_id:
                devices.add(device_id)
            
            timestamp_str = msg.get("timestamp")
            if timestamp_str:
                msg_dt = parse_datetime_safe(timestamp_str)
                if msg_dt:
                    if time_range["earliest"] is None or msg_dt < time_range["earliest"]:
                        time_range["earliest"] = msg_dt
                    if time_range["latest"] is None or msg_dt > time_range["latest"]:
                        time_range["latest"] = msg_dt
        
        stats = {
            "total_messages": len(filtered_messages),
            "valid_cpm_count": len(cpm_values),
            "valid_coordinates": valid_coords,
            "unique_devices": len(devices),
            "category_breakdown": categories,
            "time_range": {
                "earliest": time_range["earliest"].isoformat() if time_range["earliest"] else None,
                "latest": time_range["latest"].isoformat() if time_range["latest"] else None
            }
        }
        
        if cpm_values:
            sorted_cpm = sorted(cpm_values)
            stats["cpm_statistics"] = {
                "min": min(cpm_values),
                "max": max(cpm_values),
                "avg": round(sum(cpm_values) / len(cpm_values), 2),
                "median": sorted_cpm[len(sorted_cpm)//2],
                "percentile_90": sorted_cpm[int(0.9 * len(sorted_cpm))],
                "percentile_95": sorted_cpm[int(0.95 * len(sorted_cpm))]
            }
            top_locations = sorted(
                [(loc, sum(cpms)/len(cpms)) for loc, cpms in location_cpm.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]
            stats["top_locations"] = [{"location": loc, "avg_cpm": round(avg, 2)} for loc, avg in top_locations]
        
        response = {
            "status": "success",
            "stats": stats,
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
                "country": country
            },
            "timestamp": datetime.now().isoformat()
        }
        
        if redis_client:
            redis_client.setex(cache_key, STATS_CACHE_TTL, json.dumps(response))
            logger.info(f"Cached stats: {cache_key}")
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error in stats endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@app.route("/api/radiation/safe/custom", methods=["GET"])
@rate_limit()
def get_safe_radiation_custom():
    """Get only safe radiation data with custom threshold."""
    try:
        threshold = float(request.args.get("threshold", 50))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if threshold < 0:
            return jsonify({"status": "error", "message": "Threshold must be positive"}), 400
        
        logger.info(f"Safe custom request: threshold={threshold}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        safe_data = []
        
        for msg in messages:
            cpm = get_safe_cpm_value(msg, 0)
            if cpm < threshold:
                new_msg = msg.copy()
                new_msg["cpm"] = cpm
                new_msg["category"] = "safe"
                new_msg["threshold_used"] = threshold
                safe_data.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": len(safe_data),
            "data": safe_data,
            "threshold": threshold,
            "category": "safe",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in safe custom endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/api/radiation/dangerous/custom", methods=["GET"])
@rate_limit()
def get_dangerous_radiation_custom():
    """Get only dangerous radiation data with custom threshold."""
    try:
        threshold = float(request.args.get("threshold", 100))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if threshold < 0:
            return jsonify({"status": "error", "message": "Threshold must be positive"}), 400
        
        logger.info(f"Dangerous custom request: threshold={threshold}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        dangerous_data = []
        
        for msg in messages:
            cpm = get_safe_cpm_value(msg, 0)
            if cpm > threshold:
                new_msg = msg.copy()
                new_msg["cpm"] = cpm
                new_msg["category"] = "dangerous"
                new_msg["threshold_used"] = threshold
                dangerous_data.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": len(dangerous_data),
            "data": dangerous_data,
            "threshold": threshold,
            "category": "dangerous",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in dangerous custom endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@app.route("/api/radiation/elevated/custom", methods=["GET"])
@rate_limit()
def get_elevated_radiation_custom():
    """Get only elevated radiation data with custom min/max thresholds."""
    try:
        min_cpm = float(request.args.get("min_cpm", 50))
        max_cpm = float(request.args.get("max_cpm", 100))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if min_cpm >= max_cpm:
            return jsonify({"status": "error", "message": "min_cpm must be less than max_cpm"}), 400
        if min_cpm < 0 or max_cpm < 0:
            return jsonify({"status": "error", "message": "CPM values must be positive"}), 400
        
        logger.info(f"Elevated custom request: min_cpm={min_cpm}, max_cpm={max_cpm}")
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        elevated_data = []
        
        for msg in messages:
            cpm = get_safe_cpm_value(msg, 0)
            if min_cpm <= cpm <= max_cpm:
                new_msg = msg.copy()
                new_msg["cpm"] = cpm
                new_msg["category"] = "elevated"
                new_msg["min_threshold"] = min_cpm
                new_msg["max_threshold"] = max_cpm
                elevated_data.append(new_msg)
        
        return jsonify({
            "status": "success",
            "total_count": len(elevated_data),
            "data": elevated_data,
            "min_threshold": min_cpm,
            "max_threshold": max_cpm,
            "category": "elevated",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in elevated custom endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/api/radiation/sensor", methods=["GET"])
@rate_limit()
def get_radiation_by_sensor():
    """Filter radiation data by sensor/device ID with pagination and caching."""
    try:
        sensor_id = request.args.get("sensor_id")
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
        offset_reset = request.args.get("offset_reset", "earliest")
        
        if not sensor_id or not re.match(r'^[a-zA-Z0-9_-]+$', sensor_id):
            return jsonify({"status": "error", "message": "Invalid or missing sensor_id (alphanumeric, underscores, hyphens only)"}), 400
        if limit < 1 or limit > 1000:
            return jsonify({"status": "error", "message": "Limit must be between 1 and 1000"}), 400
        if offset < 0:
            return jsonify({"status": "error", "message": "Offset must be non-negative"}), 400
        
        logger.info(f"Sensor filter: sensor_id={sensor_id}, limit={limit}, offset={offset}")
        
        cache_key = f"sensor_data:{sensor_id}:{limit}:{offset}"
        if redis_client:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Serving sensor {sensor_id} from cache")
                return jsonify(json.loads(cached_data))
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset, max_records=limit + offset)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        sensor_data = []
        time_range = {"earliest": None, "latest": None}
        cpm_values = []
        
        for msg in messages:
            if msg.get("device_id") == sensor_id:
                cpm = get_safe_cpm_value(msg, 0)
                new_msg = msg.copy()
                new_msg["sensor_filter_applied"] = True
                sensor_data.append(new_msg)
                
                timestamp_str = msg.get("timestamp")
                if timestamp_str:
                    msg_dt = parse_datetime_safe(timestamp_str)
                    if msg_dt:
                        if time_range["earliest"] is None or msg_dt < time_range["earliest"]:
                            time_range["earliest"] = msg_dt
                        if time_range["latest"] is None or msg_dt > time_range["latest"]:
                            time_range["latest"] = msg_dt
                if cpm > 0:
                    cpm_values.append(cpm)
        
        sensor_data = sensor_data[offset:offset + limit]
        
        metadata = {
            "total_readings": len(sensor_data),
            "time_range": {
                "earliest": time_range["earliest"].isoformat() if time_range["earliest"] else None,
                "latest": time_range["latest"].isoformat() if time_range["latest"] else None
            },
            "cpm_stats": {
                "count": len(cpm_values),
                "avg": round(sum(cpm_values) / len(cpm_values), 2) if cpm_values else 0,
                "max": max(cpm_values) if cpm_values else 0,
                "min": min(cpm_values) if cpm_values else 0
            } if cpm_values else {}
        }
        
        response = {
            "status": "success",
            "total_count": len(sensor_data),
            "data": sensor_data,
            "sensor_id": sensor_id,
            "metadata": metadata,
            "timestamp": datetime.now().isoformat()
        }
        
        if redis_client:
            redis_client.setex(cache_key, CACHE_TTL, json.dumps(response))
            logger.info(f"Cached sensor {sensor_id} data")
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error in sensor endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@app.route("/api/radiation/heatmap", methods=["GET"])
@rate_limit()
def get_radiation_heatmap():
    """Generate heatmap data with aggregated CPM values by location."""
    try:
        offset_reset = request.args.get("offset_reset", "earliest")
        grid_size = float(request.args.get("grid_size", 1.0))  # Degrees for grid
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        
        if grid_size <= 0 or grid_size > 10:
            return jsonify({"status": "error", "message": "grid_size must be between 0 and 10 degrees"}), 400
        
        start_dt = parse_datetime_safe(start_date) if start_date else None
        end_dt = parse_datetime_safe(end_date) if end_date else None
        if start_date and end_date and (not start_dt or not end_dt or start_dt >= end_dt):
            return jsonify({"status": "error", "message": "Invalid date range"}), 400
        
        cache_key = f"heatmap:{start_date or 'all'}:{end_date or 'all'}:{grid_size}"
        if redis_client:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Serving heatmap from cache: {cache_key}")
                return jsonify(json.loads(cached_data))
        
        result = fetch_messages_from_topics(KAFKA_TOPICS, offset_reset, max_records=5000)
        if result["status"] != "success":
            return jsonify(result), 503
        
        messages = result["data"]
        heatmap_data = defaultdict(list)
        
        for msg in messages:
            timestamp_str = msg.get("timestamp")
            if start_date and end_date and timestamp_str:
                msg_dt = parse_datetime_safe(timestamp_str)
                if not msg_dt or not (start_dt <= msg_dt <= end_dt):
                    continue
            
            try:
                lat = float(msg.get("latitude", 0))
                lon = float(msg.get("longitude", 0))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    cpm = get_safe_cpm_value(msg, 0)
                    grid_lat = round(lat / grid_size) * grid_size
                    grid_lon = round(lon / grid_size) * grid_size
                    heatmap_data[f"{grid_lat:.2f},{grid_lon:.2f}"].append(cpm)
            except (ValueError, TypeError):
                continue
        
        processed_data = [
            {
                "latitude": float(loc.split(",")[0]),
                "longitude": float(loc.split(",")[1]),
                "avg_cpm": round(sum(cpms) / len(cpms), 2),
                "count": len(cpms)
            }
            for loc, cpms in heatmap_data.items()
        ]
        
        response = {
            "status": "success",
            "total_points": len(processed_data),
            "data": processed_data,
            "grid_size": grid_size,
            "filters": {"start_date": start_date, "end_date": end_date},
            "timestamp": datetime.now().isoformat()
        }
        
        if redis_client:
            redis_client.setex(cache_key, CACHE_TTL, json.dumps(response))
            logger.info(f"Cached heatmap: {cache_key}")
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error in heatmap endpoint: {e}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


if __name__ == "__main__":
    kafka_thread = threading.Thread(target=background_kafka_consumer, daemon=True)
    kafka_thread.start()
    logger.info("Background Kafka consumer thread started")
<<<<<<< HEAD
    app.run(host="0.0.0.0", port=8000, debug=True)
=======
    app.run(host="0.0.0.0", port=8000, debug=True)
>>>>>>> 59a051abe05e0f874c4538e785787ea44e80ce9b
