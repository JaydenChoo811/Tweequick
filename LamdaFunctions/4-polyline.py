import json
import math
import requests
import polyline
import os
import psycopg
from decimal import Decimal
from urllib.parse import parse_qs

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# --- CORS headers ---
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Requested-With",
}

DB_HOST = os.environ.get("PGHOST")
DB_NAME = os.environ.get("PGDATABASE")
DB_USER = os.environ.get("PGUSER")
DB_PASS = os.environ.get("PGPASSWORD")
DB_PORT = os.environ.get("PGPORT", "5432")

# --- Global DB connection ---
try:
    conn = psycopg.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )
except Exception as e:
    conn = None
    print("Failed to connect to DB at init:", e)

# --- Custom JSON encoder for Decimal + datetime ---
from decimal import Decimal
from datetime import datetime

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if exact precision is needed
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# --- Haversine (km) ---
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
    return 2 * R * math.asin(math.sqrt(a))

# --- Geocode place name to [lat, lng] ---
def geocode_place(place):
    params = {"address": place, "key": GOOGLE_API_KEY, "region": "my"}
    r = requests.get(GEOCODE_URL, params=params)
    r.raise_for_status()
    data = r.json()
    if not data["results"]:
        raise ValueError(f"Could not geocode: {place}")
    loc = data["results"][0]["geometry"]["location"]
    return [loc["lat"], loc["lng"]]


# --- Parse "lat,lon" shorthand into [lat, lon] ---
def parse_latlng(text):
    if not isinstance(text, str):
        return None
    try:
        parts = [p.strip() for p in text.split(",")]
        if len(parts) != 2:
            return None
        lat = float(parts[0])
        lon = float(parts[1])
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return None
        return [lat, lon]
    except Exception:
        return None


# --- Helpers to read and sanitize query params ---
def _first_qs(event, key):
    """Return the first value for a query param from API Gateway event.

    Supports both queryStringParameters and multiValueQueryStringParameters.
    """
    try:
        # Standard Lambda proxy (REST/HTTP API)
        qsp = event.get("queryStringParameters")
        if isinstance(qsp, dict) and key in qsp and qsp[key] is not None:
            return str(qsp[key])
        mv = event.get("multiValueQueryStringParameters")
        if isinstance(mv, dict) and key in mv and isinstance(mv[key], list) and mv[key]:
            return str(mv[key][0])

        # Non-proxy custom mapping (common shape)
        params = event.get("params")
        if isinstance(params, dict):
            qs = params.get("querystring") or params.get("queryString")
            if isinstance(qs, dict) and key in qs and qs[key] is not None:
                return str(qs[key])

        # HTTP API v2 rawQueryString fallback
        rqs = event.get("rawQueryString")
        if isinstance(rqs, str) and rqs:
            parsed = parse_qs(rqs, keep_blank_values=True)
            vals = parsed.get(key)
            if isinstance(vals, list) and vals:
                return str(vals[0])
    except Exception:
        pass
    return None


def _strip_quotes(val):
    if not isinstance(val, str):
        return val
    s = val.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

# --- Fetch hazards from DB ---
def fetch_hazards():
    if conn is None:
        raise RuntimeError("No DB connection available")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, district, latitude, longitude, final_score, risk_level, recommendation, calculated_at
            FROM risk_assessments
            ORDER BY id DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()

    hazards = []
    for row in rows:
        hazards.append({
            "id": row[0],
            "district": row[1],
            "lat": row[2],
            "lng": row[3],
            "final_score": row[4],
            "risk_level": row[5],
            "recommendation": row[6],
            "calculated_at": row[7]
        })
    return hazards

# --- Radius derivation from hazards ---
def _env_int(name, default):
    try:
        v = os.environ.get(name)
        return int(v) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

# Allow overriding per-level radii via env (in meters)
RADIUS_LOW_M = _env_int("HAZARD_RADIUS_LOW", 1500)
RADIUS_MEDIUM_M = _env_int("HAZARD_RADIUS_MEDIUM", 3000)
RADIUS_HIGH_M = _env_int("HAZARD_RADIUS_HIGH", 6000)
RADIUS_CRITICAL_M = _env_int("HAZARD_RADIUS_CRITICAL", 10000)

def weather_scale(weather: str) -> float:
    """Return a multiplicative scale factor based on weather conditions."""
    if not weather:
        return 1.0
    w = str(weather).strip().lower()
    if w in {"storm", "thunderstorm", "heavy rain", "tropical storm"}:
        return 1.8
    if w in {"rain", "fog", "haze", "drizzle"}:
        return 1.3
    return 1.0

def hazard_radius_from_level(level: str) -> int:
    if not isinstance(level, str):
        return RADIUS_LOW_M
    l = level.strip().lower()
    if l in {"critical", "very high", "severe"}:
        return RADIUS_CRITICAL_M
    if l in {"high"}:
        return RADIUS_HIGH_M
    if l in {"medium", "moderate"}:
        return RADIUS_MEDIUM_M
    return RADIUS_LOW_M

def hazard_radius_from_score(score) -> int:
    """Map a numeric score to a radius (meters). Assumes 0-100 scale; clamps if out of range."""
    try:
        # Convert Decimals to float
        s = float(score)
    except Exception:
        return RADIUS_LOW_M
    # Clamp to [0, 100] if wildly out-of-range
    if s >= 80:
        return RADIUS_CRITICAL_M
    if s >= 60:
        return RADIUS_HIGH_M
    if s >= 40:
        return RADIUS_MEDIUM_M
    return RADIUS_LOW_M

def annotate_hazards_with_radius(hazards, weather: str):
    """Add a per-hazard radius_m computed from risk_level/final_score and weather scaling."""
    scale = weather_scale(weather)
    out = []
    for hz in hazards:
        base_r = None
        if "risk_level" in hz and hz["risk_level"] is not None:
            base_r = hazard_radius_from_level(hz["risk_level"])  # meters
        elif "final_score" in hz and hz["final_score"] is not None:
            base_r = hazard_radius_from_score(hz["final_score"])  # meters
        else:
            base_r = RADIUS_LOW_M
        hz2 = dict(hz)
        hz2["radius_m"] = int(round(base_r * scale))
        out.append(hz2)
    return out

# --- Check if route intersects hazards ---
def route_intersects_hazard(points, hazards):
    """Return True if any route vertex is within that hazard's specific radius."""
    for lat, lng in points:
        for hz in hazards:
            dist_m = haversine_km(lat, lng, hz["lat"], hz["lng"]) * 1000
            r = hz.get("radius_m") or hazard_radius_from_level(hz.get("risk_level"))
            if dist_m < r:
                return True
    return False

# --- Decide hazard radius dynamically ---
def decide_radius(weather, num_hazards):
    """Deprecated: kept for backward-compat if referenced elsewhere. Not used."""
    return RADIUS_MEDIUM_M

# --- Get routes from Google API ---
def get_routes(origin, destination, travel_mode, waypoints=None):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": "routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline"
    }

    body = {
        "origin": {"location": {"latLng": {"latitude": origin[0], "longitude": origin[1]}}},
        "destination": {"location": {"latLng": {"latitude": destination[0], "longitude": destination[1]}}},
        "travelMode": travel_mode,
        "computeAlternativeRoutes": True
    }

    if waypoints:
        body["intermediates"] = [
            {"location": {"latLng": {"latitude": wp[0], "longitude": wp[1]}}}
            for wp in waypoints
        ]

    r = requests.post(ROUTES_URL, headers=headers, json=body)
    r.raise_for_status()
    return r.json().get("routes", [])

# --- Lambda Handler ---
def lambda_handler(event, context):
    try:
        # Handle CORS preflight
        method = ""
        if isinstance(event, dict):
            method = (event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
        if method == "OPTIONS":
            # Preflight OK
            return {"statusCode": 204, "headers": CORS_HEADERS, "body": ""}

        # Extract from queryStringParameters first (no typos supported)
        origin = None
        destination = None
        travel_mode = "DRIVE"
        weather = "clear"

        if isinstance(event, dict):
            # Prefer query params
            origin = _first_qs(event, "origin")
            destination = _first_qs(event, "destination")
            tm = _first_qs(event, "travelMode")
            wt = _first_qs(event, "weather")
            tm_present = tm is not None
            wt_present = wt is not None
            if tm_present:
                travel_mode = str(tm).upper()
            if wt_present:
                weather = str(wt)

            # Strip surrounding quotes if present (e.g., origin="KLCC")
            if isinstance(origin, str):
                origin = _strip_quotes(origin)
            if isinstance(destination, str):
                destination = _strip_quotes(destination)

            # If not present in query, look at body (API GW) or top-level
            if origin is None or destination is None or not tm_present or not wt_present:
                body = event.get("body")
                payload = None
                if isinstance(body, str):
                    try:
                        payload = json.loads(body)
                    except Exception:
                        payload = None
                elif isinstance(body, dict):
                    payload = body

                src = payload if isinstance(payload, dict) else event
                if origin is None:
                    origin = _strip_quotes(src.get("origin")) if isinstance(src.get("origin"), str) else src.get("origin")
                if destination is None:
                    destination = _strip_quotes(src.get("destination")) if isinstance(src.get("destination"), str) else src.get("destination")
                if not tm_present and ("travelMode" in src):
                    travel_mode = str(src.get("travelMode") or "DRIVE").upper()
                if not wt_present and ("weather" in src):
                    weather = str(src.get("weather") or "clear")

        if not origin or not destination:
            return {"statusCode": 400, "headers": CORS_HEADERS, "body": json.dumps({"error": "Missing origin or destination"})}

        # Support "lat,lon" shorthand without geocoding
        if isinstance(origin, str):
            parsed = parse_latlng(origin)
            origin = parsed if parsed is not None else geocode_place(origin)
        if isinstance(destination, str):
            parsed = parse_latlng(destination)
            destination = parsed if parsed is not None else geocode_place(destination)

        hazards_raw = fetch_hazards()
        routes = get_routes(origin, destination, travel_mode)

        scored = []
        hazards = annotate_hazards_with_radius(hazards_raw, weather)

        for r in routes:
            enc = r["polyline"]["encodedPolyline"]
            pts = polyline.decode(enc)

            if route_intersects_hazard(pts, hazards):
                continue

            min_dist = min(
                haversine_km(lat, lng, hz["lat"], hz["lng"]) * 1000
                for hz in hazards
                for lat, lng in pts
            )

            duration_s = int(r.get("duration", "0s").replace("s", "")) if "duration" in r else 0
            distance_m = r.get("distanceMeters", 0)

            scored.append({
                "polyline": enc,
                "duration_s": duration_s,
                "distance_m": distance_m,
                "min_dist": min_dist
            })

        if not scored:
            return {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps({
                "message": "No safe routes found",
                "hazards": hazards
            }, cls=CustomEncoder)}

        best = sorted(scored, key=lambda x: x["duration_s"])[0]

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "bestRoute": {
                    "polyline": best["polyline"],
                    "duration_s": best["duration_s"],
                    "distance_m": best["distance_m"],
                    "min_dist": best["min_dist"]
                },
                "alternatives": [
                    {"polyline": r["polyline"]}
                    for r in scored if r["polyline"] != best["polyline"]
                ],
                "hazards": hazards
            }, cls=CustomEncoder)
        }


    except Exception as e:
        return {"statusCode": 500, "headers": CORS_HEADERS, "body": json.dumps({"error": str(e)})}
