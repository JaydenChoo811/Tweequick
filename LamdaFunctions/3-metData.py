"""AWS Lambda: MET warnings + combined risk score (DB-only locations).

What it does (short):
- Resolves location ONLY from PostgreSQL cache tables (no MET /locations calls)
- Fetches WARNING data for today from MET (RAINS/RAIN)
- Maps severity (0..4) and combines with NLP urgency (0..10) → final score

Inputs (event):
- { "city": "Shah Alam", "nlp": { "urgency_score": 8.0 } }
- or { "lat": 3.139, "lon": 101.686, "nlp": { "urgency_score": 6.0 } }
- Can also be wrapped as { "body": "...json..." } or include { "analysis": ... }

Environment:
- MET_GOV_KEY: MET API token (required for /data)
- PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (required for DB lookup)
    - Location cache table: env DB_TABLE_TOWNS (default 'public.towns')
    - Results sink: 'risk_assessments'
"""

from __future__ import annotations

from typing import Any, Optional
import json
from datetime import datetime
from urllib.parse import urlencode

BASE_URL = "https://api.met.gov.my/v2.1"

def fetch_met_warnings_for_location(*, location_id: str, token: str, start: str, end: str, lang: Optional[str] = "en") -> Optional[dict]:
    for cat in ("RAINS", "RAIN"):
        url = build_data_url(dataset="WARNING", category=cat, location=location_id, start=start, end=end, lang=lang)
        data = fetch_url(url, token=token)
        if isinstance(data, dict):
            res = data.get("results") or data.get("data") or []
            if res:
                data["_category_used"] = cat
                return data
    return None


# ---------- Severity mapping and risk scoring ----------
def _severity_to_level(sev: Optional[str | int]) -> int:
    if sev is None:
        return 0
    try:
        n = int(sev)
        return max(0, min(4, n))
    except Exception:
        pass
    s = str(sev).strip().lower()
    if s in {"red", "emergency", "severe"}:
        return 4
    if s in {"orange", "warning"}:
        return 3
    if s in {"amber", "watch"}:
        return 2
    if s in {"yellow", "advisory", "info", "information"}:
        return 1
    return 0


def _level_name(level: int) -> str:
    return {0: "None", 1: "Advisory", 2: "Watch", 3: "Warning", 4: "Emergency"}.get(level, "None")


def _max_severity_level(results: list[dict]) -> int:
    max_lv = 0
    for r in results or []:
        sev = r.get("severity") or r.get("level") or r.get("severity_level")
        lv = _severity_to_level(sev)
        if lv > max_lv:
            max_lv = lv
    return max_lv


def _compute_final_risk(nlp_urgency_score: float, met_level: int) -> tuple[float, str, str]:
    nlp = max(0.0, min(10.0, float(nlp_urgency_score or 0.0)))
    met_norm = max(0.0, min(10.0, float(met_level) * 2.5))
    final_score = round(nlp * 0.5 + met_norm * 0.5, 1)
    if final_score <= 3.0:
        rl, color = "Low", "green"
    elif final_score <= 6.0:
        rl, color = "Moderate", "yellow"
    elif final_score <= 8.0:
        rl, color = "High", "orange"
    else:
        rl, color = "Critical", "red"
    return final_score, rl, color


# ---------- MET API URL builders and fetch ----------
def build_data_url(*, dataset: str, category: str, location: str, start: str, end: str, lang: Optional[str] = None) -> str:
    params = {"datasetid": dataset, "datacategoryid": category, "locationid": location, "start_date": start, "end_date": end}
    if isinstance(lang, str) and lang:
        params["lang"] = lang
    return f"{BASE_URL}/data?{urlencode(params)}"


def fetch_url(url: str, *, token: str, parse_json: bool = True) -> Optional[Any]:
    try:
        import requests
    except Exception:
        print("The 'requests' library is required. Install with: pip install requests")
        raise

    headers = {"Authorization": f"METToken {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=(30, 30))
        resp.raise_for_status()
    except Exception as exc:
        print(f"Request failed: {exc}")
        return None

    if parse_json:
        try:
            return resp.json()
        except ValueError:
            return resp.text
    return resp.text


# ---------- PostgreSQL helpers (optional) ----------
def get_db_conn():
    """Return psycopg2 connection using PG* env vars; None if unavailable."""
    try:
        import os
        import psycopg  # type: ignore
        host = os.environ.get("PGHOST")
        db = os.environ.get("PGDATABASE")
        user = os.environ.get("PGUSER")
        pwd = os.environ.get("PGPASSWORD")
        port = int(os.environ.get("PGPORT", "5432"))
        if not (host and db and user and pwd):
            return None
        return psycopg.connect(host=host, port=port, dbname=db, user=user, password=pwd)
    except Exception:
        return None


def _row_to_loc_dict_from_named(row: dict) -> dict:
    """Map a row dict with arbitrary columns to the fields we need.

    Tries common alternatives for id/name/lat/lon/state.
    """
    return {
        "locationid": row.get("id") or row.get("locationid") or row.get("location_id"),
        "locationname": row.get("name") or row.get("locationname") or row.get("town_name"),
        "latitude": row.get("latitude") or row.get("lat"),
        "longitude": row.get("longitude") or row.get("lon") or row.get("lng"),
        "state_id": row.get("state_id") or row.get("stateid"),
        "state_name": row.get("state_name") or row.get("state") or row.get("statename"),
    }


def db_find_location(conn, *, city: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None, top: int = 1) -> list[dict]:
    """Find locations by name or nearest coordinates, aligning with inspector behavior.

    Uses SELECT * with ILIKE on CAST(name AS TEXT) to tolerate varying schemas.
    Table override: env DB_TABLE_TOWNS (default 'public.towns').
    """
    try:
        import os
        table = os.environ.get("DB_TABLE_TOWNS", "public.towns")
        with conn.cursor() as cur:
            if lat is not None and lon is not None:
                q = f"""
                    SELECT *,
                           (6371*2*ASIN(SQRT(POWER(SIN(RADIANS(%s - CAST(latitude AS DOUBLE PRECISION))/2),2) 
                           + COS(RADIANS(%s))*COS(RADIANS(CAST(latitude AS DOUBLE PRECISION)))
                           * POWER(SIN(RADIANS(%s - CAST(longitude AS DOUBLE PRECISION))/2),2)))) AS distance_km
                    FROM {table}
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                    ORDER BY distance_km ASC
                    LIMIT %s
                """
                cur.execute(q, (lat, lat, lon, max(1, int(top))))
                desc = cur.description or []
                cols = [d.name for d in desc]
                rows = cur.fetchall() or []
                out = []
                for r in rows:
                    obj = {cols[i]: r[i] for i in range(len(cols))}
                    out.append(_row_to_loc_dict_from_named(obj))
                return out

            if not city:
                return []
            name = city.strip()
            # exact (case-insensitive)
            q = f"""
                SELECT * FROM {table}
                WHERE CAST(name AS TEXT) ILIKE %s
                LIMIT %s
            """
            cur.execute(q, (name, max(1, int(top))))
            desc = cur.description or []
            cols = [d.name for d in desc]
            rows = cur.fetchall() or []
            if rows:
                out = []
                for r in rows:
                    obj = {cols[i]: r[i] for i in range(len(cols))}
                    out.append(_row_to_loc_dict_from_named(obj))
                return out

            # startswith
            q = f"""
                SELECT * FROM {table}
                WHERE CAST(name AS TEXT) ILIKE %s
                LIMIT %s
            """
            cur.execute(q, (f"{name}%", max(1, int(top))))
            desc = cur.description or []
            cols = [d.name for d in desc]
            rows = cur.fetchall() or []
            if rows:
                out = []
                for r in rows:
                    obj = {cols[i]: r[i] for i in range(len(cols))}
                    out.append(_row_to_loc_dict_from_named(obj))
                return out

            # contains
            q = f"""
                SELECT * FROM {table}
                WHERE CAST(name AS TEXT) ILIKE %s
                LIMIT %s
            """
            cur.execute(q, (f"%{name}%", max(1, int(top))))
            desc = cur.description or []
            cols = [d.name for d in desc]
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                obj = {cols[i]: r[i] for i in range(len(cols))}
                out.append(_row_to_loc_dict_from_named(obj))
            return out
    except Exception:
        return []


def _recommendation_for_level(level: str) -> str:
    lvl = (level or "").lower()
    if lvl == "critical":
        return "Danger: Avoid travel in affected areas; move to higher ground and follow official instructions."
    if lvl == "high":
        return "High risk: Monitor official warnings, prepare evacuation plan, avoid low-lying areas."
    if lvl == "moderate":
        return "Moderate risk: Stay alert, check local advisories, avoid flood-prone roads."
    return "Low risk: No immediate action needed; stay informed of updates."


def db_insert_risk_assessment(conn, *, district: Optional[str], latitude: Optional[float], longitude: Optional[float], final_score: float, risk_level: str) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            score = max(1.0, min(10.0, float(final_score)))
            rec = _recommendation_for_level(risk_level)
            cur.execute(
                """
                INSERT INTO risk_assessments (district, latitude, longitude, final_score, risk_level, recommendation)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (district, latitude, longitude, score, risk_level, rec),
            )
            rid = cur.fetchone()[0]
            conn.commit()
            return int(rid)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return None

# --------------- AWS Lambda entrypoint ---------------
def lambda_handler(event, context):
    import os

    try:
        # Extract inputs
        city = None
        nlp_urgency = None
        original = None
        lat = None
        lon = None

        if isinstance(event, dict) and isinstance(event.get("body"), str):
            try:
                body = json.loads(event["body"]) or {}
            except Exception:
                body = {}
            city = body.get("city") or (body.get("analysis", {}).get("extracted_locations", {}).get("cities") or [None])[0]
            nlp_urgency = (body.get("nlp", {}) or {}).get("urgency_score") or body.get("analysis", {}).get("urgency_score")
            original = body.get("original_tweet")
            try:
                lat = float(body.get("lat")) if body.get("lat") is not None else None
            except Exception:
                lat = None
            try:
                lon = float(body.get("lon")) if body.get("lon") is not None else None
            except Exception:
                lon = None
        else:
            city = (event or {}).get("city") or ((event or {}).get("analysis", {}).get("extracted_locations", {}).get("cities") or [None])[0]
            nlp_urgency = ((event or {}).get("nlp", {}) or {}).get("urgency_score") or (event or {}).get("analysis", {}).get("urgency_score")
            original = (event or {}).get("original_tweet")
            try:
                lat = float((event or {}).get("lat")) if (event or {}).get("lat") is not None else None
            except Exception:
                lat = None
            try:
                lon = float((event or {}).get("lon")) if (event or {}).get("lon") is not None else None
            except Exception:
                lon = None

        # Require at least a city or coordinates
        if not city and (lat is None or lon is None):
            return {"statusCode": 400, "body": json.dumps({"error": "missing-location", "detail": "Provide city or (lat, lon)"})}

        token = os.environ.get("MET_GOV_KEY")
        if not token:
            return {"statusCode": 500, "body": json.dumps({"error": "missing-token", "detail": "MET_GOV_KEY not set"})}

        # DB-only location resolution
        conn = get_db_conn()
        if not conn:
            return {"statusCode": 500, "body": json.dumps({"error": "database-not-configured", "detail": "PG* env vars required"})}
        import os
        table_used = os.environ.get("DB_TABLE_TOWNS", "public.towns")
        candidates = db_find_location(conn, city=city, lat=lat, lon=lon, top=1)
        loc = candidates[0] if candidates else None
        if not loc:
            return {"statusCode": 404, "body": json.dumps({"error": "location-not-found", "city": city, "lat": lat, "lon": lon, "table": table_used})}

        location_id = loc.get("locationid") or loc.get("id")
        if not location_id:
            return {"statusCode": 500, "body": json.dumps({"error": "no-location-id", "city": city})}

        # Fetch warnings for today (MET uses YYYY-MM-DD)
        today = datetime.now().strftime("%Y-%m-%d")
        data = fetch_met_warnings_for_location(location_id=location_id, token=token, start=today, end=today, lang="en")
        results = (data.get("results") or data.get("data") or []) if isinstance(data, dict) else []
        met_level = _max_severity_level(results)
        final_score, risk_level, color = _compute_final_risk(nlp_urgency, met_level)

        out = {
            "city": city,
            "state": loc.get("state_name"),
            "met": {
                "category": data.get("_category_used") if isinstance(data, dict) else None,
                "warning_count": len(results),
                "max_severity_level": met_level,
                "max_severity_name": _level_name(met_level),
            },
            "nlp": {"urgency_score": float(nlp_urgency or 0.0)},
            "risk": {"final_score": final_score, "risk_level": risk_level, "color": color},
            "calculated_at": datetime.now().isoformat(),
        }

        # Save risk assessment
        try:
            assessment_id = db_insert_risk_assessment(
                conn,
                district=loc.get("state_name"),
                latitude=loc.get("latitude"),
                longitude=loc.get("longitude"),
                final_score=final_score,
                risk_level=risk_level,
            )
            if assessment_id is not None:
                out["assessment_id"] = assessment_id
                out["recommendation"] = _recommendation_for_level(risk_level)
        except Exception:
            pass

        if original:
            out["original_tweet"] = original

        return {"statusCode": 200, "body": json.dumps(out)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": "internal-error", "detail": str(e)})}


