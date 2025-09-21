import os
import json
import time
from typing import Any, Dict, List, Optional

import tweepy


MALAYSIAN_STATES = [
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan",
    "Pahang", "Perak", "Perlis", "Penang", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu", "Kuala Lumpur",
    "Labuan", "Putrajaya",
]


def _get_client() -> tweepy.Client:
    """Create a Tweepy v2 Client using a bearer token from env.

    Env:
      - TWITTER_BEARER_TOKEN: Required. Twitter API v2 bearer token.
    """
    token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        raise ValueError("Missing TWITTER_BEARER_TOKEN environment variable")
    return tweepy.Client(bearer_token=token, wait_on_rate_limit=True)


def _find_states(text: str) -> List[str]:
    if not text:
        return []
    lower = text.lower()
    return [s for s in MALAYSIAN_STATES if s.lower() in lower]


def build_flood_query(
    *,
    country: str = "MY",
    languages: Optional[List[str]] = None,
    include_hashtags: bool = True,
    include_landslide: bool = True,
    include_heavy_rain: bool = True,
    use_states_scope: bool = True,
    exclude_retweets: bool = True,
    exclude_replies: bool = True,
) -> str:
    """Construct a broad query for flooding/natural disaster in Malaysia.

    Tries to keep within Twitter v2 query length limits by focusing on
    the most relevant terms and scoping by country/states.
    """
    # Core flood terms (English + Malay)
    base_terms = [
        "flood", "floods", "flooding", "\"flash flood\"", "\"flash floods\"",
        "inundation", "banjir", "\"banjir kilat\"",
    ]

    if include_landslide:
        base_terms += [
            "landslide", "mudslide", "\"mud slide\"", "\"tanah runtuh\"",
        ]
    if include_heavy_rain:
        base_terms += [
            "\"heavy rain\"", "\"torrential rain\"", "monsoon", "monsun",
        ]

    hashtags: List[str] = []
    if include_hashtags:
        hashtags = ["#banjir", "#banjirkilat", "#flood", "#flooding", "#banjirKL"]

    # Scope to Malaysia: prefer place_country when available, but also
    # include textual mentions/state names as a fallback.
    country_scope = []
    if country:
        country_scope.append(f"place_country:{country}")

    names_scope = ["Malaysia", "\"Kuala Lumpur\"", "KL"]
    if use_states_scope:
        # Quote multi-word state names
        for s in MALAYSIAN_STATES:
            if " " in s:
                names_scope.append(f'"{s}"')
            else:
                names_scope.append(s)

    # Language filter
    if not languages:
        languages = ["en", "ms"]  # English + Malay
    lang_clause = " OR ".join([f"lang:{l}" for l in languages])

    # Assemble query blocks
    terms_block = " OR ".join(base_terms + hashtags)
    scope_block = " OR ".join(country_scope + names_scope)

    clauses = [f"({terms_block})", f"({scope_block})", f"({lang_clause})"]
    if exclude_retweets:
        clauses.append("-is:retweet")
    if exclude_replies:
        clauses.append("-is:reply -is:quote")

    query = " ".join(clauses)
    return query


def fetch_tweets_json(query: str, limit: int = 20, client: Optional[tweepy.Client] = None) -> List[Dict[str, Any]]:
    """Fetch recent tweets for a query and return JSON-serializable dicts.

    - query: Twitter search query string
    - limit: Max number of tweets to return (approx; pagination stops when reached)
    - client: Optional Tweepy client; if None, created from env
    """
    if client is None:
        client = _get_client()

    tweets: List[Dict[str, Any]] = []

    # Twitter API max_results per page: 10..100 for recent search
    max_results = 100 if limit > 100 else max(10, limit)

    paginator = tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=["created_at", "geo", "public_metrics", "lang", "author_id"],
        expansions=["author_id", "geo.place_id"],
        user_fields=["username", "location"],
        place_fields=["full_name", "geo"],
        max_results=max_results,
    )

    users: Dict[str, Any] = {}
    places: Dict[str, Any] = {}

    for response in paginator:
        # Collect user + place info
        includes = getattr(response, "includes", {}) or {}
        if "users" in includes and includes["users"]:
            for u in includes["users"]:
                users[u.id] = u

        if "places" in includes and includes["places"]:
            for p in includes["places"]:
                places[p.id] = p

        data = getattr(response, "data", None)
        if not data:
            # No more tweets in this page
            continue

        for tweet in data:
            user = users.get(tweet.author_id) if getattr(tweet, "author_id", None) else None
            place = places.get(tweet.geo["place_id"]) if getattr(tweet, "geo", None) else None

            lat: Optional[float] = None
            lon: Optional[float] = None
            place_name: Optional[str] = None

            if place and getattr(place, "geo", None) and isinstance(place.geo, dict) and "bbox" in place.geo:
                bbox = place.geo["bbox"]  # [west_lng, south_lat, east_lng, north_lat]
                try:
                    lon = (bbox[0] + bbox[2]) / 2
                    lat = (bbox[1] + bbox[3]) / 2
                except Exception:
                    lon, lat = None, None
                place_name = getattr(place, "full_name", None)

            metrics = getattr(tweet, "public_metrics", {}) or {}

            states = _find_states(getattr(tweet, "text", ""))

            tweets.append({
                "id": str(getattr(tweet, "id", "")),
                "text": getattr(tweet, "text", None),
                "created_at": getattr(tweet, "created_at", None).isoformat() if getattr(tweet, "created_at", None) else None,
                "lang": getattr(tweet, "lang", None),
                "retweets": metrics.get("retweet_count"),
                "likes": metrics.get("like_count"),
                "replies": metrics.get("reply_count"),
                "user": getattr(user, "username", None) if user else None,
                "user_location": getattr(user, "location", None) if user else None,
                "place_name": place_name,
                "latitude": lat,
                "longitude": lon,
                "state_mentioned": states,
            })

            if len(tweets) >= limit:
                break
        if len(tweets) >= limit:
            break

    return tweets


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler.

    Event shape:
      {
        "query": "flood Malaysia",            # required
        "limit": 20,                           # optional, default 20
        "only_with_states": false              # optional, default False
      }

    Returns JSON-serializable dict with tweets list.
    """
    # Detect if invoked by API Gateway/HTTP API before mutating event
    is_http_invoke = isinstance(event, dict) and (
        "requestContext" in event or "rawPath" in event or "version" in event
    )

    # Allow direct invocation via API Gateway (body as JSON string)
    if isinstance(event, dict) and "body" in event and isinstance(event["body"], str):
        try:
            body = json.loads(event["body"] or "{}")
            # Merge top-level keys while preserving typical API GW envelope
            merged = {**event, **body}
            event = merged
        except json.JSONDecodeError:
            pass

    # Build query: allow presets for flooding natural disasters
    query = (event or {}).get("query")
    use_flood_preset = bool((event or {}).get("use_flood_preset") or (event or {}).get("preset") == "flood")
    if use_flood_preset or not query:
        query = build_flood_query(
            country=(event or {}).get("country", "MY"),
            languages=(event or {}).get("languages"),
            include_hashtags=bool((event or {}).get("include_hashtags", True)),
            include_landslide=bool((event or {}).get("include_landslide", True)),
            include_heavy_rain=bool((event or {}).get("include_heavy_rain", True)),
            use_states_scope=bool((event or {}).get("use_states_scope", True)),
            exclude_retweets=bool((event or {}).get("exclude_retweets", True)),
            exclude_replies=bool((event or {}).get("exclude_replies", True)),
        )
    if not query:
        payload = {"status": "error", "error": "Missing required 'query' in event"}
        if is_http_invoke:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(payload, ensure_ascii=False),
            }
        return payload

    limit = (event or {}).get("limit", 20)
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    limit = max(1, min(500, limit))

    only_with_states = bool((event or {}).get("only_with_states", False))

    start = time.time()
    try:
        client = _get_client()
        tweets = fetch_tweets_json(query=query, limit=limit, client=client)
        if only_with_states:
            tweets = [t for t in tweets if t.get("state_mentioned")]  # non-empty list
        duration_ms = int((time.time() - start) * 1000)
        payload = {
            "status": "ok",
            "query": query,
            "limit": limit,
            "count": len(tweets),
            "duration_ms": duration_ms,
            "tweets": tweets,
        }
        if is_http_invoke:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(payload, ensure_ascii=False),
            }
        return payload
    except Exception as e:
        payload = {"status": "error", "error": str(e)}
        if is_http_invoke:
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(payload, ensure_ascii=False),
            }
        return payload


if __name__ == "__main__":
    test_query = os.environ.get("TEST_QUERY", "flood Malaysia")
    event = {
        "query": test_query,
        "limit": int(os.environ.get("TEST_LIMIT", "10")),
        "only_with_states": os.environ.get("TEST_ONLY_WITH_STATES", "false").lower() in {"1", "true", "yes"},
    }
    result = handler(event, None)
    print(json.dumps(result, ensure_ascii=False)[:2000])
