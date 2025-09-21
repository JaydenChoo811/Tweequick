"""Small helper to fetch weather data from a URL.

This file previously contained only a URL string. I've turned it into a
small command-line utility that fetches the given URL using `requests` and
prints a short summary (status code and optionally JSON keys).

Usage (PowerShell):
	python weather_data.py
	python weather_data.py "https://example.com/data.json"

If no URL is provided the default VisualCrossing URL for Kuala Lumpur is used.
"""

from typing import Any, Optional
import sys
import json
from datetime import datetime
from pathlib import Path

URL_TEMPLATE = (
	"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
	"{location}?unitGroup=us&key={key}&contentType=json"
)


def build_url(location: str, key: str) -> str:
	"""Build the VisualCrossing URL from `location` and `key`.

	The `location` will be URL-encoded. `key` is inserted as-is.
	"""
	from urllib.parse import quote_plus

	loc_enc = quote_plus(location)
	return URL_TEMPLATE.format(location=loc_enc, key=key)


def fetch_url(url: str, parse_json: bool = True) -> Optional[Any]:
	"""Fetch `url` using requests and return parsed JSON when possible.

	Returns parsed JSON object when `parse_json` is True and response
	contains JSON, otherwise returns raw text. On network errors returns None.
	"""
	try:
		import requests
	except Exception as e:  # pragma: no cover - runtime dependency
		print("The 'requests' library is required. Install with: pip install requests")
		raise

	try:
		resp = requests.get(url, timeout=10)
		resp.raise_for_status()
	except requests.RequestException as exc:
		print(f"Request failed: {exc}")
		return None

	if parse_json:
		try:
			return resp.json()
		except ValueError:
			# Not JSON, fall back to text
			return resp.text
	return resp.text


def _print_summary(data: Any) -> None:
	"""Print a small summary of the fetched data for quick inspection."""
	if data is None:
		print("No data fetched.")
		return
	# If it's the expected VisualCrossing response (dict), print the requested fields
	if isinstance(data, dict):
		fields = [
			"queryCost",
			"latitude",
			"longitude",
			"resolvedAddress",
			"address",
			"timezone",
			"tzoffset",
			"description",
			"days",
			"alerts",
		]

		for f in fields:
			if f in data:
				# Pretty-print days/alerts if they're complex
				if f in ("days", "alerts"):
					print(f"{f}:")
					try:
						print(json.dumps(data[f], indent=2, ensure_ascii=False)[:2000])
					except Exception:
						print(data[f])
				else:
					print(f"{f}: {data.get(f)}")
			else:
				print(f"{f}: (not present)")

	elif isinstance(data, list):
		print(f"Fetched JSON array of length {len(data)}")
	else:
		text = str(data)
		print(f"Fetched text ({len(text)} chars):")
		print(text[:1000])


def _get_units(forecast: dict) -> str:
	"""Return units string ('us' or 'metric'); default to 'us' if absent."""
	units = forecast.get("units") if isinstance(forecast, dict) else None
	if isinstance(units, str):
		return units.lower()
	return "us"


def _inches_to_mm(value_in: float) -> float:
	return float(value_in) * 25.4


def assess_flood_risk(forecast: dict) -> tuple[str, list[str]]:
	"""Heuristic flood-risk assessment. Returns (level, reasons)."""
	reasons = []
	score = 0

	if not forecast:
		return "Low", ["No forecast data"]

	if forecast.get("alerts"):
		reasons.append("Official alerts present")
		score += 3

	days = forecast.get("days") or []
	if days:
		d0 = days[0]
		# Normalize precip to millimeters for scoring
		units = _get_units(forecast)
		daily_precip_raw = float(d0.get("precip") or 0.0)
		if units == "us":
			daily_precip_mm = _inches_to_mm(daily_precip_raw)
		else:
			daily_precip_mm = daily_precip_raw
		precip_prob = float(d0.get("precipprob") or 0.0)
		precip_cover = float(d0.get("precipcover") or 0.0)
		severerisk = float(d0.get("severerisk") or 0.0)

		if daily_precip_mm >= 100:
			reasons.append(f"Daily precip very large: {daily_precip_mm:.1f} mm")
			score += 3
		elif daily_precip_mm >= 50:
			reasons.append(f"Daily precip significant: {daily_precip_mm:.1f} mm")
			score += 2

		if precip_prob >= 80 and precip_cover >= 50:
			reasons.append(f"High probability ({precip_prob}%) and wide coverage ({precip_cover}%)")
			score += 2

		if severerisk >= 50:
			reasons.append(f"Model severe-risk elevated: {severerisk}")
			score += 1

		# hourly spikes
		hours = d0.get("hours") or []
		max_hr_mm = 0.0
		for h in hours:
			try:
				p_raw = float(h.get("precip") or 0.0)
			except Exception:
				p_raw = 0.0
			p_mm = _inches_to_mm(p_raw) if units == "us" else p_raw
			if p_mm > max_hr_mm:
				max_hr_mm = p_mm
		if max_hr_mm >= 30:
			reasons.append(f"Hourly spike very intense: {max_hr_mm:.1f} mm/hr")
			score += 3
		elif max_hr_mm >= 20:
			reasons.append(f"Hourly spike intense: {max_hr_mm:.1f} mm/hr")
			score += 2

	if score >= 5:
		level = "High"
	elif score >= 2:
		level = "Medium"
	else:
		level = "Low"

	return level, reasons


def print_useful_summary(data: dict) -> None:
	"""Print a concise, useful summary for human consumption."""
	if not data:
		print("No data to summarize.")
		return

	# Basic location info
	print("Location:")
	print(f"  resolvedAddress: {data.get('resolvedAddress')}")
	print(f"  address: {data.get('address')}")
	print(f"  latitude: {data.get('latitude')}, longitude: {data.get('longitude')}")
	print(f"  timezone: {data.get('timezone')} (tzoffset {data.get('tzoffset')})")
	units = _get_units(data)
	print(f"  units: {units}")
	print()

	# Today's main metrics
	days = data.get('days') or []
	if days:
		d0 = days[0]
		print(f"Today ({d0.get('datetime')}):")
		print(f"  Conditions: {d0.get('conditions')} — {d0.get('description')}")
		print(f"  Temp: min={d0.get('tempmin')} max={d0.get('tempmax')} avg={d0.get('temp')}")
		# Normalize precip to mm for display
		daily_precip_raw = float(d0.get('precip') or 0.0)
		daily_precip_mm = _inches_to_mm(daily_precip_raw) if units == 'us' else daily_precip_raw
		print(f"  Precip total: {daily_precip_mm:.1f} mm (prob: {d0.get('precipprob')}%)")
		print(f"  Precip cover: {d0.get('precipcover')}% — types: {d0.get('preciptype')}" )
		# Show peak hourly precip if available
		hours = d0.get('hours') or []
		max_hr_mm = 0.0
		max_hr_time = None
		for h in hours:
			try:
				p_raw = float(h.get('precip') or 0.0)
			except Exception:
				p_raw = 0.0
			p_mm = _inches_to_mm(p_raw) if units == 'us' else p_raw
			if p_mm > max_hr_mm:
				max_hr_mm = p_mm
				max_hr_time = h.get('datetime')
		if max_hr_time:
			print(f"  Peak hourly precip: {max_hr_mm:.1f} mm at {max_hr_time}")

	# Alerts
	alerts = data.get('alerts')
	if alerts:
		print()
		print("ALERTS:")
		try:
			print(json.dumps(alerts, indent=2, ensure_ascii=False))
		except Exception:
			print(alerts)

	# Flood risk quick assessment
	level, reasons = assess_flood_risk(data)
	print()
	print(f"Flood risk (heuristic): {level}")
	for r in reasons:
		print(f" - {r}")


def build_enriched_today_json(data: dict, *, recent_hours: Optional[int] = None) -> dict:
	"""Return an enriched JSON object with only today's data, summary, and flood risk.

	Structure:
	{
	  generatedAt, location{...}, today{...}, alerts[...], summary{...}, floodRisk{level, reasons}
	}
	"""
	out: dict[str, Any] = {}
	out["generatedAt"] = datetime.now().isoformat()

	# Location meta
	out["location"] = {
		"resolvedAddress": data.get("resolvedAddress"),
		"address": data.get("address"),
		"latitude": data.get("latitude"),
		"longitude": data.get("longitude"),
		"timezone": data.get("timezone"),
		"tzoffset": data.get("tzoffset"),
		"units": _get_units(data),
	}

	# Today-only data (first day)
	days = data.get("days") or []
	today = days[0] if days else None
	# We'll place a cut-down 'today' without the full hours array
	today_slim: dict[str, Any] = {}
	current_hour_obj: dict[str, Any] | None = None
	if today:
		today_slim = {k: v for k, v in today.items() if k != "hours"}
		# Try to find the current hour entry
		current = data.get("currentConditions") or {}
		hours = today.get("hours") or []
		if hours:
			# currentConditions.datetime may be like '02:00:00'
			target = None
			if isinstance(current.get("datetime"), str):
				target = current["datetime"]
			# Find exact match first
			if target:
				for h in hours:
					if h.get("datetime") == target:
						current_hour_obj = h
						break
			# Fallback: pick hour with max precip, else first
			if current_hour_obj is None:
				try:
					current_hour_obj = max(
						hours,
						key=lambda x: float(x.get("precip") or 0.0)
					)
				except Exception:
					current_hour_obj = hours[0]

			# If recent_hours requested, slice the last N hours up to current
			if isinstance(recent_hours, int) and recent_hours > 0:
				# find index of current hour; if not found, use end of list
				idx = None
				if current_hour_obj is not None:
					for i, h in enumerate(hours):
						if h is current_hour_obj:
							idx = i
							break
				if idx is None:
					idx = len(hours) - 1
				start = max(0, idx - recent_hours + 1)
				out_recent = hours[start: idx + 1]
				out_recent = list(out_recent)  # shallow copy
			else:
				out_recent = []
		else:
			out_recent = []
	else:
		out_recent = []

	out["today"] = today_slim
	out["currentHour"] = current_hour_obj or {}
	if out_recent:
		out["recentHours"] = out_recent

	# Current conditions, if present
	current = data.get("currentConditions") or {}
	if current:
		units = _get_units(data)
		try:
			precip_raw = float(current.get("precip") or 0.0)
		except Exception:
			precip_raw = 0.0
		precip_mm = _inches_to_mm(precip_raw) if units == "us" else precip_raw
		out["current"] = {
			"datetime": current.get("datetime"),
			"conditions": current.get("conditions"),
			"icon": current.get("icon"),
			"temp": current.get("temp"),
			"feelslike": current.get("feelslike"),
			"humidity": current.get("humidity"),
			"precip_mm": round(precip_mm, 1),
			"wind": {
				"speed": current.get("windspeed"),
				"gust": current.get("windgust"),
				"dir": current.get("winddir"),
			},
			"pressure": current.get("pressure"),
			"uvindex": current.get("uvindex"),
		}
	else:
		out["current"] = {}

	# Alerts (pass-through)
	out["alerts"] = data.get("alerts") or []

	# Build concise summary
	summary: dict[str, Any] = {}
	if today:
		units = _get_units(data)
		daily_precip_raw = float(today.get("precip") or 0.0)
		daily_precip_mm = _inches_to_mm(daily_precip_raw) if units == "us" else daily_precip_raw
		hours = today.get("hours") or []
		max_hr_mm = 0.0
		max_hr_time = None
		for h in hours:
			try:
				p_raw = float(h.get("precip") or 0.0)
			except Exception:
				p_raw = 0.0
			p_mm = _inches_to_mm(p_raw) if units == "us" else p_raw
			if p_mm > max_hr_mm:
				max_hr_mm = p_mm
				max_hr_time = h.get("datetime")

		summary = {
			"date": today.get("datetime"),
			"conditions": today.get("conditions"),
			"description": today.get("description"),
			"temp": {
				"min": today.get("tempmin"),
				"max": today.get("tempmax"),
				"avg": today.get("temp"),
			},
			"precip": {
				"total_mm": round(daily_precip_mm, 1),
				"probability_pct": today.get("precipprob"),
				"coverage_pct": today.get("precipcover"),
				"types": today.get("preciptype"),
				"peak_hourly_mm": round(max_hr_mm, 1) if max_hr_time else 0.0,
				"peak_hourly_time": max_hr_time,
			},
			"severerisk": today.get("severerisk"),
		}
	out["summary"] = summary

	# Flood risk
	level, reasons = assess_flood_risk(data)
	out["floodRisk"] = {"level": level, "reasons": reasons}

	return out


if __name__ == "__main__":
	# Simple CLI handling: allow either a raw URL as the first arg,
	# or use --location and --key flags. If key is omitted, try env var
	# VISUAL_CROSSING_KEY.
	import os

	args = sys.argv[1:]

	# If a single positional argument looks like a full URL, use it directly
	full = False
	save = False
	outdir: Optional[str] = None
	# For filename purposes
	location_for_name: Optional[str] = None
	hours_n: Optional[int] = None

	if len(args) == 1 and args[0].startswith("http"):
		url = args[0]
		location_for_name = "custom-url"
	else:
		# parse simple flags: --location LOCATION --key KEY
		location = None
		key = os.environ.get("VISUAL_CROSSING_KEY")
		i = 0
		while i < len(args):
			a = args[i]
			if a in ("--help", "-h"):
				print(
					"Usage: python weather_data.py [URL] | [--location <name>] [--key <API_KEY>] [--full] [--save] [--outdir <DIR>] [--hours N]\n"
					"\n"
					"Options:\n"
					"  URL                 Fetch a specific URL directly (bypasses location/key).\n"
					"  -l, --location      Location name (e.g., 'kuala lumpur'). Defaults to 'kuala lumpur'.\n"
					"  -k, --key           VisualCrossing API key. If omitted, uses VISUAL_CROSSING_KEY env var.\n"
					"      --full          Print enriched JSON (today + current + floodRisk + summary).\n"
					"      --save          Save enriched JSON to a timestamped file.\n"
					"      --outdir DIR    Directory to write the saved JSON file (default: current directory).\n"
					"  -H, --hours N       Include last N hours as 'recentHours' in enriched JSON.\n"
					"  -h, --help          Show this help message and exit.\n"
				)
				sys.exit(0)
			if a in ("--location", "-l") and i + 1 < len(args):
				location = args[i + 1]
				i += 2
			elif a in ("--key", "-k") and i + 1 < len(args):
				key = args[i + 1]
				i += 2
			elif a == "--full":
				full = True
				i += 1
			elif a == "--save":
				save = True
				i += 1
			elif a == "--outdir" and i + 1 < len(args):
				outdir = args[i + 1]
				i += 2
			elif a in ("--hours", "-H") and i + 1 < len(args):
				try:
					hours_n = int(args[i + 1])
				except Exception:
					hours_n = None
				i += 2
			else:
				# Unknown token — treat as location if location not set
				if location is None:
					location = a
				i += 1

		if location is None:
			location = "kuala lumpur"

		if not key:
			print("Error: API key not provided. Set VISUAL_CROSSING_KEY or pass --key KEY")
			sys.exit(2)

	url = build_url(location, key)
	location_for_name = location

	print(f"Fetching: {url}")
	data = fetch_url(url)
	if full:
		try:
			enriched = build_enriched_today_json(
				data if isinstance(data, dict) else {},
				recent_hours=hours_n,
			)
			print(json.dumps(enriched, indent=2, ensure_ascii=False))
		except Exception:
			print(data)
	else:
		print_useful_summary(data)

	# Optionally save JSON to a timestamped file
	if save and data is not None:
		try:
			ts = datetime.now().strftime("%Y%m%d_%H%M%S")
			loc_slug = (location_for_name or "unknown").strip().replace(" ", "_").replace("/", "-")
			filename = f"weather_{loc_slug}_{ts}.json"
			directory = Path(outdir) if outdir else Path.cwd()
			directory.mkdir(parents=True, exist_ok=True)
			path = directory / filename
			with path.open("w", encoding="utf-8") as f:
				enriched = build_enriched_today_json(
					data if isinstance(data, dict) else {},
					recent_hours=hours_n,
				)
				json.dump(enriched, f, ensure_ascii=False, indent=2)
			print(f"Saved JSON to: {path}")
		except Exception as e:
			print(f"Failed to save JSON: {e}")

