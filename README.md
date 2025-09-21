## Tweequick

Fact-check flood reports from Twitter with AI and meteorological data, then visualize risks and safe routes on an interactive map.

Live demo: https://main.dffql005ozs1q.amplifyapp.com/

### What it does
- Scrapes recent Twitter posts about flooding and severe weather in Malaysia.
- Analyzes content using Amazon Bedrock (LLM) to detect flood-related reports and extract locations/urgency.
- Cross-checks reports against Malaysian Meteorological Department (MET) warnings to validate risk.
- Stores results in Amazon Aurora PostgreSQL for aggregation and retrieval.
- Serves a web app on AWS Amplify that shows hazards on a map and computes safe alternate routes via a Lambda polyline service.

---

## Architecture

- Ingestion and AI
	- AWS Lambda (Twitter scraping) → Tweepy v2
	- AWS Lambda (NLP processing) → Amazon Bedrock Agent
	- AWS Lambda (MET data + scoring) → MET API v2.1, risk scoring, Aurora writes
	- AWS Step Functions: orchestrate Lambdas on a frequent schedule

- Data & APIs
	- Amazon Aurora PostgreSQL: tweets, analysis, weather, risk scores (see `DB-Setup/schema.sql`)
	- API Gateway → Lambda (Polyline): returns routes and hazard overlays; integrates with Google Routes and DB

- Frontend
	- Next.js app (Amplify hosting) in `AmplifyNextjs/`
	- Google Maps JavaScript API renders routes, hazards, and circles with radii based on risk level

---

## Tech stack

- Frontend: Next.js 15, React 19, `@react-google-maps/api`
- Backend: AWS Lambda (Python), API Gateway, Step Functions
- AI/NLP: Amazon Bedrock Agent Runtime
- Data: Amazon Aurora PostgreSQL (psycopg), MET API
- Routing: Google Routes API (Directions v2) + Polyline encoding
- Hosting: AWS Amplify (frontend)

---

## Data flow

1) Twitter → Scrape
	 - Lambda `1-twitterScrapping.py` builds a Malaysia-focused flood query and fetches recent tweets via Tweepy.
	 - Output includes tweet text, time, user, place centroid, and state mentions.

2) NLP → Bedrock
	 - Lambda `2-nlpProcessing.py` calls a Bedrock Agent to determine: `is_flooded`, `urgency_score`, `confidence`, and extracted `states/cities`.

3) MET check + Risk scoring
	 - Lambda `3-metData.py` resolves the location from DB (towns) and queries MET WARNING (RAINS/RAIN) for today.
	 - Combines MET severity with NLP urgency to compute a final risk score and level; writes an entry to `risk_assessments`.

4) Serving routes + hazards
	 - Lambda `4-polyline.py` (behind API Gateway) reads recent `risk_assessments`, calls Google Routes to compute alternatives, filters unsafe ones intersecting hazard radii, and returns the best route plus hazard overlays.

5) Frontend rendering
	 - `AmplifyNextjs/app/page.tsx` calls the Polyline API, decodes the polyline, and renders route and hazard circles on Google Maps.

---

## Repository layout

```
DB-Setup/
	db.py                # local psycopg helper & env docs
	schema.sql           # tables: tweets, analysis_results, weather_data, risk_scores, towns/districts/states
	*.py                 # importers/fetchers for bootstrapping data
LamdaFunctions/
	1-twitterScrapping.py
	2-nlpProcessing.py
	3-metData.py
	4-polyline.py
AmplifyNextjs/
	app/                 # Next.js app (Google Maps UI)
	package.json         # dev/build scripts
```

---

## Database schema (Aurora PostgreSQL)

See `DB-Setup/schema.sql` for tables. Core operational table used by the map is `risk_assessments` (inserted by `3-metData.py`; read by `4-polyline.py`). Towns/districts/states tables are used to resolve MET `locationid`s.

---

## Deployment notes

- Amplify hosts the Next.js app in `AmplifyNextjs/`.
- API Gateway integrates with the `4-polyline.py` Lambda to serve route and hazard data to the frontend.
- Step Functions schedules and orchestrates the three processing Lambdas (scrape → NLP → MET/risk) to refresh data frequently.
- Aurora PostgreSQL stores tweet, analysis, weather, and risk data.
---

## Quick links

- Frontend source: `AmplifyNextjs/`
- Lambdas: `LamdaFunctions/`
- Schema and DB helper: `DB-Setup/`
- Live app: https://main.dffql005ozs1q.amplifyapp.com/
