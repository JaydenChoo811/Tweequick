r"""Load MET location indexes into PostgreSQL (states, districts, towns).

This script reads CSV/JSON files produced by the weather/locations tools and
persists them in a normalized schema for easy querying:

  - states(id, name)
  - districts(id, name, state_id)
  - towns(id, name, latitude, longitude, state_id, district_id)

Relationships:
  - A district belongs to a state.
  - A town belongs to a state, and optionally to a district (if derivable).

Usage (PowerShell):
  # Dry run (no DB writes). Reads from default 'weather/locations' folder
  # and prints counts derived from files.
    .venv\Scripts\python.exe locations_to_db.py --dry-run

  # Create schema (tables + indexes) and load all data
    .venv\Scripts\python.exe locations_to_db.py --create-schema --load

  # Specify a custom source path/glob
    .venv\Scripts\python.exe locations_to_db.py --create-schema --load --source "weather/locations/*.csv"

Environment / DB connection:
  Uses db.py which reads libpq-style env vars (PGHOST, PGPORT, PGUSER,
  PGPASSWORD, PGDATABASE, PGSSLMODE, etc). A .env file is auto-loaded if present.
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Dict, Iterable, List, Optional, Tuple

import db
from weather.find_location import (
    Location,
    load_locations,
    build_hierarchy_indexes,
    derive_state_for_location,
)


# -----------------------------
# DDL helpers
# -----------------------------
SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS public.states (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.districts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    state_id TEXT NOT NULL REFERENCES public.states(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.towns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    state_id TEXT REFERENCES public.states(id) ON DELETE SET NULL,
    district_id TEXT REFERENCES public.districts(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fast name search (case-insensitive) and relationship lookups
CREATE INDEX IF NOT EXISTS idx_states_name ON public.states (lower(name));
CREATE INDEX IF NOT EXISTS idx_districts_name ON public.districts (lower(name));
CREATE INDEX IF NOT EXISTS idx_towns_name ON public.towns (lower(name));
CREATE INDEX IF NOT EXISTS idx_towns_state ON public.towns (state_id);
CREATE INDEX IF NOT EXISTS idx_towns_district ON public.towns (district_id);

-- Convenience unified view for ad-hoc queries/search
CREATE OR REPLACE VIEW public.places_view AS
    SELECT 'STATE'::text AS category, s.id, s.name, NULL::double precision AS latitude, NULL::double precision AS longitude,
        s.id AS state_id, NULL::text AS district_id
    FROM public.states s
    UNION ALL
    SELECT 'DISTRICT'::text AS category, d.id, d.name, NULL::double precision AS latitude, NULL::double precision AS longitude,
        d.state_id, d.id AS district_id
    FROM public.districts d
    UNION ALL
    SELECT 'TOWN'::text AS category, t.id, t.name, t.latitude, t.longitude,
        t.state_id, t.district_id
    FROM public.towns t;
"""


UPSERT_STATE_SQL = r"""
INSERT INTO public.states (id, name, updated_at)
VALUES (%s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  updated_at = now();
"""


UPSERT_DISTRICT_SQL = r"""
INSERT INTO public.districts (id, name, state_id, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  state_id = EXCLUDED.state_id,
  updated_at = now();
"""


UPSERT_TOWN_SQL = r"""
INSERT INTO public.towns (id, name, latitude, longitude, state_id, district_id, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  state_id = EXCLUDED.state_id,
  district_id = EXCLUDED.district_id,
  updated_at = now();
"""


def _cat(l: Location) -> str:
    return (l.category or "").upper()


def _dedupe_by_id(locs: Iterable[Location]) -> List[Location]:
    seen: Dict[str, Location] = {}
    for l in locs:
        if not l.id:
            continue
        seen[l.id] = l  # last wins
    return list(seen.values())


def _derive_roles(locs: List[Location]):
    """Split locations by category and compute relationships.

    Returns dict with keys: states, districts, towns. Each is a list of dict rows
    prepared for upsert SQL.
    """
    idx = build_hierarchy_indexes(locs)

    # Collect state IDs to help detect when a TOWN root is a STATE
    state_ids = {l.id for l in locs if _cat(l) == "STATE"}
    district_ids = {l.id for l in locs if _cat(l) == "DISTRICT"}

    states_rows = []
    districts_rows = []
    towns_rows = []

    for l in locs:
        cat = _cat(l)
        if cat == "STATE":
            states_rows.append({
                "id": l.id,
                "name": l.name,
            })
        elif cat == "DISTRICT":
            st_id, st_name = derive_state_for_location(l, idx)
            # Some datasets already have rootid as state id
            state_id = st_id or l.rootid
            districts_rows.append({
                "id": l.id,
                "name": l.name,
                "state_id": state_id,
            })
        elif cat == "TOWN":
            st_id, st_name = derive_state_for_location(l, idx)
            # Decide district: if TOWN.rootid is a district, use it; else None
            district_id = l.rootid if (l.rootid in district_ids) else None
            towns_rows.append({
                "id": l.id,
                "name": l.name,
                "latitude": l.lat,
                "longitude": l.lon,
                "state_id": st_id,
                "district_id": district_id,
            })
        else:
            # Other categories are ignored for normalized store
            continue

    return {
        "states": states_rows,
        "districts": districts_rows,
        "towns": towns_rows,
    }


def create_schema_if_needed() -> None:
    logger = logging.getLogger("locations_loader")
    logger.debug("Creating schema objects if not present...")
    with db.get_conn(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    logger.info("Schema ensured (tables, indexes, and view).")


def upsert_locations(rows: dict) -> None:
    logger = logging.getLogger("locations_loader")
    states_rows = rows.get("states", [])
    districts_rows = rows.get("districts", [])
    towns_rows = rows.get("towns", [])

    # Insert in dependency order: states -> districts -> towns
    with db.get_conn(autocommit=False) as conn:
        with conn.cursor() as cur:
            if states_rows:
                logger.info("Upserting %d states...", len(states_rows))
                cur.executemany(UPSERT_STATE_SQL, [(r["id"], r["name"]) for r in states_rows])
            if districts_rows:
                logger.info("Upserting %d districts...", len(districts_rows))
                cur.executemany(
                    UPSERT_DISTRICT_SQL,
                    [(r["id"], r["name"], r.get("state_id")) for r in districts_rows],
                )
            if towns_rows:
                logger.info("Upserting %d towns...", len(towns_rows))
                cur.executemany(
                    UPSERT_TOWN_SQL,
                    [
                        (
                            r["id"],
                            r["name"],
                            r.get("latitude"),
                            r.get("longitude"),
                            r.get("state_id"),
                            r.get("district_id"),
                        )
                        for r in towns_rows
                    ],
                )
        conn.commit()
    logger.info("Upsert committed.")


def _count_table_rows() -> Dict[str, int]:
    counts: Dict[str, int] = {"states": 0, "districts": 0, "towns": 0}
    try:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM public.states")
                counts["states"] = int(cur.fetchone()[0])
                cur.execute("SELECT count(*) FROM public.districts")
                counts["districts"] = int(cur.fetchone()[0])
                cur.execute("SELECT count(*) FROM public.towns")
                counts["towns"] = int(cur.fetchone()[0])
    except Exception:
        # If tables do not exist yet or connection fails, just return zeros
        pass
    return counts


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Load MET locations into PostgreSQL (states/districts/towns)")
    p.add_argument("--source", default="weather/locations", help="File, folder, or glob of CSV/JSON location indexes (default: weather/locations)")
    p.add_argument("--create-schema", action="store_true", help="Create tables and indexes if not exist")
    p.add_argument("--load", action="store_true", help="Load/Upsert data into DB")
    p.add_argument("--dry-run", action="store_true", help="Analyze files and print counts without touching DB")
    p.add_argument("--check", action="store_true", help="Only show row counts in DB and exit")
    p.add_argument("--verbose", action="store_true", help="Set log level to INFO")
    p.add_argument("--debug", action="store_true", help="Set log level to DEBUG (overrides --verbose)")
    args = p.parse_args(argv)

    # Configure logging
    level = logging.WARNING
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger("locations_loader")

    # Load locations from source
    logger.info("Source: %s", args.source)
    locs = load_locations(args.source)
    if not locs:
        logger.error("No locations loaded from source.")
        return 1
    locs = _dedupe_by_id(locs)
    roles = _derive_roles(locs)

    states_n = len(roles["states"]) if roles.get("states") else 0
    districts_n = len(roles["districts"]) if roles.get("districts") else 0
    towns_n = len(roles["towns"]) if roles.get("towns") else 0

    # Diagnostic: missing relationships
    missing_district_state = sum(1 for r in roles.get("districts", []) if not r.get("state_id"))
    missing_town_state = sum(1 for r in roles.get("towns", []) if not r.get("state_id"))
    missing_town_district = sum(1 for r in roles.get("towns", []) if not r.get("district_id"))

    logger.info(
        "Parsed: states=%d, districts=%d, towns=%d (districts without state=%d, towns without state=%d, towns without district=%d)",
        states_n,
        districts_n,
        towns_n,
        missing_district_state,
        missing_town_state,
        missing_town_district,
    )

    if args.check:
        logger.info("DB conn: %s", db.conninfo_str(mask_password=True))
        counts = _count_table_rows()
        print(f"DB counts -> states={counts['states']}, districts={counts['districts']}, towns={counts['towns']}")
        return 0

    if args.dry_run and not (args.create_schema or args.load):
        logger.info("Dry-run only: no DB changes will be made. Use --load to write to the database.")
        return 0

    if args.create_schema:
        logger.info("Creating schema (if not exists)...")
        create_schema_if_needed()
        logger.info("Schema ready.")

    if args.load:
        logger.info("DB conn: %s", db.conninfo_str(mask_password=True))
        before = _count_table_rows()
        logger.info(
            "Before upsert -> states=%d, districts=%d, towns=%d",
            before.get("states", 0), before.get("districts", 0), before.get("towns", 0)
        )
        logger.info("Upserting rows...")
        upsert_locations(roles)
        after = _count_table_rows()
        logger.info(
            "After upsert -> states=%d, districts=%d, towns=%d",
            after.get("states", 0), after.get("districts", 0), after.get("towns", 0)
        )
        print("Done.")
    else:
        logger.warning("--load not specified: no data was written to the database.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
