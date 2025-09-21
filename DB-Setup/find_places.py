r"""Query helper for normalized places tables (states, districts, towns).

Examples (PowerShell):
  # Find towns matching text (any state)
    .venv\Scripts\python.exe find_places.py --town "putra" --limit 10

  # Find towns by state name (partial) and town name (partial)
    .venv\Scripts\python.exe find_places.py --town "jaya" --state "selangor"

  # List districts within a state
    .venv\Scripts\python.exe find_places.py --district "*" --state "johor"
"""
from __future__ import annotations

import argparse
from typing import List, Optional, Tuple

import db


def _like_param(text: str) -> str:
    return f"%{text.lower()}%"


def query_states(name_like: Optional[str]) -> List[Tuple[str, str]]:
    sql = "SELECT id, name FROM public.states"
    params: list = []
    if name_like:
        sql += " WHERE lower(name) LIKE %s"
        params.append(_like_param(name_like))
    sql += " ORDER BY name"
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [(r[0], r[1]) for r in cur.fetchall()]


def query_districts(name_like: Optional[str], state_like: Optional[str]) -> List[Tuple[str, str, str]]:
    sql = """
        SELECT d.id, d.name, s.name as state
        FROM public.districts d
        JOIN public.states s ON s.id = d.state_id
    """
    where = []
    params: list = []
    if name_like and name_like != "*":
        where.append("lower(d.name) LIKE %s")
        params.append(_like_param(name_like))
    if state_like:
        where.append("lower(s.name) LIKE %s")
        params.append(_like_param(state_like))
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY s.name, d.name"
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def query_towns(name_like: Optional[str], state_like: Optional[str], limit: int = 50):
    sql = """
        SELECT t.id, t.name, t.latitude, t.longitude, s.name as state, d.name as district
        FROM public.towns t
        LEFT JOIN public.states s ON s.id = t.state_id
        LEFT JOIN public.districts d ON d.id = t.district_id
    """
    where = []
    params: list = []
    if name_like and name_like != "*":
        where.append("lower(t.name) LIKE %s")
        params.append(_like_param(name_like))
    if state_like:
        where.append("lower(s.name) LIKE %s")
        params.append(_like_param(state_like))
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY s.name, d.name NULLS LAST, t.name"
    sql += " LIMIT %s"
    params.append(limit)
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Find towns/districts/states in PostgreSQL")
    p.add_argument("--state", help="State name filter (partial, case-insensitive)")
    p.add_argument("--town", help="Town name filter (partial, case-insensitive). Use * to list all (with optional --state)")
    p.add_argument("--district", help="District name filter (partial, case-insensitive). Use * to list all (with optional --state)")
    p.add_argument("--states", action="store_true", help="List states (optionally filter by --state)")
    p.add_argument("--limit", type=int, default=50, help="Limit results for towns (default 50)")
    args = p.parse_args(argv)

    if args.states:
        rows = query_states(args.state)
        for rid, name in rows:
            print(f"{rid}, {name}")
        return 0

    if args.district:
        rows = query_districts(args.district, args.state)
        for rid, name, state in rows:
            print(f"{rid}, {name}, {state}")
        return 0

    if args.town:
        rows = query_towns(args.town, args.state, limit=args.limit)
        for rid, name, lat, lon, state, district in rows:
            lat_s = "" if lat is None else f"{lat:.6f}"
            lon_s = "" if lon is None else f"{lon:.6f}"
            print(f"{rid}, {name}, {lat_s}, {lon_s}, {state or ''}, {district or ''}")
        return 0

    p.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
