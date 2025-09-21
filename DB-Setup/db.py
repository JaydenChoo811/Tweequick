"""
Simple PostgreSQL connection helper using psycopg (v3).

Environment variables are loaded from a local .env file automatically (if present).
Variables used (with defaults):
- PGHOST (default: "localhost")
- PGPORT (default: "5432")
- PGUSER (default: "postgres")
- PGPASSWORD (default: "")
- PGDATABASE (default: "postgres")
- PGSSLMODE (default: "disable")  # set to "require" for many cloud providers

Optional SSL variables (if provided they will be passed through):
- PGSSLROOTCERT -> sslrootcert (path to CA bundle, e.g., rds-combined-ca-bundle.pem)
- PGSSLCERT     -> sslcert (client cert, rarely needed)
- PGSSLKEY      -> sslkey (client key, rarely needed)
- PGSSLPASSWORD -> sslpassword (if key is encrypted)

Usage:
    from db import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print(cur.fetchone())

To quickly verify configuration without connecting:
    import db
    print(db.conninfo_str(mask_password=True))

To run a smoke test that actually connects, set env DB_TEST_CONNECT=1:
    PowerShell:
        $env:DB_TEST_CONNECT = "1"; py db_smoke_test.py
"""
from __future__ import annotations

import os
from typing import Dict, Any

import psycopg
from dotenv import load_dotenv

# Load .env on import (safe no-op if file is missing)
try:
    load_dotenv()
except Exception:
    pass


def _env(key: str, default: str | None = None) -> str | None:
    return os.getenv(key, default)


def get_env_conn_params() -> Dict[str, str]:
    """Return libpq/psycopg connection parameters from environment.

    Keys match psycopg.connect kwargs (libpq), e.g., host, port, user, password, dbname, sslmode.
    """
    params: Dict[str, str] = {
        "host": _env("PGHOST", "localhost") or "localhost",
        "port": _env("PGPORT", "5432") or "5432",
        "user": _env("PGUSER", "postgres") or "postgres",
        "password": _env("PGPASSWORD", "") or "",
        "dbname": _env("PGDATABASE", "postgres") or "postgres",
        "sslmode": _env("PGSSLMODE", "disable") or "disable",
    }

    # Optional SSL params
    sslrootcert = _env("PGSSLROOTCERT")
    if sslrootcert:
        params["sslrootcert"] = sslrootcert
    sslcert = _env("PGSSLCERT")
    if sslcert:
        params["sslcert"] = sslcert
    sslkey = _env("PGSSLKEY")
    if sslkey:
        params["sslkey"] = sslkey
    sslpassword = _env("PGSSLPASSWORD")
    if sslpassword:
        params["sslpassword"] = sslpassword

    return params


def conninfo_str(mask_password: bool = True, **overrides: Any) -> str:
    """Build a DSN-like string for display/logging.

    Warning: with mask_password=False this will include the raw password.
    """
    params = get_env_conn_params()
    for k, v in overrides.items():
        if v is not None:
            params[str(k)] = str(v)

    pw = params.get("password", "")
    pw_display = ("***" if pw else "") if mask_password else pw
    parts = [
        f"host={params.get('host','')}",
        f"port={params.get('port','')}",
        f"user={params.get('user','')}",
        f"password={pw_display}",
        f"dbname={params.get('dbname','')}",
        f"sslmode={params.get('sslmode','')}",
    ]
    return " ".join(parts)


def get_conn(autocommit: bool = False, **overrides: Any) -> psycopg.Connection:
    """Create and return a psycopg connection.

    Args:
        autocommit: If True, set connection autocommit mode.
        **overrides: Optional connection parameter overrides (host, port, user, password, dbname, sslmode, etc.).
    """
    params: Dict[str, Any] = get_env_conn_params()
    for k, v in overrides.items():
        if v is not None:
            params[str(k)] = v

    conn = psycopg.connect(**params)
    if autocommit:
        conn.autocommit = True
    return conn


if __name__ == "__main__":
    # When run directly: connect if DB_TEST_CONNECT=1, else print sanitized conninfo.
    if os.getenv("DB_TEST_CONNECT") == "1":
        try:
            with get_conn(autocommit=True) as conn:  # autocommit for simple probes
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    ver = cur.fetchone()[0]
                    print("Connected. Server:", ver)
        except Exception as e:
            print("Connection failed:", e)
    else:
        print(conninfo_str(mask_password=True))
