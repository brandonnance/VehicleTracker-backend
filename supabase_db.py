import json
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

DB_HOST = os.getenv("DB_HOST", "aws-1-us-east-2.pooler.supabase.com")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres.ksudmixptotaiynpvcvt")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password4GAME9988")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode=DB_SSLMODE,  # Supabase requires SSL
    )
    try:
        yield conn
    finally:
        conn.close()

def fetch_jobs():
    """
    Fetch all jobs (with coords) from the jobs table.
    Returns a list of dicts with id, job_code, name, lat, long
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                select id, job_code, name, latitude, longitude
                from public.jobs
                """
            )
            rows = cur.fetchall()
        return rows

   
def upsert_vehicle(
    external_id: str,
    source_system: str,
    name: Optional[str] = None,
    vtype: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """
    Ensure the vehicle exists and return its uuid.
    """
    sql = """
    insert into public.vehicles (external_id, source_system, name, type, description)
    values (%s, %s, %s, %s, %s)
    on conflict (external_id, source_system)
    do update set
      name = coalesce(excluded.name, public.vehicles.name),
      type = coalesce(excluded.type, public.vehicles.type),
      description = coalesce(excluded.description, public.vehicles.description)
    returning id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (external_id, source_system, name, vtype, description))
            row = cur.fetchone()
            if row:
                vehicle_id = row[0]
            else:
                vehicle_id = None
        conn.commit()
    return str(vehicle_id)


def get_job_id_by_code(job_code: str) -> Optional[str]:
    sql = "select id from public.jobs where job_code = %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_code,))
            row = cur.fetchone()
    return row[0] if row else None


def insert_vehicle_position(
    vehicle_id: str,
    job_id: Optional[str],
    lat: float,
    lon: float,
    heading: Optional[float],
    speed_kph: Optional[float],
    odometer_km: Optional[float],
    timestamp_utc: datetime,
    source_raw: Optional[Dict[str, Any]] = None,
) -> None:
    sql = """
    insert into public.vehicle_positions (
      vehicle_id, job_id, latitude, longitude, heading,
      speed_kph, odometer_km, timestamp_utc, source_raw
    )
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    source_json = json.dumps(source_raw) if source_raw is not None else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    vehicle_id,
                    job_id,
                    lat,
                    lon,
                    heading,
                    speed_kph,
                    odometer_km,
                    timestamp_utc,
                    source_json,
                ),
            )
        conn.commit()


def get_latest_positions() -> list[dict]:
    """
    Read from the latest_vehicle_positions view.
    """
    sql = """
    select
      vehicle_id,
      vehicle_name,
      vehicle_type,
      job_id,
      job_code,
      job_name,
      latitude,
      longitude,
      speed_kph,
      timestamp_utc
    from public.latest_vehicle_positions
    order by vehicle_name;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return list(rows) 

if __name__ == "__main__":
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select version();")
            row = cur.fetchone()
            print("Connected! Server version:", row[0] if row else None)