from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import math

from dateutil import parser as dt_parser  # pip install python-dateutil

from samsara_client import fetch_vehicle_gps_stats, normalize_vehicle_record

from supabase_db import (
    upsert_vehicle,
    # get_job_id_by_code,
    insert_vehicle_position,
    fetch_jobs,
)

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute great-circle distance between two (lat, lon) points in kilometers.
    """
    R = 6371.0  # Earth radius in km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2
    ) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def find_nearest_job(
    vehicle_lat: float,
    vehicle_lon: float,
    jobs: List[Dict[str, Any]],
    max_distance_km: float = 1.0,  # tweak radius as needed
) -> Optional[Dict[str, Any]]:
    """
    Given a vehicle lat/lon and a list of jobs (with lat/lon),
    return the nearest job if it is within max_distance_km, else None.
    """
    best_job = None
    best_distance = None

    for job in jobs:
        jlat = job.get("latitude")
        jlon = job.get("longitude")
        if jlat is None or jlon is None:
            continue

        d = haversine_km(float(vehicle_lat), float(vehicle_lon), float(jlat), float(jlon))

        if best_distance is None or d < best_distance:
            best_distance = d
            best_job = job

    if best_job is not None and best_distance is not None and best_distance <= max_distance_km:
        return best_job

    return None


def parse_timestamp(ts: str) -> datetime:
    """
    Samsara uses RFC 3339 timestamps; dateutil handles these well.
    """
    dt = dt_parser.isoparse(ts)
    # Ensure it's timezone-aware; if Samsara sends Z, it already is UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def map_to_job_code(record) -> Optional[str]:
    """
    Placeholder for mapping GPS position to a job_code like '25-052'.

    Right now, returns None (no job). Later you can:
      - map based on Samsara tags
      - map based on geofence / proximity to known job lat/lon
      - or use a lookup table.
    """
    # Example stub:
    # if record["vehicle_name"].startswith("T33"):
    #     return "25-052"
    return None


def run_sync_once():
    raw_items = fetch_vehicle_gps_stats()
    print(f"Fetched {len(raw_items)} items from Samsara.")

    # Fetch jobs once per sync and keep only those with coordinates
    jobs = fetch_jobs()
    jobs_with_coords = [
        j
        for j in jobs
        if j.get("latitude") is not None and j.get("longitude") is not None
    ]
    print(f"Loaded {len(jobs_with_coords)} jobs with coordinates.")

    inserted_count = 0
    normalized_count = 0
    missing_latlon_count = 0
    skipped_norm_count = 0

    for idx, raw in enumerate(raw_items):
        norm = normalize_vehicle_record(raw)
        if not norm:
            skipped_norm_count += 1
            if idx < 3:
                print(f"[DEBUG] Item {idx}: normalize_vehicle_record returned None. Keys: {list(raw.keys())}")
            continue

        normalized_count += 1

        if norm["latitude"] is None or norm["longitude"] is None:
            missing_latlon_count += 1
            if idx < 3:
                print(f"[DEBUG] Item {idx}: latitude/longitude missing in normalized record: {norm}")
            continue

        # 1) Ensure vehicle exists in vehicles table
        vehicle_id = upsert_vehicle(
            external_id=norm["external_id"],
            source_system=norm["source_system"],
            name=norm["name"],
            vtype=norm["vehicle_type"],
        )

        # 2) Find nearest job by distance, if any
        nearest_job = None
        if jobs_with_coords:
            nearest_job = find_nearest_job(
                norm["latitude"],
                norm["longitude"],
                jobs_with_coords,
                max_distance_km=1.0,  # tweak this radius as desired
            )

        job_id = nearest_job["id"] if nearest_job else None

        # 3) Parse timestamp
        ts = norm["timestamp_utc"]
        if isinstance(ts, str):
            ts_dt = parse_timestamp(ts)
        else:
            ts_dt = ts

        # 4) Insert position
        insert_vehicle_position(
            vehicle_id=vehicle_id,
            job_id=job_id,
            lat=float(norm["latitude"]),
            lon=float(norm["longitude"]),
            heading=float(norm["heading"]) if norm["heading"] is not None else None,
            speed_kph=float(norm["speed_kph"]) if norm["speed_kph"] is not None else None,
            odometer_km=float(norm["odometer_km"]) if norm["odometer_km"] is not None else None,
            timestamp_utc=ts_dt,
            source_raw=norm["raw"],
        )
        inserted_count += 1

    print(f"Normalized: {normalized_count}")
    print(f"Skipped (normalize returned None): {skipped_norm_count}")
    print(f"Skipped (missing lat/lon): {missing_latlon_count}")
    print(f"Inserted {inserted_count} positions into Supabase.")


if __name__ == "__main__":
    run_sync_once()
