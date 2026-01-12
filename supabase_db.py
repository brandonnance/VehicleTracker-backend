import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

# Load .env file if present (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars

from supabase import create_client, Client

# ForeSyt Supabase connection via REST API
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Lazy-initialized client
_supabase_client: Optional[Client] = None


def get_client() -> Client:
    """Get or create the Supabase client."""
    global _supabase_client
    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment"
            )
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _supabase_client


def _ensure_datetime_utc(ts) -> datetime:
    """
    Convert various timestamp formats into a timezone-aware datetime in UTC.

    Handles:
      - datetime (returns as-is, ensuring tzinfo=UTC if missing)
      - int/float: Unix seconds or milliseconds since epoch
      - str: ISO8601 like '2025-11-13T19:26:28Z' or with offset
    """
    # Already a datetime
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    # Numeric: treat as unix seconds or ms
    if isinstance(ts, (int, float)):
        value = float(ts)
        # Heuristic: ms vs sec
        if value > 1e12:
            # milliseconds
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc)

    # String: try ISO8601
    if isinstance(ts, str):
        s = ts.strip()
        try:
            # Handle trailing Z
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            # Last resort: try interpreting as float seconds
            try:
                value = float(s)
                if value > 1e12:
                    value = value / 1000.0
                return datetime.fromtimestamp(value, tz=timezone.utc)
            except Exception:
                pass

    # Fallback: now()
    return datetime.now(timezone.utc)


def upsert_vehicle(
    organization_id: str,
    external_id: str,
    source_system: str,
    name: Optional[str] = None,
    vtype: Optional[str] = None,
    description: Optional[str] = None,
) -> Optional[str]:
    """
    Ensure the vehicle exists and return its uuid.
    Uses (organization_id, external_id, source_system) as the unique key.

    Returns None if the vehicle is soft-deleted (blocklisted), indicating
    it should be skipped during sync.
    """
    client = get_client()

    # Check if vehicle already exists and is soft-deleted
    existing = (
        client.table("vehicles")
        .select("id, is_deleted")
        .eq("organization_id", organization_id)
        .eq("external_id", external_id)
        .eq("source_system", source_system)
        .execute()
    )

    if existing.data and len(existing.data) > 0:
        vehicle = existing.data[0]
        if vehicle.get("is_deleted"):
            # Vehicle is soft-deleted, skip syncing it
            return None

    now = datetime.now(timezone.utc).isoformat()
    data = {
        "organization_id": organization_id,
        "external_id": external_id,
        "source_system": source_system,
        "name": name,
        "type": vtype,
        "description": description,
        "updated_at": now,
        "last_seen_at": now,  # Track when vehicle was last seen in sync
    }

    # Remove None values (let DB use defaults)
    data = {k: v for k, v in data.items() if v is not None}

    result = (
        client.table("vehicles")
        .upsert(data, on_conflict="organization_id,external_id,source_system")
        .execute()
    )

    if result.data and len(result.data) > 0:
        return str(result.data[0]["id"])

    # If upsert didn't return data, query for the vehicle
    query_result = (
        client.table("vehicles")
        .select("id")
        .eq("organization_id", organization_id)
        .eq("external_id", external_id)
        .eq("source_system", source_system)
        .single()
        .execute()
    )
    return str(query_result.data["id"])


def insert_position(
    organization_id: str,
    vehicle_id: str,
    lat: float,
    lon: float,
    heading: Optional[float],
    speed_kph: Optional[float],
    odometer_km: Optional[float],
    timestamp_utc,
    source_raw: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Insert or update the *latest* vehicle/equipment position.

    We keep exactly one row per vehicle_id (unique constraint).
    job_id is NOT set here - the refresh_vehicle_positions() function
    or the latest_vehicle_positions view handles job assignment.
    """
    client = get_client()
    ts_dt = _ensure_datetime_utc(timestamp_utc)

    data = {
        "organization_id": organization_id,
        "vehicle_id": vehicle_id,
        "latitude": lat,
        "longitude": lon,
        "heading": heading,
        "speed_kph": speed_kph,
        "odometer_km": odometer_km,
        "timestamp_utc": ts_dt.isoformat(),
        "source_raw": source_raw,
    }

    # Upsert based on vehicle_id unique constraint
    client.table("vehicle_positions").upsert(
        data, on_conflict="vehicle_id"
    ).execute()


def get_latest_positions(organization_id: Optional[str] = None) -> list[dict]:
    """
    Read from the latest_vehicle_positions view.
    Optionally filter by organization_id.
    """
    client = get_client()

    query = client.table("latest_vehicle_positions").select(
        "vehicle_id, vehicle_name, vehicle_type, external_id, source_system, "
        "job_id, job_code, job_name, job_latitude, job_longitude, "
        "latitude, longitude, speed_kph, heading, odometer_km, "
        "timestamp_utc, organization_id, distance_to_job_meters"
    )

    if organization_id:
        query = query.eq("organization_id", organization_id)

    query = query.order("vehicle_name")
    result = query.execute()

    return result.data if result.data else []


def delete_stale_vehicles(organization_id: str, stale_days: int = 7) -> int:
    """
    Delete vehicles not seen in any sync for the specified number of days.

    Args:
        organization_id: The organization to clean up
        stale_days: Number of days after which a vehicle is considered stale (default: 7)

    Returns:
        Number of vehicles deleted

    Note:
        vehicle_positions are automatically deleted via CASCADE constraint.
    """
    client = get_client()

    # Calculate cutoff timestamp
    cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_days)).isoformat()

    # First, get the vehicles to be deleted (for count and logging)
    stale_result = (
        client.table("vehicles")
        .select("id, name, external_id, source_system")
        .eq("organization_id", organization_id)
        .lt("last_seen_at", cutoff)
        .execute()
    )

    stale_vehicles = stale_result.data or []
    delete_count = len(stale_vehicles)

    if delete_count > 0:
        # Log which vehicles are being deleted
        for v in stale_vehicles:
            print(f"  Removing stale vehicle: {v['name']} ({v['source_system']}/{v['external_id']})")

        # Delete stale vehicles (positions auto-deleted via CASCADE)
        client.table("vehicles") \
            .delete() \
            .eq("organization_id", organization_id) \
            .lt("last_seen_at", cutoff) \
            .execute()

    return delete_count


def delete_vehicle(
    organization_id: str,
    vehicle_id: Optional[str] = None,
    external_id: Optional[str] = None,
    name: Optional[str] = None,
    source_system: Optional[str] = None,
) -> bool:
    """
    Delete a specific vehicle by ID, external_id, or name.

    Args:
        organization_id: The organization the vehicle belongs to
        vehicle_id: UUID of the vehicle (highest priority)
        external_id: External ID from source system (requires source_system)
        name: Vehicle name (least specific, may match multiple)
        source_system: Source system filter (required with external_id, optional with name)

    Returns:
        True if vehicle(s) were deleted, False if no matching vehicle found

    Raises:
        ValueError: If no identifier provided or invalid combination

    Note:
        vehicle_positions are automatically deleted via CASCADE constraint.
    """
    if not any([vehicle_id, external_id, name]):
        raise ValueError("Must provide at least one of: vehicle_id, external_id, or name")

    if external_id and not source_system:
        raise ValueError("source_system is required when using external_id")

    client = get_client()

    # Build query
    query = client.table("vehicles").delete().eq("organization_id", organization_id)

    if vehicle_id:
        # Most specific: delete by UUID
        query = query.eq("id", vehicle_id)
    elif external_id:
        # Delete by external_id + source_system
        query = query.eq("external_id", external_id).eq("source_system", source_system)
    else:
        # Delete by name (optionally filtered by source_system)
        query = query.eq("name", name)
        if source_system:
            query = query.eq("source_system", source_system)

    result = query.execute()

    # Check if any rows were deleted
    return result.data is not None and len(result.data) > 0


def soft_delete_vehicle(
    organization_id: str,
    vehicle_id: Optional[str] = None,
    external_id: Optional[str] = None,
    name: Optional[str] = None,
    source_system: Optional[str] = None,
) -> int:
    """
    Soft delete (blocklist) a vehicle by setting is_deleted=true.

    This prevents the vehicle from:
    - Appearing in the frontend (view excludes deleted vehicles)
    - Being re-created during sync (upsert checks is_deleted flag)

    Args:
        organization_id: The organization the vehicle belongs to
        vehicle_id: UUID of the vehicle (highest priority)
        external_id: External ID from source system (requires source_system)
        name: Vehicle name (least specific, may match multiple)
        source_system: Source system filter (required with external_id, optional with name)

    Returns:
        Number of vehicles soft-deleted

    Raises:
        ValueError: If no identifier provided or invalid combination
    """
    if not any([vehicle_id, external_id, name]):
        raise ValueError("Must provide at least one of: vehicle_id, external_id, or name")

    if external_id and not source_system:
        raise ValueError("source_system is required when using external_id")

    client = get_client()

    now = datetime.now(timezone.utc).isoformat()

    # Build query
    query = (
        client.table("vehicles")
        .update({"is_deleted": True, "deleted_at": now})
        .eq("organization_id", organization_id)
        .eq("is_deleted", False)  # Only update non-deleted vehicles
    )

    if vehicle_id:
        query = query.eq("id", vehicle_id)
    elif external_id:
        query = query.eq("external_id", external_id).eq("source_system", source_system)
    else:
        query = query.eq("name", name)
        if source_system:
            query = query.eq("source_system", source_system)

    result = query.execute()

    return len(result.data) if result.data else 0


def restore_vehicle(
    organization_id: str,
    vehicle_id: Optional[str] = None,
    external_id: Optional[str] = None,
    name: Optional[str] = None,
    source_system: Optional[str] = None,
) -> int:
    """
    Restore a soft-deleted vehicle by setting is_deleted=false.

    Args:
        organization_id: The organization the vehicle belongs to
        vehicle_id: UUID of the vehicle (highest priority)
        external_id: External ID from source system (requires source_system)
        name: Vehicle name
        source_system: Source system filter

    Returns:
        Number of vehicles restored
    """
    if not any([vehicle_id, external_id, name]):
        raise ValueError("Must provide at least one of: vehicle_id, external_id, or name")

    if external_id and not source_system:
        raise ValueError("source_system is required when using external_id")

    client = get_client()

    # Build query
    query = (
        client.table("vehicles")
        .update({"is_deleted": False, "deleted_at": None})
        .eq("organization_id", organization_id)
        .eq("is_deleted", True)  # Only update deleted vehicles
    )

    if vehicle_id:
        query = query.eq("id", vehicle_id)
    elif external_id:
        query = query.eq("external_id", external_id).eq("source_system", source_system)
    else:
        query = query.eq("name", name)
        if source_system:
            query = query.eq("source_system", source_system)

    result = query.execute()

    return len(result.data) if result.data else 0


if __name__ == "__main__":
    # Test connection
    try:
        client = get_client()
        # Try a simple query
        result = client.table("vehicles").select("id").limit(1).execute()
        print(f"Connected! Supabase URL: {SUPABASE_URL}")
        print(f"Vehicles table accessible: {result.data is not None}")
    except Exception as e:
        print(f"Connection failed: {e}")
