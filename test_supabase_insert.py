from datetime import datetime, timezone

from supabase_db import (
    upsert_vehicle,
    get_job_id_by_code,
    insert_vehicle_position,
    get_latest_positions,
)

def main():
    # 1) Example data (swap these to match what you actually have in jobs/vehicles)
    samsara_vehicle_id = "TRUCK123"
    vehicle_name = "Truck 123"
    vehicle_type = "dump_truck"
    job_code = "25-001"  # must exist in public.jobs for this to resolve

    lat = 46.2005
    lon = -119.1495
    speed_kph = 0.0
    odometer_km = 12000.0
    heading = 90.0
    timestamp_utc = datetime.now(timezone.utc)
    raw_payload = {"source": "test-script", "note": "hello from python"}

    # 2) Ensure vehicle exists
    vehicle_id = upsert_vehicle(
        external_id=samsara_vehicle_id,
        source_system="samsara",
        name=vehicle_name,
        vtype=vehicle_type,
    )
    print("Vehicle ID:", vehicle_id)

    # 3) Look up job_id (optional)
    job_id = get_job_id_by_code(job_code)
    print("Job ID:", job_id)

    # 4) Insert a new position
    insert_vehicle_position(
        vehicle_id=vehicle_id,
        job_id=job_id,
        lat=lat,
        lon=lon,
        heading=heading,
        speed_kph=speed_kph,
        odometer_km=odometer_km,
        timestamp_utc=timestamp_utc,
        source_raw=raw_payload,
    )
    print("Inserted position.")

    # 5) Read back latest positions
    rows = get_latest_positions()
    print("Latest positions:")
    for r in rows:
        print(r)


if __name__ == "__main__":
    main()
