# app.py
import os
import math
import pandas as pd
import numpy as np
import streamlit as st
import pydeck as pdk


# ---------- CONFIG ----------
os.environ["MAPBOX_API_KEY"] = "pk.eyJ1IjoiYnJhbmRvbm5hbmNlIiwiYSI6ImNtaHdmNG52ZjA1c2Iya3B2MmQ4ZHZlM2IifQ.8A-1uK_195w6igptwfkRZA"
DATA_DIR = r"C:\Users\Brandon\Documents\DEV\VehicleTracker"
VEHICLE_CSV = os.path.join(DATA_DIR, "data.csv")  # from your Samsara fetcher
JOBS_CSV    = os.path.join(DATA_DIR, "jobs.csv")  # your curated jobs file

# ---------- HELPERS ----------
def haversine_miles(lat1, lon1, lat2, lon2):
    """
    Vectorized Haversine distance in miles.
    lat/lon can be numpy arrays/pandas Series of equal shape.
    """
    R = 3958.7613  # Earth radius in miles
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def latest_vehicle_positions(df):
    """
    Keep only the most recent ping per vehicleId.
    Expects columns: vehicleId, timestamp, latitude, longitude, odometer
    """
    # Convert timestamp; infer format and coerce errors
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    # Drop rows without coordinates or timestamp
    df = df.dropna(subset=["timestamp", "latitude", "longitude"])
    # Sort by timestamp descending, then keep first per vehicleId
    df = df.sort_values("timestamp", ascending=False)
    latest = df.groupby("vehicleId", as_index=False).first()
    return latest

def assign_to_jobs(vehicles_latest, jobs, threshold_miles):
    """
    For each vehicle, find nearest job and distance.
    If distance > threshold, bucket as 'Other'.
    Returns a DataFrame with assignment columns.
    """
    v = vehicles_latest.copy()
    # Prepare arrays
    job_lats = jobs["latitude"].to_numpy()
    job_lons = jobs["longitude"].to_numpy()
    job_ids  = jobs["job_id"].to_numpy()
    job_names = jobs["job_name"].to_numpy()

    # For each vehicle, compute distance to all jobs and pick min
    nearest_job_id = []
    nearest_job_name = []
    nearest_dist_mi = []

    for _, row in v.iterrows():
        dists = haversine_miles(row["latitude"], row["longitude"], job_lats, job_lons)
        idx = int(np.argmin(dists))
        nearest_job_id.append(job_ids[idx])
        nearest_job_name.append(job_names[idx])
        nearest_dist_mi.append(float(dists[idx]))

    v["nearest_job_id"] = nearest_job_id
    v["nearest_job_name"] = nearest_job_name
    v["nearest_distance_mi"] = np.round(nearest_dist_mi, 3)
    v["assigned_bucket"] = np.where(v["nearest_distance_mi"] <= threshold_miles,
                                    v["nearest_job_name"], "Other")
    return v

def load_data():
    jobs = pd.read_csv(JOBS_CSV, dtype={"job_id": "string"})
    vehicles = pd.read_csv(VEHICLE_CSV)
    return jobs, vehicles

# ---------- UI ----------
st.set_page_config(page_title="GAME Inc • Vehicle → Project Locator", layout="wide")

st.title("🚚 Vehicle → Project Locator")
st.caption("Matches latest vehicle GPS to the closest project. Vehicles farther than a threshold go to **Other**.")

# Controls
with st.sidebar:
    st.header("Controls")
    threshold_miles = st.number_input("Distance threshold (miles)", min_value=0.1, max_value=50.0, value=2.0, step=0.1)
    st.write("Files:")
    st.code(f"Vehicles: {VEHICLE_CSV}\nJobs:     {JOBS_CSV}")
    st.info("Tip: schedule your Samsara fetcher to overwrite `data.csv` on your cadence (midnight, etc.). "
            "Reload this app to pick up new data.")

# Load data
try:
    jobs, vehicles = load_data()
except FileNotFoundError as e:
    st.error(f"Missing file: {e}")
    st.stop()
except Exception as e:
    st.error(f"Could not load data: {e}")
    st.stop()

# Basic validation
required_vehicle_cols = {"vehicleId", "vehicleName", "timestamp", "latitude", "longitude"}
missing_vehicle = required_vehicle_cols - set(vehicles.columns)
if missing_vehicle:
    st.error(f"`data.csv` missing required columns: {missing_vehicle}")
    st.stop()

required_job_cols = {"job_id", "job_name", "latitude", "longitude"}
missing_jobs = required_job_cols - set(jobs.columns)
if missing_jobs:
    st.error(f"`jobs.csv` missing required columns: {missing_jobs}")
    st.stop()

# Keep only latest ping per vehicle
vehicles_latest = latest_vehicle_positions(vehicles)

# Assign
assigned = assign_to_jobs(vehicles_latest, jobs, threshold_miles)

# Summary
counts = assigned["assigned_bucket"].value_counts().rename_axis("Job").reset_index(name="# Vehicles")
left, right = st.columns([1,2])
with left:
    st.subheader("Summary")
    st.dataframe(counts, use_container_width=True)
with right:
    st.subheader("Latest Vehicle Positions & Assignments")
    show_cols = ["vehicleName", "latitude", "longitude",
                 "nearest_job_name", "nearest_distance_mi", "assigned_bucket"]
    
    #  Rename columns
    # Rename them for display
    renamed = assigned[show_cols].rename(columns={
        "vehicleName": "Vehicle",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "nearest_job_name": "Nearest Project",
        "nearest_distance_mi": "Distance (mi)",
        "assigned_bucket": "Assigned Project"
    })

    # Sort and display
    st.dataframe(
        renamed.sort_values(["Assigned Project", "Vehicle"]).reset_index(drop=True),
        use_container_width=True
    )

# Map (jobs + vehicles)
st.subheader("Map")
if not assigned.empty and not jobs.empty:
    # Center map around mean of all points (jobs + vehicles)
    all_lats = pd.concat([assigned["latitude"], jobs["latitude"]])
    all_lons = pd.concat([assigned["longitude"], jobs["longitude"]])
    center_lat = all_lats.mean()
    center_lon = all_lons.mean()

    # ---- Jobs (records + 'job' field) ----
    jobs_plot = jobs.rename(columns={"latitude": "lat", "longitude": "lon"}).copy()
    jobs_plot["job"] = jobs_plot["job_name"].astype(str)
    job_records = jobs_plot.to_dict(orient="records")

    job_layer = pdk.Layer(
        "ScatterplotLayer",
        data=job_records,
        get_position="[lon, lat]",
        get_radius=50,
        pickable=True,
        auto_highlight=True,
        radius_min_pixels=6,
        radius_max_pixels=10,
        get_fill_color=[0, 122, 255, 200],
    )

    # ---- Vehicles (records + 'vehicle' field + color) ----
    veh_plot = assigned.rename(columns={"latitude":"lat","longitude":"lon"}).copy()
    NAME_COL = "vehicleName" if "vehicleName" in veh_plot.columns else "vehicleId"
    veh_plot["vehicle"] = veh_plot[NAME_COL].fillna("").astype(str)

    cond = veh_plot["assigned_bucket"].eq("Other").to_numpy()
    colors = np.where(
        cond[:, None],
        np.array([220, 53, 69, 200], dtype=np.int32),
        np.array([40, 167, 69, 200], dtype=np.int32)
    )
    veh_plot["color"] = colors.tolist()

    veh_records = veh_plot.replace({np.nan: None}).to_dict(orient="records")

    veh_layer = pdk.Layer(
        "ScatterplotLayer",
        data=veh_records,
        get_position="[lon, lat]",
        get_radius=40,
        pickable=True,
        auto_highlight=True,
        radius_min_pixels=5,
        radius_max_pixels=8,
        get_fill_color="color",
    )

    # ---- Tooltips: reference top-level fields directly ----
    tooltip = {
        "html": (
            "<b>Vehicle:</b> {vehicle}<br/>"
            "<b>Bucket:</b> {assigned_bucket}<br/>"
            "<b>Nearest:</b> {nearest_job_name} ({nearest_distance_mi} mi)"
            "<br/><b>Job:</b> {job}"  # will render on job points; harmless on vehicles
        ),
        "style": {"backgroundColor": "rgba(0,0,0,0.85)", "color": "white"}
    }

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/satellite-streets-v12",
        initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=9),
        layers=[job_layer, veh_layer],
        tooltip=tooltip,  # dict is valid per docs
    ))
else:
    st.info("No data to display on the map yet.")

# Per-job drilldown
st.subheader("Per-Project Breakdown")

# Ensure string job_id and a handy vehicle display column
jobs_sorted = jobs.assign(job_id=jobs["job_id"].astype("string")).sort_values("job_id")
NAME_COL = "vehicleName" if "vehicleName" in assigned.columns else "vehicleId"

# Split assigned into non-Other and Other
assigned_non_other = assigned[assigned["assigned_bucket"] != "Other"].copy()
assigned_non_other["nearest_job_id"] = assigned_non_other["nearest_job_id"].astype("string")

# Build a dict of vehicle subsets per job_id (fast lookups)
by_job = {
    jid: assigned_non_other[assigned_non_other["nearest_job_id"] == jid].copy()
    for jid in jobs_sorted["job_id"]
}

# Group 1: projects with vehicles (sorted by job_id)
with_vehicles = [
    (jid,
     jobs_sorted.loc[jobs_sorted["job_id"] == jid, "job_name"].iloc[0],
     df)
    for jid, df in by_job.items() if len(df) > 0
]
with_vehicles.sort(key=lambda x: x[0])  # sort by job_id

# Group 3: projects with zero vehicles (keep jobs_sorted order)
zero_vehicles = [
    (row.job_id, row.job_name)
    for row in jobs_sorted.itertuples(index=False)
    if len(by_job[row.job_id]) == 0
]

# ---------- Render ----------
# 1) Projects WITH vehicles
if with_vehicles:
    st.markdown("### Projects with vehicles")
    for jid, jname, df in with_vehicles:
        df["vehicle"] = df[NAME_COL].astype(str)
        with st.expander(f"{jid} — {jname}  •  {len(df)} vehicle(s)"):
            st.dataframe(
                df[["vehicle", "nearest_distance_mi"]]
                  .sort_values(["vehicle", "nearest_distance_mi"], ascending=[True, False]),
                use_container_width=True
            )
else:
    st.markdown("### Projects with vehicles")
    st.caption("No vehicles currently assigned to any project.")

# 2) OTHER bucket in the middle
subset_other = assigned[assigned["assigned_bucket"] == "Other"].copy()
if not subset_other.empty:
    subset_other["vehicle"] = subset_other[NAME_COL].astype(str)
    with st.expander(f"Other — {len(subset_other)} vehicle(s)"):
        st.dataframe(
            subset_other[["vehicle", "timestamp", "nearest_job_name", "nearest_distance_mi"]]
              .sort_values(["vehicle", "timestamp"], ascending=[True, False]),
            use_container_width=True
        )

# 3) Projects with ZERO vehicles (sorted by job_id)
if zero_vehicles:
    st.markdown("### Projects with 0 vehicles")
    for jid, jname in zero_vehicles:
        st.write(f"{jid} — {jname}  •  0 vehicle(s)")
