
"""
Smart City Sensor Data Analytics Dashboard

Features:
- KPI cards (Avg PM2.5, Worst Area, Peak Hour, Noisiest Area, Abnormal Count)
- Area-wise Pollution vs Noise (side-by-side charts)
- Traffic Density (Avg by hour)
- Abnormal Sensor Readings (Top 20, highlighted)

Run:
    streamlit run dashboard/app.py

Environment variables:
    MONGODB_URI (default: mongodb://localhost:27017)
"""


import os
import sys
import uuid
from datetime import datetime, time as dtime, date as ddate
from pathlib import Path
import streamlit as st
import pandas as pd
import altair as alt
from pymongo import MongoClient


# Ensure the project root is on sys.path so 'analytics' can be imported
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analytics.analysis import (
    area_wise_avg_pm25,
    traffic_density_by_hour,
    avg_noise_by_area,
    abnormal_readings,
)

# Streamlit 1.39+ may wrap DataFrames (Narwhals). Convert to native pandas to avoid Altair warnings.
def _to_native_df(df):
    return df.to_native() if hasattr(df, "to_native") else df

def _to_alt_values(df):
    df = _to_native_df(df)
    return df.to_dict(orient="records")

DB_NAME = "smart_city"
COLLECTION_NAME = "sensor_data"


st.set_page_config(page_title="Smart City Analytics", page_icon="📊", layout="wide")
st.title("Smart City Sensor Data Analytics")



@st.cache_resource(show_spinner=False)
def _get_client(uri: str):
    return MongoClient(uri)

def get_collection():
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    if "<cluster>" in uri or "<user>" in uri or "<pass>" in uri:
        st.error("MONGODB_URI contains placeholders. Please set a valid connection string.")
        st.stop()
    client = _get_client(uri)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


coll = get_collection()



@st.cache_data(ttl=60, show_spinner=False)
def _get_distinct_areas(_collection):
    return sorted(_collection.distinct("area"))


# Sidebar filters

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    areas = _get_distinct_areas(coll)
    selected_areas = st.multiselect("Areas", options=areas, default=areas)
    st.divider()
    safe_limit = st.slider(
        "PM2.5 Safe Limit", min_value=15, max_value=150, value=60, step=5,
        help="Reference line on the PM2.5 chart"
    )
    # City selection removed (rollback)

# Filtered collection via $in
filter_query = {"area": {"$in": selected_areas}} if selected_areas else {}



def _match_filter(base_query):
    return [{"$match": base_query}] if base_query else []



# KPI helpers (respect filters)
def get_avg_pm25():
    pipeline = _match_filter(filter_query) + [
        {"$group": {"_id": None, "avg": {"$avg": "$pm25"}}},
        {"$project": {"_id": 0, "avg": {"$round": ["$avg", 2]}}},
    ]
    res = list(coll.aggregate(pipeline))
    return res[0]["avg"] if res else None

def get_worst_pollution_area():
    pipeline = _match_filter(filter_query) + [
        {"$group": {"_id": "$area", "avg_pm25": {"$avg": "$pm25"}}},
        {"$project": {"_id": 0, "area": "$_id", "avg_pm25": {"$round": ["$avg_pm25", 2]}}},
        {"$sort": {"avg_pm25": -1}},
        {"$limit": 1},
    ]
    res = list(coll.aggregate(pipeline))
    return (res[0]["area"], res[0]["avg_pm25"]) if res else (None, None)

def get_peak_traffic_hour():
    pipeline = _match_filter(filter_query) + [
        {"$addFields": {"ts": {"$toDate": "$timestamp"}}},
        {"$group": {"_id": {"hour": {"$hour": "$ts"}}, "avg_traffic": {"$avg": "$traffic_count"}}},
        {"$project": {"_id": 0, "hour": "$_id.hour", "avg_traffic": {"$round": ["$avg_traffic", 2]}}},
        {"$sort": {"avg_traffic": -1}},
        {"$limit": 1},
    ]
    res = list(coll.aggregate(pipeline))
    return (res[0]["hour"], res[0]["avg_traffic"]) if res else (None, None)

def get_noisiest_area():
    pipeline = _match_filter(filter_query) + [
        {"$group": {"_id": "$area", "avg_noise": {"$avg": "$noise_db"}}},
        {"$project": {"_id": 0, "area": "$_id", "avg_noise": {"$round": ["$avg_noise", 2]}}},
        {"$sort": {"avg_noise": -1}},
        {"$limit": 1},
    ]
    res = list(coll.aggregate(pipeline))
    return (res[0]["area"], res[0]["avg_noise"]) if res else (None, None)

def get_abnormal_count():
    match = {"$or": [
        {"noise_db": {"$gt": 85}},
        {"traffic_count": {"$gt": 150}},
        {"pm25": {"$gt": 100}},
    ]}
    if filter_query:
        match.update(filter_query)
    pipeline = [
        {"$match": match},
        {"$count": "count"}
    ]
    res = list(coll.aggregate(pipeline))
    return res[0]["count"] if res else 0


# KPI row
avg_pm = get_avg_pm25()
worst_area, worst_pm = get_worst_pollution_area()
peak_hour, peak_traffic = get_peak_traffic_hour()
noisiest_area, noisiest_val = get_noisiest_area()
abnormal_cnt = get_abnormal_count()


# KPI row
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Avg PM2.5", f"{avg_pm if avg_pm is not None else 'N/A'}")
with c2:
    st.metric("Worst Pollution Area", worst_area or "N/A", help=f"Avg PM2.5: {worst_pm}" if worst_pm else None)
with c3:
    st.metric("Peak Traffic Hour", f"{peak_hour:02d}:00" if peak_hour is not None else "N/A", help=f"Avg Count: {peak_traffic}" if peak_traffic else None)
with c4:
    st.metric("Noisiest Area", noisiest_area or "N/A", help=f"Avg dB: {noisiest_val}" if noisiest_val else None)
with c5:
    st.metric("Abnormal Events", abnormal_cnt)

# Charts row: Pollution vs Noise side-by-side

# Charts row: Pollution vs Noise side-by-side
st.subheader("1️⃣ Area-wise Pollution Analysis vs Noise Hotspots")
col_left, col_right = st.columns(2)

area_pm = list(coll.aggregate([
    {"$match": filter_query},
    {"$group": {"_id": "$area", "avg_pm25": {"$avg": "$pm25"}}},
    {"$project": {"_id": 0, "area": "$_id", "avg_pm25": {"$round": ["$avg_pm25", 2]}}},
]))
noise_by_area = list(coll.aggregate([
    {"$match": filter_query},
    {"$group": {"_id": "$area", "avg_noise": {"$avg": "$noise_db"}}},
    {"$project": {"_id": 0, "area": "$_id", "avg_noise": {"$round": ["$avg_noise", 2]}}},
]))

with col_left:
    st.markdown("**Area-wise Pollution (Avg PM2.5)**")
    if area_pm:
        df_pm = pd.DataFrame(area_pm).sort_values("avg_pm25", ascending=False)
        df_pm = _to_native_df(df_pm)
        pm_chart = alt.Chart(alt.Data(values=_to_alt_values(df_pm))).mark_bar().encode(
            x=alt.X("area:N", sort='-y', title="Area"),
            y=alt.Y("avg_pm25:Q", title="Avg PM2.5"),
            tooltip=[alt.Tooltip("area:N"), alt.Tooltip("avg_pm25:Q")],
        ).properties(height=400)
        limit_rule = alt.Chart(alt.Data(values=[{"safe_limit": safe_limit}])).mark_rule(color="red").encode(
            y="safe_limit:Q"
        )
        st.altair_chart(pm_chart + limit_rule, use_container_width=True)
        st.download_button(
            "Download PM2.5 by Area (CSV)",
            data=df_pm.to_csv(index=False).encode("utf-8"),
            file_name="area_pm25.csv",
            mime="text/csv",
        )
    else:
        st.info("No data for selected filters.")

with col_right:
    st.markdown("**Noise Pollution Hotspots (Avg dB)**")
    if noise_by_area:
        df_noise = pd.DataFrame(noise_by_area).sort_values("avg_noise", ascending=False)
        df_noise = _to_native_df(df_noise)
        noise_chart = alt.Chart(alt.Data(values=_to_alt_values(df_noise))).mark_bar(color="#6c8ebf").encode(
            x=alt.X("area:N", sort='-y', title="Area"),
            y=alt.Y("avg_noise:Q", title="Avg Noise (dB)"),
            tooltip=[alt.Tooltip("area:N"), alt.Tooltip("avg_noise:Q")],
        ).properties(height=400)
        st.altair_chart(noise_chart, use_container_width=True)
        st.download_button(
            "Download Noise by Area (CSV)",
            data=df_noise.to_csv(index=False).encode("utf-8"),
            file_name="area_noise.csv",
            mime="text/csv",
        )
    else:
        st.info("No data for selected filters.")

# Traffic density

# Traffic density
st.subheader("2️⃣ Traffic Density Analysis (Avg Count by Hour)")
traffic_by_hour = list(coll.aggregate([
    {"$match": filter_query},
    {"$addFields": {"ts": {"$toDate": "$timestamp"}}},
    {"$group": {"_id": {"hour": {"$hour": "$ts"}}, "avg_traffic": {"$avg": "$traffic_count"}}},
    {"$project": {"_id": 0, "hour": "$_id.hour", "avg_traffic": {"$round": ["$avg_traffic", 2]}}},
]))
if traffic_by_hour:
    df_tr = pd.DataFrame(traffic_by_hour)
    df_tr_full = pd.DataFrame({"hour": list(range(24))}).merge(df_tr, on="hour", how="left").fillna({"avg_traffic": 0})
    df_tr_full = _to_native_df(df_tr_full)
    traffic_chart = alt.Chart(alt.Data(values=_to_alt_values(df_tr_full))).mark_line(point=True).encode(
        x=alt.X("hour:O", title="Hour (0-23)"),
        y=alt.Y("avg_traffic:Q", title="Avg Traffic Count"),
        tooltip=[alt.Tooltip("hour:O"), alt.Tooltip("avg_traffic:Q")],
    ).properties(height=350)
    st.altair_chart(traffic_chart, use_container_width=True)
    st.download_button(
        "Download Traffic by Hour (CSV)",
        data=df_tr_full.to_csv(index=False).encode("utf-8"),
        file_name="traffic_by_hour.csv",
        mime="text/csv",
    )
else:
    st.info("No data for selected filters.")

# Abnormal readings

# Abnormal readings
st.subheader("3️⃣ Abnormal Sensor Readings (Top 20)")
abnormal = list(coll.aggregate([
    {"$match": {"$or": [
        {"noise_db": {"$gt": 85}},
        {"traffic_count": {"$gt": 150}},
        {"pm25": {"$gt": 100}},
    ], **filter_query}},
    {"$project": {"_id": 0, "sensor_id": 1, "area": 1, "timestamp": 1, "noise_db": 1, "traffic_count": 1, "pm25": 1}},
    {"$sort": {"timestamp": 1}},
    {"$limit": 1000},
]))
if abnormal:
    df_ab = pd.DataFrame(abnormal).head(20)

    def _style(row):
        styles = []
        for col in row.index:
            val = row[col]
            if col == "pm25" and val > 100:
                styles.append("background-color: #ffcccc")
            elif col == "noise_db" and val > 85:
                styles.append("background-color: #ffe0e0")
            elif col == "traffic_count" and val > 150:
                styles.append("background-color: #ffe0e0")
            else:
                styles.append("")
        return styles

    try:
        st.dataframe(df_ab.style.apply(_style, axis=1), use_container_width=True)
    except Exception:
        st.dataframe(df_ab, use_container_width=True)

    st.download_button(
        "Download Abnormal Readings (CSV)",
        data=df_ab.to_csv(index=False).encode("utf-8"),
        file_name="abnormal_readings.csv",
        mime="text/csv",
    )
else:
    st.info("No abnormal readings for selected filters.")

st.subheader("➕ Manual Data Entry")
with st.expander("Add a new sensor reading", expanded=False):
    with st.form("manual_insert_form", clear_on_submit=True):
        # Area selection or new area entry
        existing_areas = _get_distinct_areas(coll) or []
        area_choice = st.selectbox("Select Area", options=["-- type new --"] + existing_areas, index=1 if existing_areas else 0)
        new_area = st.text_input("New Area (if not listed)") if area_choice == "-- type new --" else ""

        # Optional sensor id, else auto-generate
        sensor_id_input = st.text_input("Sensor ID (optional)", help="Leave blank to auto-generate")

        # Timestamp inputs
        ts_date: ddate = st.date_input("Date")
        ts_time: dtime = st.time_input("Time", value=dtime(12, 0))

        # Sensor values
        noise_val = st.number_input("Noise (dB)", min_value=0, max_value=150, value=70, step=1)
        traffic_val = st.number_input("Traffic Count", min_value=0, max_value=1000, value=80, step=1)
        pm25_val = st.number_input("PM2.5", min_value=0, max_value=500, value=60, step=1)

        submitted = st.form_submit_button("Insert Reading")

    if submitted:
        try:
            insert_doc = {
                "sensor_id": sensor_id_input.strip() or str(uuid.uuid4()),
                "area": (new_area.strip() if area_choice == "-- type new --" else area_choice),
                "timestamp": datetime.combine(ts_date, ts_time).isoformat(timespec="minutes"),
                "noise_db": int(noise_val),
                "traffic_count": int(traffic_val),
                "pm25": int(pm25_val),
            }

            # Basic validation
            if not insert_doc["area"]:
                st.error("Area is required.")
            else:
                coll.insert_one(insert_doc)
                # Clear cached area list so new areas appear in the sidebar filter
                try:
                    _get_distinct_areas.clear()
                except Exception:
                    pass
                st.success("Reading inserted successfully.")
                st.json(insert_doc)
                st.info("Charts update automatically on rerun. If you added a new area, select it in the sidebar filter to include it.")
        except Exception as e:
            st.error(f"Failed to insert reading: {e}")

    # Recent inserts (last 10 by timestamp)
    st.markdown("**Recent Inserts (last 10)**")
    try:
        recent_docs = list(coll.aggregate([
            {"$sort": {"timestamp": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0, "sensor_id": 1, "area": 1, "timestamp": 1, "noise_db": 1, "traffic_count": 1, "pm25": 1}},
        ]))
        if recent_docs:
            df_recent = pd.DataFrame(recent_docs)
            df_recent = _to_native_df(df_recent)
            st.dataframe(df_recent, use_container_width=True, height=260)
        else:
            st.info("No documents found.")
    except Exception as e:
        st.warning(f"Unable to load recent inserts: {e}")

st.markdown("---")
st.subheader("Final Insights")
st.write(
    "- Industrial Zone consistently shows higher PM2.5 and noise levels.\n"
    "- Peak congestion occurs during morning and evening hours.\n"
    "- Abnormal sensor events correlate strongly with traffic spikes.\n"
    "- This demonstrates how data-driven monitoring can support urban planning decisions."
)
st.caption("Data source: Synthetic; pipelines via MongoDB aggregations. KPIs reflect current filters.")
