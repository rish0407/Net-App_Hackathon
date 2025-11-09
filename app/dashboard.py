# app/dashboard.py
import os
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import requests
import time
import pandas as pd
from typing import Optional

# --- BASE / secrets resolution (env preferred) ---
BASE = os.getenv("BASE_URL")
if not BASE:
    if os.path.exists(".streamlit/secrets.toml"):
        try:
            BASE = st.secrets.get("BASE_URL", None)
        except Exception:
            BASE = None
    if not BASE:
        BASE = "http://localhost:8000"

# page layout
st.set_page_config(page_title="Data-in-Motion Dashboard", layout="wide")
st.title("Data-in-Motion — Demo Dashboard")

# -------------------------
# Sidebar: API key + controls
# -------------------------
st.sidebar.title("Controls")
api_key_input = st.sidebar.text_input("API Key (Compulsory)", value="", type="password")
HEADERS = {"x-api-key": api_key_input} if api_key_input else {}

def validate_api_key_now(api_key: str) -> bool:
    if not api_key:
        return True
    try:
        r = requests.get(f"{BASE}/api/auth/validate", headers={"x-api-key": api_key}, timeout=3)
        return r.status_code == 200
    except Exception:
        return False

is_valid_key = validate_api_key_now(api_key_input)
if api_key_input and not is_valid_key:
    st.sidebar.error("❌ Invalid API Key")
elif api_key_input and is_valid_key:
    st.sidebar.success("🔒 API Key accepted")

auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_seconds = st.sidebar.selectbox("Refresh every (s)", [3, 5, 10, 30], index=1)

st.sidebar.markdown("---")
st.sidebar.markdown("Quick actions")
if st.sidebar.button("Trigger scheduler run", disabled=not is_valid_key):
    try:
        r = requests.post(f"{BASE}/api/scheduler/run", headers=HEADERS, timeout=5)
        st.sidebar.success("Scheduler triggered")
    except Exception as e:
        st.sidebar.error(f"Scheduler call failed: {e}")

# -------------------------
# caching helpers
# -------------------------
@st.cache_data(ttl=5)
def fetch_datasets(headers) -> list:
    try:
        r = requests.get(f"{BASE}/api/datasets", headers=headers, timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []

@st.cache_data(ttl=5)
def fetch_recommendations(headers) -> list:
    try:
        r = requests.get(f"{BASE}/api/recommendations", headers=headers, timeout=5)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []
# -------------------------
# fetch migration history (robust: tries a few possible endpoints)
# -------------------------
@st.cache_data(ttl=5)
def fetch_migration_history(headers) -> list:
    """
    Expected record shape: {
      "id": int,
      "dataset_id": int,
      "from_tier": "WARM",
      "to_tier": "COLD",
      "reason": "auto-scheduler",
      "timestamp": "2025-11-08T11:57:18.038689"
    }
    """
    endpoints = [
        f"{BASE}/api/migration_history",
        f"{BASE}/api/migrations",
        f"{BASE}/api/migrationhistory",
        f"{BASE}/api/migration/history",
    ]
    for ep in endpoints:
        try:
            r = requests.get(ep, headers=headers, timeout=4)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
        except Exception:
            pass
    # fallback: empty
    return []

# -------------------------
# API helpers
# -------------------------
def create_dataset_api(name: str, size_gb: float, headers) -> dict:
    payload = {"name": name, "size_gb": float(size_gb)}
    r = requests.post(f"{BASE}/api/datasets", json=payload, headers=headers, timeout=5)
    try:
        return r.json()
    except Exception:
        return {"error": str(r.text)}

def send_events_api(dataset_id:int, n:int, headers):
    for _ in range(n):
        try:
            requests.post(f"{BASE}/api/stream/send", json={"dataset_id": dataset_id, "access_type":"read"}, headers=headers, timeout=2)
        except Exception:
            pass

def enqueue_migration_api(dataset_id:int, target_tier:str, target_storage:Optional[str], headers, reason="manual: dashboard"):
    payload = {"target_tier": target_tier, "reason": reason}
    if target_storage:
        payload["target_storage"] = target_storage
    r = requests.post(f"{BASE}/api/migrate_enqueue/{dataset_id}", json=payload, headers=headers, timeout=5)
    return r

# -------------------------
# Auto-refresh implementation (session_state)
# -------------------------
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

def maybe_refresh():
    if auto_refresh:
        now = time.time()
        if now - st.session_state.last_refresh >= refresh_seconds:
            st.session_state.last_refresh = now
            st.rerun()

# -------------------------
# Layout: two columns
# -------------------------
cols = st.columns([2, 1])

with cols[0]:
    st.subheader("Datasets overview")

    # Create dataset UI (top of left column)
    with st.expander("Create dataset", expanded=True):
        with st.form("create_ds_form", clear_on_submit=True):
            new_name = st.text_input("Dataset name", value="")
            new_size = st.number_input("Size (GB)", value=1.0, min_value=0.01, step=0.1)
            submitted = st.form_submit_button("Create dataset", disabled=not is_valid_key)
            if submitted:
                if not new_name:
                    st.error("Please provide a name.")
                else:
                    res = create_dataset_api(new_name.strip(), new_size, HEADERS)
                    # Clear cached fetch so we immediately see new dataset
                    fetch_datasets.clear()
                    if res.get("id"):
                        st.success(f"Created dataset id={res['id']}")
                    else:
                        st.error(f"Create failed: {res}")

    # fetch datasets
    datasets = fetch_datasets(HEADERS)
    if not datasets:
        st.info("No datasets found or failed to fetch. Check backend and API key.")
    else:
        df = pd.DataFrame(datasets)
        if "storage_type" not in df.columns:
            df["storage_type"] = df.get("storage_type", "UNKNOWN")
        if "size_gb" not in df.columns:
            df["size_gb"] = df.get("size_gb", 0.0)

        # Top-level storage charts
        st.subheader("Storage distribution")
        counts = df["storage_type"].fillna("UNKNOWN").value_counts()
        gb = df.groupby(df["storage_type"].fillna("UNKNOWN"))["size_gb"].sum()
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Datasets by storage**")
            st.bar_chart(counts)
        with c2:
            st.markdown("**Total GB by storage**")
            st.bar_chart(gb)

        st.markdown("---")
        st.markdown("**Datasets (click/select a dataset to operate)**")

        # present table and dataset selector
        display_df = df[["id","name","size_gb","current_tier","storage_type","location_uri","access_count","last_access"]].copy().fillna("")
        st.dataframe(display_df, use_container_width=True, height=300)

with cols[1]:
    st.subheader("Actions")
    st.markdown("Select dataset and perform actions below.")
    # Build a mapping for selectbox
    datasets = fetch_datasets(HEADERS)
    options = []
    id_map = {}
    for ds in datasets:
        label = f"{ds['id']} — {ds.get('name','')}"
        options.append(label)
        id_map[label] = ds["id"]

    selected_label = st.selectbox("Select dataset", options=options if options else ["1 — (no datasets)"])
    selected_id = id_map.get(selected_label, 1)
    send_n = st.slider("Send N events", min_value=1, max_value=500, value=10, step=1)
    tier_choice = st.selectbox("Target tier (for manual migration)", ["HOT","WARM","COLD"])
    storage_choice = st.selectbox("Target storage (explicit override)", ["", "ONPREM","PRIVATE","PUBLIC"])

    if st.button("Send events", disabled=not is_valid_key):
        with st.spinner(f"Sending {send_n} events to dataset {selected_id}..."):
            send_events_api(selected_id, send_n, HEADERS)
            # clear cache and trigger refresh
            fetch_recommendations.clear()
            fetch_datasets.clear()
            time.sleep(0.2)
            st.success("Events sent; aggregator will process shortly.")

    if st.button("Force migrate (enqueue)", disabled=not is_valid_key):
        with st.spinner(f"Enqueuing migration for dataset {selected_id} → {tier_choice} / {storage_choice or 'default'}"):
            try:
                r = enqueue_migration_api(selected_id, tier_choice, storage_choice or None, HEADERS)
                if r.status_code == 200:
                    # clear caches so UI reflects change
                    fetch_datasets.clear()
                    fetch_recommendations.clear()
                    st.success("Migration enqueued")
                else:
                    st.error(f"Failed: {r.status_code} {r.text}")
            except Exception as e:
                st.error(f"Error enqueuing: {e}")

    st.markdown("---")

st.subheader("Recommendations (actionable)")
recs = fetch_recommendations(HEADERS)
if recs:
    # normalize to DataFrame and sort by score desc
    rec_df = pd.DataFrame(recs)
    if "score" in rec_df.columns:
        rec_df = rec_df.sort_values(by="score", ascending=False)
    # show summary metrics
    st.markdown(f"**{len(rec_df)}** recommendations")
    # render each recommendation as a small card/row with action
    for _, row in rec_df.iterrows():
        dsid = int(row.get("id") or row.get("dataset_id") or 0)
        name = row.get("name","")
        score = row.get("score", 0.0)
        recommended = row.get("recommended_tier", "")
        est_save = row.get("estimated_monthly_saving_usd", 0.0)
        cols_r = st.columns([3,1,1,1])
        with cols_r[0]:
            st.markdown(f"**{dsid} — {name}**  ·  score: **{score:.3f}**  ·  rec: **{recommended}**")
            if row.get("note"):
                st.caption(row.get("note"))
        with cols_r[1]:
            st.write(f"${est_save:.2f}")
        with cols_r[2]:
            # trigger migration to recommended tier
            btn_key = f"migrate_rec_{dsid}"
            if st.button(f"Migrate → {recommended}", key=btn_key, disabled=not is_valid_key):
                # enqueue migration
                payload = {"target_tier": recommended, "reason": "auto: from recommendations UI"}
                try:
                    r = requests.post(f"{BASE}/api/migrate_enqueue/{dsid}", json=payload, headers=HEADERS, timeout=6)
                    if r.status_code == 200:
                        st.success(f"Enqueued migration for {dsid}")
                        # clear caches and refresh
                        fetch_datasets.clear()
                        fetch_recommendations.clear()
                        fetch_migration_history.clear()
                        st.rerun()
                    else:
                        st.error(f"Failed: {r.status_code} {r.text}")
                except Exception as e:
                    st.error(f"Error: {e}")
        with cols_r[3]:
            # small "Inspect" button to open details in a modal-like area (we show details below)
            if st.button("Details", key=f"details_{dsid}"):
                st.write(row.to_dict())
else:
    st.write("No recommendations currently.")

# footer / auto-refresh (non-blocking)
maybe_refresh = True
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

if auto_refresh:
    now = time.time()
    if now - st.session_state.last_refresh >= refresh_seconds:
        st.session_state.last_refresh = now
        # clear caches before rerun so data is fresh
        fetch_datasets.clear()
        fetch_recommendations.clear()
        st.rerun()
else:
    st.write("Auto-refresh disabled. Click actions to refresh data.")

st.subheader("Migrations Visualisation")
# -------------------------
# Migration visualization: Sankey + timeline + recent history table
# -------------------------
import plotly.graph_objects as go
from datetime import datetime
from collections import Counter, defaultdict

migrations = fetch_migration_history(HEADERS)

if migrations:
    st.subheader("Migration flows & timeline")

    # --- prepare sankey data (from_tier -> to_tier counts) ---
    links = Counter()
    for m in migrations:
        a = (m.get("from_tier") or "UNKNOWN")
        b = (m.get("to_tier") or "UNKNOWN")
        links[(a, b)] += 1

    # nodes and mapping
    nodes = []
    node_index = {}
    for (a, b) in links.keys():
        if a not in node_index:
            node_index[a] = len(nodes); nodes.append(a)
        if b not in node_index:
            node_index[b] = len(nodes); nodes.append(b)

    source = []
    target = []
    value = []
    for (a,b), cnt in links.items():
        source.append(node_index[a])
        target.append(node_index[b])
        value.append(cnt)

    # Sankey chart
    sankey_fig = go.Figure(
        go.Sankey(
            node = dict(label=nodes, pad=20, thickness=20),
            link = dict(source=source, target=target, value=value)
        )
    )
    sankey_fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(sankey_fig, use_container_width=True)

    # --- timeline: migrations per hour (or day if many) ---
    # parse timestamps and bucket by hour
    times = []
    for m in migrations:
        ts = m.get("timestamp") or m.get("time") or m.get("created_at")
        if ts:
            try:
                # accept naive or iso timestamps
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if "T" in ts else datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
                times.append(dt)
            except Exception:
                try:
                    times.append(datetime.fromisoformat(ts))
                except Exception:
                    pass
    if times:
        times_sorted = sorted(times)
        # bucket per hour
        buckets = defaultdict(int)
        for dt in times_sorted:
            key = dt.replace(minute=0, second=0, microsecond=0)
            buckets[key] += 1
        # build series
        series_df = pd.DataFrame({"time": list(buckets.keys()), "count": list(buckets.values())}).sort_values("time")
        # plot
        timeline_fig = go.Figure()
        timeline_fig.add_trace(go.Bar(x=series_df["time"], y=series_df["count"]))
        timeline_fig.update_layout(title="Migrations over time (hourly)", xaxis_title="Time", yaxis_title="Count", height=220, margin=dict(l=10,r=10,t=30,b=30))
        st.plotly_chart(timeline_fig, use_container_width=True)

    # --- recent history table (last 20) ---
    st.markdown("**Recent migrations**")
    mh_df = pd.DataFrame(migrations)
    if not mh_df.empty:
        mh_df = mh_df.sort_values(by="timestamp", ascending=False).head(20)
        st.dataframe(mh_df[["id","dataset_id","from_tier","to_tier","reason","timestamp"]].fillna(""), use_container_width=True, height=220)
else:
    st.info("No migration history available (or endpoint not found).")

# Sidebar help
st.sidebar.markdown("---")

