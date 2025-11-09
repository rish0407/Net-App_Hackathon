# app/main.py
import os
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from sqlmodel import select
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from app.db import get_session
from app import models

from app.db import create_db_and_tables, get_session
from app.models import Dataset, AccessEvent, MigrationHistory, DatasetAggregate
from app.ml_predictor import predict_future_mean

# add these imports near other imports in app/main.py
import asyncio
from queue import SimpleQueue
from app.storage_adapters import adapter_for_storage, onprem_adapter, private_adapter, public_adapter
from dotenv import load_dotenv

# Kafka producer
from aiokafka import AIOKafkaProducer
import logging

load_dotenv()

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dataset-events")
SCHED_INTERVAL = int(os.getenv("SCHED_INTERVAL_SECONDS", "10"))
SCHED_COOLDOWN = int(os.getenv("SCHED_COOLDOWN_SECONDS", "60"))
SCHED_MIN_SAVING_USD = float(os.getenv("SCHED_MIN_SAVING_USD", "0.0"))
EWMA_WEIGHT_MAX = float(os.getenv("EWMA_WEIGHT_MAX", "50.0"))
STORAGE_MIN_SAVING_USD = float(os.getenv("STORAGE_MIN_SAVING_USD", "0.5"))
STORAGE_COOLDOWN_SECONDS = int(os.getenv("STORAGE_COOLDOWN_SECONDS", "86400"))  # default 1 day

# migration queue (in-memory)
migration_queue: "SimpleQueue[dict]" = SimpleQueue()
# mapping tiers to target storage_type
TIER_TO_STORAGE = {
    "HOT": "ONPREM",
    "WARM": "PRIVATE",
    "COLD": "PUBLIC",
}
API_KEY = os.getenv("API_KEY", None)

# Tier ordering
TIER_ORDER = {"HOT": 3, "WARM": 2, "COLD": 1}

app = FastAPI(title="Data-in-Motion MVP")

# CORS for Streamlit dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple API key dependency for write endpoints
def require_api_key(x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# Pydantic request schemas
class DatasetCreate(BaseModel):
    name: str
    size_gb: float

class StreamEvent(BaseModel):
    dataset_id: int
    access_type: Optional[str] = "read"

class MigrateRequest(BaseModel):
    target_tier: str
    reason: Optional[str] = None

# Start/stop kafka producer
producer: Optional[AIOKafkaProducer] = None

async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    print("[Main] Kafka producer started")

async def stop_kafka_producer():
    global producer
    if producer:
        await producer.stop()
        print("[Main] Kafka producer stopped")

# helper utilities
def is_colder(current: str, candidate: str) -> bool:
    return TIER_ORDER.get(candidate, 2) < TIER_ORDER.get(current, 2)

# def time_since(dt: Optional[datetime]) -> float:
#     if not dt:
#         return float("inf")
#     return (datetime.utcnow().replace(tzinfo=timezone.utc) - dt).total_seconds()
def time_since(dt: Optional[datetime]) -> float:
    """Return seconds since dt. Accepts naive or tz-aware datetimes.
       If dt is None, return +inf to indicate "very old / not present"."""
    if not dt:
        return float("inf")
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    # normalize dt to tz-aware UTC if it's naive
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).total_seconds()

# scoring: combine short-term rate (access_count / age), recency, EWMA prediction, size penalty
def compute_score(ds: Dataset) -> float:
    # short-term rate normalized
    age_hours = max(1.0, (datetime.utcnow() - ds.created_at).total_seconds() / 3600.0)
    rate = (ds.access_count or 0) / age_hours
    rate_norm = min(rate / 50.0, 1.0)  # scale to [0,1]

    # recency: recent access boosts score
    recency_seconds = time_since(ds.last_access)
    recency = 1.0 if recency_seconds < 3600 else max(0.0, 1.0 - recency_seconds / (24*3600))

    # EWMA aggregate if present
    with get_session() as s:
        q = select(DatasetAggregate).where(DatasetAggregate.dataset_id == ds.id)
        agg = s.exec(q).one_or_none()
        ewma = (agg.ewma if agg else 0.0) or 0.0
    ewma_norm = min(ewma / EWMA_WEIGHT_MAX, 1.0)

    # ML predicted future mean (cheap call) - you can limit frequency in production
    try:
        pred = predict_future_mean(ds.id, hours=24, bucket_minutes=60, predict_steps=3)
    except Exception:
        pred = 0.0
    pred_norm = min(pred / 50.0, 1.0)

    # size penalty (larger datasets slightly penalized toward colder)
    size_penalty = min(ds.size_gb / 100.0, 1.0)

    # weighted sum
    score = 0.45 * rate_norm + 0.2 * recency + 0.15 * ewma_norm + 0.15 * pred_norm - 0.05 * size_penalty
    return float(max(0.0, min(score, 1.0)))

def recommend_tier(score: float, up_threshold=0.6, down_threshold=0.35):
    if score >= up_threshold:
        return "HOT"
    elif score >= down_threshold:
        return "WARM"
    else:
        return "COLD"

def estimate_p_access_from_predmean(pred_mean: float) -> float:
    # Poisson: P(>=1) = 1 - exp(-lambda)
    try:
        import math
        return 1.0 - math.exp(-max(0.0, pred_mean))
    except Exception:
        return min(1.0, pred_mean / 10.0)

# cost constants (tune these)
STORAGE_COST_PER_GB = {"ONPREM": 0.20, "PRIVATE": 0.05, "PUBLIC": 0.01}
LATENCY_PENALTY_PER_ACCESS = {"ONPREM": 0.01, "PRIVATE": 0.05, "PUBLIC": 0.2}
TRANSFER_COST_PER_GB = {"ONPREM": 0.05, "PRIVATE": 0.01, "PUBLIC": 0.02}
AMORTIZE_MONTHS = 1
MIN_SAVING_USD = 0.1    # demo threshold

def compute_expected_monthly_cost(storage_type: str, size_gb: float, expected_accesses_per_month: float, amortized_transfer: float = 0.0):
    storage_cost = STORAGE_COST_PER_GB.get(storage_type, 0.03) * size_gb
    latency_cost = LATENCY_PENALTY_PER_ACCESS.get(storage_type, 0.05) * expected_accesses_per_month
    return storage_cost + latency_cost + amortized_transfer

def decide_storage_for_dataset(ds: Dataset, horizon_hours:int=24):
    # 1) predict mean accesses in horizon using existing predictor (avg per bucket * buckets)
    pred_mean = predict_future_mean(ds.id, hours=horizon_hours, bucket_minutes=60, predict_steps=1)
    # convert to probability of >=1 access in that horizon
    p_access = estimate_p_access_from_predmean(pred_mean)
    # convert to expected accesses per month (approx)
    expected_accesses_per_month = p_access * (30*24) / max(1, horizon_hours)

    # evaluate candidate storage choices
    current_storage = ds.storage_type or TIER_TO_STORAGE.get(ds.current_tier, "ONPREM")
    candidates = ["ONPREM","PRIVATE","PUBLIC"]
    best = {"storage": current_storage, "cost": compute_expected_monthly_cost(current_storage, ds.size_gb, expected_accesses_per_month, amortized_transfer=0.0)}
    for c in candidates:
        # if moving, include amortized transfer cost
        if c == current_storage:
            amort = 0.0
        else:
            transfer = TRANSFER_COST_PER_GB.get(c, 0.02) * ds.size_gb
            amort = transfer / AMORTIZE_MONTHS
        cost = compute_expected_monthly_cost(c, ds.size_gb, expected_accesses_per_month, amortized_transfer=amort)
        if cost < best["cost"]:
            best = {"storage": c, "cost": cost}
    # decide
    saving = best["cost"] - compute_expected_monthly_cost(current_storage, ds.size_gb, expected_accesses_per_month, amortized_transfer=0.0)
    # return recommended storage and savings (positive means moving to best reduces cost)
    return best["storage"], -saving

async def _migration_worker_loop():
    print("[Migrator] migration worker running")
    loop = asyncio.get_running_loop()
    while True:
        # blockingly get an item from the queue in threadpool to not block event loop
        job = await loop.run_in_executor(None, migration_queue.get)
        if not job:
            await asyncio.sleep(1)
            continue
        dataset_id = int(job["dataset_id"])
        target_tier = job["target_tier"]
        reason = job.get("reason", "enqueue")
        # determine target storage: prefer explicit job target_storage if provided
        target_storage = job.get("target_storage")
        if not target_storage:
            target_storage = TIER_TO_STORAGE.get(target_tier, "ONPREM")


        try:
            with get_session() as s:
                ds = s.get(Dataset, dataset_id)
                if not ds:
                    print(f"[Migrator] dataset {dataset_id} not found, skipping")
                    continue

                # optimistic lock
                if ds.migrating:
                    print(f"[Migrator] dataset {dataset_id} is already migrating, skipping")
                    continue
                ds.migrating = True
                s.add(ds); s.commit(); s.refresh(ds)

                src_storage = (ds.storage_type or TIER_TO_STORAGE.get(ds.current_tier, "ONPREM"))
                src_uri = ds.location_uri
                # determine adapters
                src_adapter = adapter_for_storage(src_storage)
                dst_adapter = adapter_for_storage(target_storage)

                print(f"[Migrator] migrating ds={dataset_id} {src_storage}({src_uri}) -> {target_storage}")

                # perform copy (local adapters will create placeholder if src missing)
                new_uri = dst_adapter.copy_from(src_uri, dataset_id)

                # update metadata atomically
                ds.storage_type = target_storage
                ds.location_uri = new_uri
                ds.current_tier = target_tier
                ds.migrating = False
                ds.last_storage_migration = datetime.utcnow().replace(tzinfo=timezone.utc)
                s.add(ds)
                mh = MigrationHistory(dataset_id=dataset_id, from_tier=src_storage, to_tier=target_storage, reason=reason)                
                s.add(mh)
                s.commit()
                print(f"[Migrator] migration completed: {dataset_id} -> {new_uri}")
        except Exception as e:
            # try to clear migrating flag on error
            try:
                with get_session() as s2:
                    ds2 = s2.get(Dataset, dataset_id)
                    if ds2:
                        ds2.migrating = False
                        s2.add(ds2); s2.commit()
            except Exception:
                pass
            print("[Migrator] Error processing job:", e)
        # short sleep to yield CPU
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_all():
    create_db_and_tables()
    try:
        await start_kafka_producer()
    except Exception as e:
        print("[Startup] Kafka producer failed to start:", e)
    # existing background tasks
    asyncio.create_task(_scheduler_loop())
    asyncio.create_task(_migration_worker_loop())   # <-- start migration worker
    print("[Startup] background tasks started")


@app.on_event("shutdown")
async def shutdown_all():
    await stop_kafka_producer()

# CRUD endpoints
@app.get("/api/auth/validate")
def validate_api(x_api_key: Optional[str] = Depends(require_api_key)):
    """Return 200 only when API key is valid (or API_KEY is unset on server)."""
    return {"ok": True}

@app.post("/api/datasets", response_model=Dataset)
def create_dataset(payload: DatasetCreate, x_api_key: Optional[str] = Depends(require_api_key)):
    ds = Dataset(name=payload.name, size_gb=payload.size_gb)
    with get_session() as s:
        s.add(ds)
        s.commit()
        s.refresh(ds)
        return ds

@app.get("/api/datasets", response_model=List[Dataset])
def list_datasets():
    with get_session() as s:
        q = select(Dataset)
        return s.exec(q).all()

@app.get("/api/datasets/{dataset_id}", response_model=Dataset)
def get_dataset(dataset_id: int):
    with get_session() as s:
        ds = s.get(Dataset, dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="dataset not found")
        return ds

# produce event to Kafka (async)
@app.post("/api/stream/send")
async def send_event(ev: StreamEvent, x_api_key: Optional[str] = Depends(require_api_key)):
    # quick validation
    with get_session() as s:
        ds = s.get(Dataset, ev.dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="dataset not found")
    if producer is None:
        raise HTTPException(status_code=503, detail="producer not ready")
    payload = ev.dict()
    payload["ts"] = datetime.utcnow().isoformat()
    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(payload).encode())
    return {"status": "sent_to_kafka"}

# Enqueue migration job (non-blocking)
@app.post("/api/migrate_enqueue/{dataset_id}")
def migrate_enqueue(dataset_id: int, payload: MigrateRequest, x_api_key: Optional[str] = Depends(require_api_key)):
    """
    Enqueue a migration job: the worker will perform the copy and atomically update metadata.
    Payload.target_tier should be HOT/WARM/COLD (we compute target storage type).
    """
    target_tier = payload.target_tier
    if target_tier not in TIER_TO_STORAGE:
        raise HTTPException(status_code=400, detail="invalid target_tier")
    # enqueue job
    migration_queue.put({
        "dataset_id": dataset_id,
        "target_tier": target_tier,
        "reason": payload.reason or "enqueue"
    })
    return {"enqueued": True, "dataset_id": dataset_id, "target_tier": target_tier}

# migrate endpoint (manual)
@app.post("/api/datasets/{dataset_id}/migrate", response_model=Dataset)
def apply_migration(dataset_id: int, payload: MigrateRequest, x_api_key: Optional[str] = Depends(require_api_key)):
    target = payload.target_tier
    with get_session() as s:
        ds = s.get(Dataset, dataset_id)
        if not ds:
            raise HTTPException(status_code=404, detail="dataset not found")
        # optimistic lock: avoid concurrent migrations
        if ds.migrating:
            raise HTTPException(status_code=409, detail="dataset currently migrating")
        # mark migrating
        ds.migrating = True
        s.add(ds); s.commit(); s.refresh(ds)
        try:
            from_tier = ds.current_tier or "WARM"
            to_tier = target
            # apply
            ds.current_tier = to_tier
            s.add(ds)
            mh = MigrationHistory(dataset_id=dataset_id, from_tier=from_tier, to_tier=to_tier, reason=payload.reason)
            s.add(mh)
            s.commit()
            s.refresh(ds)
            return ds
        finally:
            # release lock
            ds.migrating = False
            s.add(ds); s.commit()

# recommendations endpoint
'''make it based on size'''
@app.get("/api/recommendations")
def recommendations():
    out = []
    COSTS = {"HOT": 0.10, "WARM": 0.03, "COLD": 0.005}
    with get_session() as s:
        ds_list = s.exec(select(Dataset)).all()
        for ds in ds_list:
            sc = compute_score(ds)
            rec = recommend_tier(sc)
            saving = max(0.0, (COSTS.get(ds.current_tier, 0.03) - COSTS.get(rec, 0.03)) * ds.size_gb)
            out.append({
                "id": ds.id,
                "name": ds.name,
                "size_gb": ds.size_gb,
                "current_tier": ds.current_tier,
                "score": round(sc,3),
                "recommended_tier": rec,
                "estimated_monthly_saving_usd": round(saving,3)
            })
    return out

logger = logging.getLogger("migration_history")

@app.get("/api/migration_history", response_model=List[dict])
def get_migration_history(skip: int = 0, limit: int = 200):
    """
    Returns recent MigrationHistory entries as a list of compact dicts.
    Using get_session() as a context manager to obtain an actual SQLAlchemy Session.
    """
    try:
    # get_session is a contextmanager/generator in this project — use it with `with`
        with get_session() as db: # db will be a real Session inside the 'with' block
            rows = db.query(models.MigrationHistory).order_by(models.MigrationHistory.timestamp.desc()).offset(skip).limit(limit).all()
            result = []
            for r in rows:
                result.append({
                "id": r.id,
                "dataset_id": r.dataset_id,
                "from_tier": getattr(r, "from_tier", None),
                "to_tier": getattr(r, "to_tier", None),
                "from_storage": getattr(r, "from_storage", None),
                "to_storage": getattr(r, "to_storage", None),
                "reason": getattr(r, "reason", None),
                "timestamp": r.timestamp.isoformat() if hasattr(r.timestamp, "isoformat") else str(r.timestamp),
                })
        return result
    except Exception as e:
        logger.exception("Failed to fetch migration history")
        # return empty list on error so dashboard doesn't fully break
        return []

# ML debug endpoint
@app.get("/api/ml/predict/{dataset_id}")
def ml_predict_endpoint(dataset_id:int, hours:int=24, bucket_minutes:int=60):
    val = predict_future_mean(dataset_id, hours=hours, bucket_minutes=bucket_minutes)
    return {"dataset_id": dataset_id, "predicted_mean_per_bucket": val}

# scheduler helpers
def latest_migration_within(dataset_id: int, seconds: int) -> bool:
    cutoff = datetime.utcnow() - timedelta(seconds=seconds)
    with get_session() as s:
        q = select(MigrationHistory).where(MigrationHistory.dataset_id == dataset_id).order_by(MigrationHistory.timestamp.desc()).limit(1)
        row = s.exec(q).one_or_none()
        if not row:
            return False
        return row.timestamp.replace(tzinfo=timezone.utc) >= cutoff.replace(tzinfo=timezone.utc)

async def _scheduler_loop():
    print("[Scheduler] starting loop, interval:", SCHED_INTERVAL)
    while True:
        print("[Scheduler] Running evaluation cycle…")
        try:
            with get_session() as s:
                ds_list = s.exec(select(Dataset)).all()
                COSTS = {"HOT": 0.10, "WARM": 0.03, "COLD": 0.005}
                '''
                for ds in ds_list:
                    # skip if migrating now
                    if ds.migrating:
                        continue
                    score = compute_score(ds)
                    candidate = recommend_tier(score)
                    cur = ds.current_tier or "WARM"
                    # by default: allow migrations both ways but use cooldown & min saving
                    if candidate != cur:
                        saving = max(0.0, (COSTS.get(cur, 0.03) - COSTS.get(candidate, 0.03)) * ds.size_gb)
                        # require saving threshold OR be a promotion (if you prefer promotions, set MIN_SAVING_USD low)
                        if saving >= SCHED_MIN_SAVING_USD or not is_colder(cur, candidate):
                            if not latest_migration_within(ds.id, SCHED_COOLDOWN):
                                # acquire lock and re-check
                                ds_check = s.get(Dataset, ds.id)
                                if ds_check.migrating:
                                    continue
                                ds_check.migrating = True
                                s.add(ds_check); s.commit()
                                # re-evaluate one more time for safety
                                final_score = compute_score(ds_check)
                                final_candidate = recommend_tier(final_score)
                                if final_candidate != ds_check.current_tier:
                                    from_tier = ds_check.current_tier
                                    to_tier = final_candidate
                                    ds_check.current_tier = to_tier
                                    s.add(ds_check)
                                    mh = MigrationHistory(dataset_id=ds_check.id, from_tier=from_tier, to_tier=to_tier, reason="auto-scheduler")
                                    s.add(mh)
                                    s.commit()
                                    print(f"[Scheduler] auto-migrated dataset {ds_check.id} {from_tier} → {to_tier}")
                                # release lock
                                ds_check.migrating = False
                                s.add(ds_check); s.commit()
                                '''
                # --- begin replacement block for per-dataset logic inside _scheduler_loop ---
                for ds in ds_list:
                    # skip if migrating now
                    if ds.migrating:
                        continue

                    # score & tier recommendation
                    score = compute_score(ds)
                    candidate_tier = recommend_tier(score)
                    cur_tier = ds.current_tier or "WARM"

                    # default storage implied by tier mapping
                    default_storage = TIER_TO_STORAGE.get(candidate_tier, "ONPREM")

                    # use ML-cost decision to pick storage and estimate saving (USD)
                    try:
                        best_storage, storage_saving_usd = decide_storage_for_dataset(ds, horizon_hours=24)
                    except Exception as e:
                        # if predictor fails, fall back to default
                        print(f"[Scheduler] storage decision failed for ds {ds.id}: {e}")
                        best_storage, storage_saving_usd = default_storage, 0.0

                    # choose final storage: only override default if saving > STORAGE_MIN_SAVING_USD
                    final_storage = best_storage if (storage_saving_usd is not None and storage_saving_usd > STORAGE_MIN_SAVING_USD) else default_storage

                    # check storage cooldown (do not frequently migrate storage)
                    allow_storage_migrate = True
                    if ds.last_storage_migration:
                        last = ds.last_storage_migration
                        if last.tzinfo is None:
                            last = last.replace(tzinfo=timezone.utc)
                        allow_storage_migrate = (datetime.utcnow().replace(tzinfo=timezone.utc) - last).total_seconds() >= STORAGE_COOLDOWN_SECONDS

                    # decide if we should migrate (either tier or storage differs)
                    need_tier_change = (candidate_tier != cur_tier)
                    need_storage_change = (final_storage != (ds.storage_type or TIER_TO_STORAGE.get(ds.current_tier, "ONPREM")))

                    if need_tier_change or (need_storage_change and allow_storage_migrate):
                        # require either cost saving or promotions (preserve prior behavior)
                        saving = max(0.0, (COSTS.get(cur_tier, 0.03) - COSTS.get(candidate_tier, 0.03)) * ds.size_gb)
                        if saving >= SCHED_MIN_SAVING_USD or not is_colder(cur_tier, candidate_tier) or (storage_saving_usd and storage_saving_usd > STORAGE_MIN_SAVING_USD):
                            # check last migration cooldown (for tier changes)
                            if not latest_migration_within(ds.id, SCHED_COOLDOWN):
                                # acquire DB-level optimistic lock and re-evaluate safely
                                ds_check = s.get(Dataset, ds.id)
                                if ds_check.migrating:
                                    continue
                                ds_check.migrating = True
                                s.add(ds_check); s.commit(); s.refresh(ds_check)

                                # re-evaluate to avoid race
                                final_score = compute_score(ds_check)
                                final_candidate = recommend_tier(final_score)

                                # recompute storage decision at commit time
                                try:
                                    final_best_storage, final_storage_saving = decide_storage_for_dataset(ds_check, horizon_hours=24)
                                except Exception:
                                    final_best_storage, final_storage_saving = TIER_TO_STORAGE.get(final_candidate, "ONPREM"), 0.0

                                final_storage_to_apply = final_best_storage if (final_storage_saving and final_storage_saving > STORAGE_MIN_SAVING_USD) else TIER_TO_STORAGE.get(final_candidate, "ONPREM")

                                # if nothing changed after re-eval, skip
                                if final_candidate != ds_check.current_tier or final_storage_to_apply != ds_check.storage_type:
                                    # enqueue a single migration job that will move bytes and update both tier & storage
                                    migration_queue.put({
                                        "dataset_id": ds_check.id,
                                        "target_tier": final_candidate,
                                        "reason": "auto-scheduler",
                                        "target_storage": final_storage_to_apply
                                    })
                                    print(f"[Scheduler] enqueued migration for dataset {ds_check.id}: tier {ds_check.current_tier}→{final_candidate}, storage {ds_check.storage_type}→{final_storage_to_apply}")

                                # release optimistic lock
                                ds_check.migrating = False
                                s.add(ds_check); s.commit()
                # --- end replacement block ---

        except Exception as e:
            print("[Scheduler] Error:", e)
        await asyncio.sleep(SCHED_INTERVAL)

# manual trigger
@app.post("/api/scheduler/run")
def run_scheduler_once(x_api_key: Optional[str] = Depends(require_api_key)):
    applied = []
    COSTS = {"HOT": 0.10, "WARM": 0.03, "COLD": 0.005}
    with get_session() as s:
        ds_list = s.exec(select(Dataset)).all()
        for ds in ds_list:
            if ds.migrating:
                continue
            score = compute_score(ds)
            candidate = recommend_tier(score)
            cur = ds.current_tier or "WARM"
            if candidate != cur:
                saving = max(0.0, (COSTS.get(cur, 0.03) - COSTS.get(candidate, 0.03)) * ds.size_gb)
                if saving >= SCHED_MIN_SAVING_USD or not is_colder(cur, candidate):
                    if not latest_migration_within(ds.id, SCHED_COOLDOWN):
                        ds_check = s.get(Dataset, ds.id)
                        if ds_check.migrating:
                            continue
                        ds_check.migrating = True
                        s.add(ds_check); s.commit()
                        final_score = compute_score(ds_check)
                        final_candidate = recommend_tier(final_score)
                        if final_candidate != ds_check.current_tier:
                            from_tier = ds_check.current_tier
                            to_tier = final_candidate
                            ds_check.current_tier = to_tier
                            s.add(ds_check)
                            mh = MigrationHistory(dataset_id=ds_check.id, from_tier=from_tier, to_tier=to_tier, reason="manual-run")
                            s.add(mh)
                            s.commit()
                            applied.append({"id": ds_check.id, "from": from_tier, "to": to_tier})
                        ds_check.migrating = False
                        s.add(ds_check); s.commit()
    return {"applied": applied}

# --- analytics endpoint (time-bucketed access counts) ---
from datetime import timedelta
from fastapi import Query

@app.get("/api/analytics/{dataset_id}")
def analytics(dataset_id: int, hours: int = Query(24, ge=1), bucket_minutes: int = Query(15, ge=1)):
    """
    Return time-bucketed access counts for dataset_id.
    Example: /api/analytics/1?hours=24&bucket_minutes=15
    """
    # compute window
    end = datetime.utcnow().replace(tzinfo=timezone.utc)
    start = end - timedelta(hours=hours)
    bucket_delta = timedelta(minutes=bucket_minutes)

    # fetch events in window
    with get_session() as s:
        q = select(AccessEvent).where(
            (AccessEvent.dataset_id == dataset_id) &
            (AccessEvent.timestamp >= start) &
            (AccessEvent.timestamp <= end)
        ).order_by(AccessEvent.timestamp)
        events = s.exec(q).all()

    # build buckets aligned to bucket_minutes (UTC)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    start_seconds = int((start - epoch).total_seconds())
    bucket_base_seconds = (start_seconds // (bucket_minutes * 60)) * (bucket_minutes * 60)
    bucket_base = epoch + timedelta(seconds=bucket_base_seconds)

    series = []
    cur = bucket_base
    # prepare a dict of counts keyed by bucket_start
    counts = {}
    for ev in events:
        ts = ev.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        seconds = int((ts - bucket_base).total_seconds())
        idx = seconds // (bucket_minutes * 60)
        bucket_start = bucket_base + timedelta(seconds=idx * bucket_minutes * 60)
        counts[bucket_start] = counts.get(bucket_start, 0) + 1

    # fill series from bucket_base upto end (inclusive of last partial bucket)
    while cur <= end:
        series.append({"bucket_start": cur.isoformat(), "count": int(counts.get(cur, 0))})
        cur += bucket_delta

    return {"dataset_id": dataset_id, "hours": hours, "bucket_minutes": bucket_minutes, "series": series}

