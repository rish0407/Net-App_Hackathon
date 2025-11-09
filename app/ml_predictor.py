# app/ml_predictor.py
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import numpy as np
from sklearn.linear_model import LinearRegression
from sqlmodel import select
from app.db import get_session
from app.models import AccessEvent

def _fetch_bucketed_series(dataset_id:int, hours:int=24, bucket_minutes:int=60):
    end = datetime.utcnow().replace(tzinfo=timezone.utc)
    start = end - timedelta(hours=hours)
    with get_session() as s:
        q = select(AccessEvent).where(
            (AccessEvent.dataset_id == dataset_id) &
            (AccessEvent.timestamp >= start) &
            (AccessEvent.timestamp <= end)
        ).order_by(AccessEvent.timestamp)
        events = s.exec(q).all()

    bucket_delta = timedelta(minutes=bucket_minutes)
    epoch = datetime(1970,1,1,tzinfo=timezone.utc)
    start_offset = int((start - epoch).total_seconds() // (bucket_minutes*60)) * (bucket_minutes*60)
    start_bucket = epoch + timedelta(seconds=start_offset)
    buckets = defaultdict(int)
    for ev in events:
        ts = ev.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        seconds = int((ts - start_bucket).total_seconds())
        idx = seconds // (bucket_minutes * 60)
        bucket_start = start_bucket + timedelta(seconds=idx * bucket_minutes * 60)
        buckets[bucket_start] += 1

    series = []
    cur = start_bucket
    now = end
    while cur <= now:
        series.append(int(buckets.get(cur, 0)))
        cur += bucket_delta
    return series

def predict_future_mean(dataset_id:int, hours:int=24, bucket_minutes:int=60, predict_steps:int=3):
    series = _fetch_bucketed_series(dataset_id, hours=hours, bucket_minutes=bucket_minutes)
    if len(series) < 3 or sum(series) == 0:
        return 0.0
    X = np.arange(len(series)).reshape(-1,1)
    y = np.array(series)
    model = LinearRegression().fit(X, y)
    X_pred = np.arange(len(series), len(series)+predict_steps).reshape(-1,1)
    y_pred = model.predict(X_pred)
    y_pred = np.clip(y_pred, 0, None)
    return float(np.mean(y_pred))
