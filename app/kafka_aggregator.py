# app/kafka_aggregator.py
import asyncio
import os
import json
from datetime import datetime, timezone
from collections import defaultdict

from aiokafka import AIOKafkaConsumer
from sqlmodel import select

from app.db import get_session
from app.models import Dataset, AccessEvent, DatasetAggregate

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "dataset-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "agg-group")
ALPHA = float(os.getenv("EWMA_ALPHA", "0.1"))
BATCH_SECONDS = int(os.getenv("AGG_BATCH_SECONDS", "5"))

async def run_consumer():
    consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id=GROUP_ID)
    await consumer.start()
    print("[Aggregator] consumer started, waiting for messages...")
    try:
        buf = defaultdict(int)
        last_flush = datetime.utcnow()
        async for msg in consumer:
            try:
                ev = json.loads(msg.value.decode())
                dsid = int(ev.get("dataset_id"))
                buf[dsid] += 1
            except Exception as e:
                print("[Aggregator] message parse error:", e)

            # flush by time
            if (datetime.utcnow() - last_flush).total_seconds() >= BATCH_SECONDS:
                if buf:
                    ts = datetime.utcnow().replace(tzinfo=timezone.utc)
                    with get_session() as s:
                        for dd, count in list(buf.items()):
                            ds = s.get(Dataset, dd)
                            if not ds:
                                continue
                            # update counters
                            ds.access_count = (ds.access_count or 0) + count
                            ds.last_access = ts
                            s.add(ds)
                            # update/create aggregate row (EWMA uses count as recent signal)
                            q = select(DatasetAggregate).where(DatasetAggregate.dataset_id == dd)
                            agg = s.exec(q).one_or_none()
                            prev = (agg.ewma if agg else 0.0) or 0.0
                            new = ALPHA * count + (1 - ALPHA) * prev
                            if not agg:
                                agg = DatasetAggregate(dataset_id=dd, ewma=new, last_updated=ts)
                                s.add(agg)
                            else:
                                agg.ewma = new
                                agg.last_updated = ts
                                s.add(agg)
                        s.commit()
                    buf.clear()
                    last_flush = datetime.utcnow()
                    print("[Aggregator] flushed batch at", last_flush.isoformat())
    finally:
        await consumer.stop()
        print("[Aggregator] stopped")

if __name__ == "__main__":
    asyncio.run(run_consumer())
