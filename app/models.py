# app/models.py
from typing import Optional
from sqlmodel import SQLModel, Field
from datetime import datetime

class Dataset(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    size_gb: float = 0.0
    current_tier: str = Field(default="WARM")
    access_count: int = Field(default=0)
    last_access: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    migrating: bool = Field(default=False)

    # multi-cloud fields
    storage_type: str = Field(default="ONPREM")   # ONPREM | PRIVATE | PUBLIC
    location_uri: Optional[str] = Field(default=None)

    # NEW: timestamp of last storage migration (to avoid churn)
    last_storage_migration: Optional[datetime] = None


class AccessEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int
    access_type: str = Field(default="read")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class MigrationHistory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int
    from_tier: str
    to_tier: str
    reason: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class DatasetAggregate(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_id: int
    ewma: float = Field(default=0.0)
    last_updated: Optional[datetime] = None
