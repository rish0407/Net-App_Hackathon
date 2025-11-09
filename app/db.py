# app/db.py
from sqlmodel import SQLModel, create_engine, Session
from contextlib import contextmanager
import os

DB_URL = os.getenv("DATA_DB_URL", "sqlite:///data.db")
# echo=False for less noisy logs
engine = create_engine(DB_URL, echo=False, connect_args={"check_same_thread": False})

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

@contextmanager
def get_session():
    with Session(engine) as session:
        yield session
