"""Shared fixtures for all test modules."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import mongomock
import pymongo
import pytest
from bson import Decimal128, ObjectId

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_DOCS = [
    {
        "_id": ObjectId("65a000000000000000000001"),
        "name": "Alice",
        "amount": 100.50,
        "created_at": datetime(2024, 1, 15, tzinfo=timezone.utc),
        "meta": {"source": "sensor_A", "version": 1},
    },
    {
        "_id": ObjectId("65a000000000000000000002"),
        "name": "Bob",
        "amount": 200.00,
        "created_at": datetime(2024, 2, 20, tzinfo=timezone.utc),
        "meta": {"source": "sensor_B", "version": 2},
    },
    {
        "_id": ObjectId("65a000000000000000000003"),
        "name": "Charlie",
        "amount": 300.75,
        "created_at": datetime(2024, 3, 10, tzinfo=timezone.utc),
        "meta": {"source": "sensor_A", "version": 1},
    },
]

# Documents with raw BSON types (for transformer tests)
BSON_DOCS = [
    {
        "_id": ObjectId("65a000000000000000000004"),
        "name": "Dana",
        "amount": Decimal128("99.99"),
        "created_at": datetime(2024, 4, 5, tzinfo=timezone.utc),
        "tags": [ObjectId("65a000000000000000000005"), "iot"],
        "nested": {"inner": {"deep": "value"}},
    },
]


# ---------------------------------------------------------------------------
# MongoDB fixtures (mongomock — no real server needed)
# ---------------------------------------------------------------------------


@pytest.fixture
def mongo_client():
    """Return an in-memory mongomock client."""
    client = mongomock.MongoClient("mongodb://localhost:27017")
    yield client
    client.close()


@pytest.fixture
def populated_mongo(mongo_client):
    """Mongo client with testdb.orders pre-populated."""
    db = mongo_client["testdb"]
    db["orders"].insert_many(SAMPLE_DOCS)
    db["users"].insert_many([{"name": "X"}, {"name": "Y"}])
    return mongo_client


# ---------------------------------------------------------------------------
# Arrow / Parquet fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_arrow_table():
    """Minimal PyArrow table for writer tests."""
    import pyarrow as pa

    return pa.table({"name": ["Alice", "Bob"], "age": [30, 25]})


# ---------------------------------------------------------------------------
# Filesystem fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for Parquet output."""
    out = tmp_path / "output"
    out.mkdir()
    return out
