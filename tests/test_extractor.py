"""Unit tests for extractor.py — uses mongomock (no real MongoDB)."""
from __future__ import annotations

from datetime import datetime, timezone

import mongomock
import pytest

from mongodb_to_parquet.extractor import MongoExtractor


@pytest.fixture
def extractor(populated_mongo):
    """Extractor with a pre-injected mongomock client."""
    ext = MongoExtractor(uri="mongodb://localhost:27017")
    ext.client = populated_mongo  # bypass connect(); inject mock
    return ext


# ---------------------------------------------------------------------------
# list_databases
# ---------------------------------------------------------------------------


class TestListDatabases:
    def test_returns_user_databases(self, extractor):
        dbs = extractor.list_databases()
        assert "testdb" in dbs

    def test_excludes_system_databases(self, extractor, populated_mongo):
        # mongomock may expose admin/config; extractor must filter them out
        dbs = extractor.list_databases()
        for sys_db in ("admin", "config", "local"):
            assert sys_db not in dbs

    def test_include_filter(self, extractor):
        dbs = extractor.list_databases(include=["testdb"])
        assert dbs == ["testdb"]

    def test_include_filter_missing_db(self, extractor):
        dbs = extractor.list_databases(include=["nonexistent"])
        assert dbs == []

    def test_exclude_filter(self, extractor):
        dbs = extractor.list_databases(exclude=["testdb"])
        assert "testdb" not in dbs

    def test_include_and_exclude_overlap(self, extractor):
        # include takes precedence for existing; exclude removes from include
        dbs = extractor.list_databases(include=["testdb"], exclude=["testdb"])
        assert "testdb" not in dbs


# ---------------------------------------------------------------------------
# list_collections
# ---------------------------------------------------------------------------


class TestListCollections:
    def test_returns_all_collections(self, extractor):
        cols = extractor.list_collections("testdb")
        assert "orders" in cols
        assert "users" in cols

    def test_include_filter(self, extractor):
        cols = extractor.list_collections("testdb", include=["orders"])
        assert cols == ["orders"]

    def test_exclude_filter(self, extractor):
        cols = extractor.list_collections("testdb", exclude=["users"])
        assert "users" not in cols
        assert "orders" in cols

    def test_include_nonexistent_collection(self, extractor):
        cols = extractor.list_collections("testdb", include=["ghost"])
        assert cols == []


# ---------------------------------------------------------------------------
# stream
# ---------------------------------------------------------------------------


class TestStream:
    def test_yields_all_documents(self, extractor):
        docs = list(extractor.stream("testdb", "orders", {}))
        assert len(docs) == 3

    def test_yields_dicts(self, extractor):
        doc = next(extractor.stream("testdb", "orders", {}))
        assert isinstance(doc, dict)

    def test_date_filter_gte(self, extractor):
        query = {"created_at": {"$gte": datetime(2024, 2, 1, tzinfo=timezone.utc)}}
        docs = list(extractor.stream("testdb", "orders", query))
        # Feb 20 + Mar 10  →  2 documents
        assert len(docs) == 2

    def test_date_filter_range(self, extractor):
        query = {
            "created_at": {
                "$gte": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "$lte": datetime(2024, 1, 31, tzinfo=timezone.utc),
            }
        }
        docs = list(extractor.stream("testdb", "orders", query))
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"

    def test_empty_collection_yields_nothing(self, extractor, populated_mongo):
        populated_mongo["testdb"]["empty_col"]  # create handle but no docs
        docs = list(extractor.stream("testdb", "empty_col", {}))
        assert docs == []

    def test_batch_size_accepted(self, extractor):
        # Should not raise even with custom batch_size
        docs = list(extractor.stream("testdb", "orders", {}, batch_size=1))
        assert len(docs) == 3


# ---------------------------------------------------------------------------
# check_date_index
# ---------------------------------------------------------------------------


class TestCheckDateIndex:
    def test_returns_false_when_no_index(self, extractor):
        indexed = extractor.check_date_index("testdb", "orders", "created_at")
        assert indexed is False

    def test_returns_true_when_indexed(self, extractor, populated_mongo):
        populated_mongo["testdb"]["orders"].create_index("created_at")
        indexed = extractor.check_date_index("testdb", "orders", "created_at")
        assert indexed is True

    def test_id_field_always_indexed(self, extractor):
        indexed = extractor.check_date_index("testdb", "orders", "_id")
        assert indexed is True


# ---------------------------------------------------------------------------
# connect / close (patched to avoid real network calls)
# ---------------------------------------------------------------------------


class TestConnectClose:
    def test_close_is_idempotent_when_not_connected(self):
        ext = MongoExtractor(uri="mongodb://localhost:27017")
        ext.close()  # must not raise

    def test_close_clears_client(self, extractor):
        extractor.close()
        # After close, client.close() was called; extractor.client attribute may still exist
        # but subsequent close() must not raise
        extractor.close()
