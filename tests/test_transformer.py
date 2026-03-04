"""Unit tests for transformer.py."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from bson import Decimal128, ObjectId

from mongodb_to_parquet.transformer import DocumentTransformer


@pytest.fixture
def t():
    return DocumentTransformer()


# ---------------------------------------------------------------------------
# Type conversion
# ---------------------------------------------------------------------------


class TestTypeConversion:
    def test_objectid_to_str(self, t):
        oid = ObjectId("65a000000000000000000001")
        result = t.transform({"_id": oid})
        assert result["_id"] == "65a000000000000000000001"
        assert isinstance(result["_id"], str)

    def test_decimal128_to_float(self, t):
        result = t.transform({"amount": Decimal128("99.99")})
        assert result["amount"] == pytest.approx(99.99)
        assert isinstance(result["amount"], float)

    def test_datetime_preserved(self, t):
        dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
        result = t.transform({"created_at": dt})
        assert result["created_at"] == dt

    def test_string_preserved(self, t):
        result = t.transform({"name": "Alice"})
        assert result["name"] == "Alice"

    def test_int_preserved(self, t):
        result = t.transform({"count": 42})
        assert result["count"] == 42

    def test_list_of_objectids(self, t):
        oid1 = ObjectId("65a000000000000000000001")
        oid2 = ObjectId("65a000000000000000000002")
        result = t.transform({"ids": [oid1, oid2]})
        assert result["ids"] == [str(oid1), str(oid2)]

    def test_nested_objectid_in_dict(self, t):
        oid = ObjectId("65a000000000000000000003")
        # Nested dict passed as a leaf value (beyond flatten depth)
        result = t._convert({"ref": oid})
        assert result == {"ref": str(oid)}

    def test_none_preserved(self, t):
        result = t.transform({"optional": None})
        assert result["optional"] is None


# ---------------------------------------------------------------------------
# Flattening
# ---------------------------------------------------------------------------


class TestFlatten:
    def test_top_level_fields_unchanged(self, t):
        result = t.transform({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}

    def test_one_level_nested(self, t):
        result = t.transform({"meta": {"source": "A", "version": 1}})
        assert result["meta.source"] == "A"
        assert result["meta.version"] == 1
        assert "meta" not in result

    def test_two_levels_nested(self, t):
        result = t.transform({"a": {"b": {"c": "leaf"}}})
        assert result["a.b.c"] == "leaf"

    def test_depth_limit_stops_at_flatten_depth(self):
        t2 = DocumentTransformer(flatten_depth=2)
        result = t2.transform({"a": {"b": {"c": {"d": "deep"}}}})
        # depth 0→a, depth 1→a.b, depth 2→stop: a.b.c is a dict leaf
        assert "a.b.c" in result
        assert isinstance(result["a.b.c"], dict)
        assert result["a.b.c"] == {"d": "deep"}

    def test_depth_zero_no_flattening(self):
        t0 = DocumentTransformer(flatten_depth=0)
        result = t0.transform({"meta": {"source": "A"}})
        # No flattening at all
        assert "meta" in result
        assert isinstance(result["meta"], dict)

    def test_empty_document(self, t):
        assert t.transform({}) == {}

    def test_field_naming_no_leading_dot(self, t):
        result = t.transform({"x": {"y": 1}})
        assert "x.y" in result
        assert ".x.y" not in result


# ---------------------------------------------------------------------------
# to_arrow
# ---------------------------------------------------------------------------


class TestToArrow:
    def test_basic_conversion(self, t):
        docs = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        table = t.to_arrow(docs)
        assert isinstance(table, pa.Table)
        assert len(table) == 2
        assert set(table.column_names) == {"name", "age"}

    def test_schema_inferred_on_first_call(self, t):
        assert t.schema is None
        t.to_arrow([{"x": 1}])
        assert t.schema is not None

    def test_schema_reused_on_second_call(self, t):
        t.to_arrow([{"x": 1}])
        first_schema = t.schema
        t.to_arrow([{"x": 2}])
        assert t.schema is first_schema  # same object

    def test_schema_drift_log_mode_does_not_raise(self):
        t_log = DocumentTransformer(on_schema_drift="log")
        t_log.to_arrow([{"x": 1}])
        # Different schema — should warn but not raise
        result = t_log.to_arrow([{"y": "hello"}])
        assert isinstance(result, pa.Table)

    def test_schema_drift_strict_mode_raises(self):
        t_strict = DocumentTransformer(on_schema_drift="strict")
        t_strict.to_arrow([{"x": 1}])
        with pytest.raises(ValueError, match="Schema drift"):
            t_strict.to_arrow([{"y": "hello"}])

    def test_single_document(self, t):
        table = t.to_arrow([{"only": "one"}])
        assert len(table) == 1

    def test_rows_match_input(self, t):
        docs = [{"i": i} for i in range(50)]
        table = t.to_arrow(docs)
        assert len(table) == 50


# ---------------------------------------------------------------------------
# Schema merge mode
# ---------------------------------------------------------------------------


class TestSchemaMerge:
    def test_new_field_added_to_schema(self):
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1}])
        result = tm.to_arrow([{"x": 2, "y": "hello"}])
        assert "y" in result.schema.names
        assert "x" in result.schema.names

    def test_stored_schema_updated_after_merge(self):
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1}])
        tm.to_arrow([{"x": 2, "y": "hello"}])
        assert "y" in tm.schema.names

    def test_missing_field_padded_with_nulls(self):
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1, "y": "a"}])
        # Second batch is missing "y"
        result = tm.to_arrow([{"x": 2}])
        assert "y" in result.schema.names
        assert result.column("y")[0].as_py() is None

    def test_both_directions(self):
        """Batch 2 adds a field and drops a field — both handled."""
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1, "dropped": True}])
        result = tm.to_arrow([{"x": 2, "added": 99}])
        assert "dropped" in result.schema.names
        assert "added" in result.schema.names
        assert result.column("dropped")[0].as_py() is None
        assert result.column("added")[0].as_py() == 99

    def test_type_conflict_falls_back_to_stored_type(self):
        """When a field changes type incompatibly, stored type wins; column is null."""
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1}])  # x is int64
        # x is now a string — incompatible with stored int64
        result = tm.to_arrow([{"x": "not-an-int"}])
        assert result.schema.field("x").type == pa.int64()
        assert result.column("x")[0].as_py() is None

    def test_merged_schema_used_for_subsequent_batches(self):
        """After merge, the third batch is aligned to the merged schema."""
        tm = DocumentTransformer(on_schema_drift="merge")
        tm.to_arrow([{"x": 1}])
        tm.to_arrow([{"x": 2, "y": "a"}])
        # Third batch — should silently include "y" column
        result = tm.to_arrow([{"x": 3, "y": "b"}])
        assert "y" in result.schema.names
        assert result.column("y")[0].as_py() == "b"

    def test_is_default_mode(self):
        tm = DocumentTransformer()
        assert tm.on_schema_drift == "merge"
