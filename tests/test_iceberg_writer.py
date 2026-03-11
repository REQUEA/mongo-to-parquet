"""Unit tests for IcebergWriter (mocked REST catalog)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pytest
import pyarrow as pa


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_arrow_table(with_ts: bool = False) -> pa.Table:
    data: dict = {"id": pa.array([1, 2, 3]), "value": pa.array(["a", "b", "c"])}
    if with_ts:
        import pandas as pd
        data["created_at"] = pa.array(
            pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"])
        )
    return pa.table(data)


def _make_iceberg_schema(arrow_schema: pa.Schema):
    """Build a minimal fake Iceberg schema with named fields."""
    fields = []
    for i, name in enumerate(arrow_schema.names):
        f = MagicMock()
        f.name = name
        f.field_id = i + 1
        fields.append(f)
    schema = MagicMock()
    schema.fields = fields
    return schema


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_catalog():
    """Return a mock Iceberg catalog."""
    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    catalog = MagicMock()
    # Namespace already exists by default → raise NamespaceAlreadyExistsError
    catalog.create_namespace.side_effect = NamespaceAlreadyExistsError("default")
    return catalog


@pytest.fixture()
def writer(mock_catalog):
    """Return an IcebergWriter with a mocked catalog (no real HTTP calls)."""
    from mongodb_to_parquet.iceberg_writer import IcebergWriter
    with patch("mongodb_to_parquet.iceberg_writer.load_catalog", return_value=mock_catalog):
        w = IcebergWriter(
            catalog_uri="http://localhost:19120/api/v1",
            warehouse="s3://bucket/warehouse",
            namespace="test_ns",
        )
    return w, mock_catalog


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIcebergWriterInit:
    def test_creates_namespace_if_missing(self):
        from mongodb_to_parquet.iceberg_writer import IcebergWriter
        catalog = MagicMock()
        catalog.create_namespace.return_value = None  # success

        with patch("mongodb_to_parquet.iceberg_writer.load_catalog", return_value=catalog):
            IcebergWriter("http://nessie/api", "s3://bkt/wh", namespace="new_ns")

        catalog.create_namespace.assert_called_once_with("new_ns")

    def test_tolerates_existing_namespace(self, writer):
        # Fixture already exercises the NamespaceAlreadyExistsError path;
        # just assert the writer was created successfully.
        w, _ = writer
        assert w.namespace == "test_ns"


class TestIcebergWriterWrite:
    def test_appends_to_existing_table(self, writer):
        w, catalog = writer
        table_mock = MagicMock()
        catalog.load_table.return_value = table_mock

        arrow_table = _make_arrow_table()
        w.write(arrow_table, "mydb", "orders")

        catalog.load_table.assert_called_once_with("test_ns.mydb__orders")
        table_mock.append.assert_called_once_with(arrow_table)
        catalog.create_table.assert_not_called()

    def test_creates_table_when_missing(self, writer):
        from pyiceberg.exceptions import NoSuchTableError

        w, catalog = writer
        catalog.load_table.side_effect = NoSuchTableError("test_ns.mydb__orders")
        new_table = MagicMock()
        catalog.create_table.return_value = new_table

        arrow_table = _make_arrow_table()
        iceberg_schema = _make_iceberg_schema(arrow_table.schema)

        with patch("mongodb_to_parquet.iceberg_writer.Schema") as MockSchema:
            MockSchema.from_arrow.return_value = iceberg_schema
            w.write(arrow_table, "mydb", "orders")

        catalog.create_table.assert_called_once()
        new_table.append.assert_called_once_with(arrow_table)

    def test_creates_table_with_day_partition_when_date_field_found(self, writer):
        from pyiceberg.exceptions import NoSuchTableError

        w, catalog = writer
        catalog.load_table.side_effect = NoSuchTableError("test_ns.mydb__events")
        new_table = MagicMock()
        catalog.create_table.return_value = new_table

        arrow_table = _make_arrow_table(with_ts=True)
        iceberg_schema = _make_iceberg_schema(arrow_table.schema)

        with patch("mongodb_to_parquet.iceberg_writer.Schema") as MockSchema, \
             patch("mongodb_to_parquet.iceberg_writer.PartitionSpec") as MockPartitionSpec, \
             patch("mongodb_to_parquet.iceberg_writer.PartitionField") as MockPartitionField:
            MockSchema.from_arrow.return_value = iceberg_schema
            MockPartitionField.return_value = MagicMock()
            MockPartitionSpec.return_value = MagicMock()

            w.write(arrow_table, "mydb", "events", date_field="created_at")

        # PartitionField should have been instantiated (day transform)
        MockPartitionField.assert_called_once()
        call_kwargs = MockPartitionField.call_args
        assert call_kwargs.kwargs.get("name") == "created_at_day"

    def test_creates_table_without_partition_when_date_field_missing_from_schema(self, writer):
        from pyiceberg.exceptions import NoSuchTableError

        w, catalog = writer
        catalog.load_table.side_effect = NoSuchTableError("test_ns.mydb__orders")
        new_table = MagicMock()
        catalog.create_table.return_value = new_table

        arrow_table = _make_arrow_table()  # no timestamp column
        iceberg_schema = _make_iceberg_schema(arrow_table.schema)

        with patch("mongodb_to_parquet.iceberg_writer.Schema") as MockSchema, \
             patch("mongodb_to_parquet.iceberg_writer.PartitionField") as MockPartitionField:
            MockSchema.from_arrow.return_value = iceberg_schema

            w.write(arrow_table, "mydb", "orders", date_field="nonexistent_field")

        # PartitionField should NOT have been called because field is absent
        MockPartitionField.assert_not_called()

    def test_table_identifier_uses_double_underscore(self, writer):
        w, catalog = writer
        table_mock = MagicMock()
        catalog.load_table.return_value = table_mock

        w.write(_make_arrow_table(), "production_db", "user_events")

        catalog.load_table.assert_called_once_with("test_ns.production_db__user_events")
