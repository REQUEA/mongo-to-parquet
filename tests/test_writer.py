"""Unit tests for writer.py."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mongodb_to_parquet.writer import ParquetWriter


@pytest.fixture
def writer(tmp_output_dir):
    return ParquetWriter(output_dir=tmp_output_dir)


# ---------------------------------------------------------------------------
# get_partition_path
# ---------------------------------------------------------------------------


class TestGetPartitionPath:
    def test_default_structure(self, writer, tmp_output_dir):
        date = datetime(2024, 3, 5, tzinfo=timezone.utc)
        path = writer.get_partition_path("mydb", "orders", date)
        expected = (
            tmp_output_dir / "mydb" / "orders" / "year=2024" / "month=03" / "day=05"
        )
        assert path == expected

    def test_zero_padded_month_and_day(self, writer, tmp_output_dir):
        date = datetime(2024, 1, 7, tzinfo=timezone.utc)
        path = writer.get_partition_path("db", "col", date)
        assert "month=01" in str(path)
        assert "day=07" in str(path)

    def test_custom_pattern(self, tmp_output_dir):
        w = ParquetWriter(
            output_dir=tmp_output_dir,
            partition_pattern="{database}/{collection}/{year}-{month}",
        )
        date = datetime(2024, 6, 15, tzinfo=timezone.utc)
        path = w.get_partition_path("mydb", "orders", date)
        assert path == tmp_output_dir / "mydb" / "orders" / "2024-06"

    def test_fallback_to_now_when_no_date(self, writer):
        # Should not raise even without a date
        path = writer.get_partition_path("db", "col")
        assert path.name.startswith("day=")


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_creates_parquet_file(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "db" / "col" / "year=2024" / "month=01" / "day=15"
        dest = writer.write(sample_arrow_table, partition)
        assert dest.exists()
        assert dest.suffix == ".parquet"

    def test_file_has_part_prefix(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "p"
        dest = writer.write(sample_arrow_table, partition)
        assert dest.name.startswith("part-")
        assert dest.suffix == ".parquet"

    def test_multiple_writes_create_distinct_files(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "p"
        dest0 = writer.write(sample_arrow_table, partition)
        dest1 = writer.write(sample_arrow_table, partition)
        assert dest0 != dest1
        assert dest0.exists()
        assert dest1.exists()

    def test_creates_intermediate_directories(self, writer, sample_arrow_table, tmp_output_dir):
        deep = tmp_output_dir / "a" / "b" / "c"
        writer.write(sample_arrow_table, deep)
        assert deep.exists()

    def test_incremental_runs_do_not_collide(self, sample_arrow_table, tmp_output_dir):
        # Simulate two separate export runs into the same partition directory.
        # UUID filenames mean each run creates new files without conflict.
        w = ParquetWriter(output_dir=tmp_output_dir)
        partition = tmp_output_dir / "part"
        dest0 = w.write(sample_arrow_table, partition)
        dest1 = w.write(sample_arrow_table, partition)
        assert dest0.exists()
        assert dest1.exists()
        assert dest0 != dest1

    def test_written_file_is_readable(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "read_test"
        dest = writer.write(sample_arrow_table, partition)
        read_back = pq.read_table(dest)
        assert len(read_back) == len(sample_arrow_table)
        assert set(read_back.column_names) == set(sample_arrow_table.column_names)

    def test_data_integrity(self, writer, tmp_output_dir):
        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        partition = tmp_output_dir / "integrity"
        dest = writer.write(table, partition)
        read_back = pq.read_table(dest)
        assert read_back["x"].to_pylist() == [1, 2, 3]
        assert read_back["y"].to_pylist() == ["a", "b", "c"]

    def test_returns_path_object(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "ret"
        dest = writer.write(sample_arrow_table, partition)
        assert isinstance(dest, Path)


# ---------------------------------------------------------------------------
# Atomic write (no .tmp file left behind)
# ---------------------------------------------------------------------------


class TestAtomicWrite:
    def test_no_tmp_file_left_on_success(self, writer, sample_arrow_table, tmp_output_dir):
        partition = tmp_output_dir / "atomic"
        writer.write(sample_arrow_table, partition)
        tmp_files = list(partition.glob("*.tmp"))
        assert tmp_files == []

    def test_compression_snappy(self, tmp_output_dir, sample_arrow_table):
        w = ParquetWriter(output_dir=tmp_output_dir, compression="snappy")
        partition = tmp_output_dir / "snappy"
        dest = w.write(sample_arrow_table, partition)
        assert dest.exists()

    def test_compression_zstd(self, tmp_output_dir, sample_arrow_table):
        w = ParquetWriter(output_dir=tmp_output_dir, compression="zstd")
        partition = tmp_output_dir / "zstd"
        dest = w.write(sample_arrow_table, partition)
        assert dest.exists()
