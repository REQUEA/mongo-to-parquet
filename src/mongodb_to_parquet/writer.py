"""Parquet writing with atomic writes and UUID-based part naming."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from .utils import build_partition_path, format_bytes

log = structlog.get_logger()


class ParquetWriter:
    """Write PyArrow Tables to Parquet files.

    Features
    --------
    - Atomic writes: data is written to a ``.tmp`` file first, then renamed —
      no partial files are ever visible.
    - UUID-based part filenames (``part-{uid}.parquet``) so incremental runs
      never collide with previously written files.
    - Configurable compression (snappy, gzip, zstd).
    """

    def __init__(
        self,
        output_dir: str | Path,
        compression: str = "zstd",
        compression_level: int = 3,
        overwrite: bool = False,
        partition_pattern: Optional[str] = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.compression = compression
        self.compression_level=compression_level
        self.overwrite = overwrite
        self.partition_pattern = partition_pattern
        self.log = log.bind(component="writer")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_partition_path(
        self,
        database: str,
        collection: str,
        date: Optional[datetime] = None,
    ) -> Path:
        """Return the directory path for a partition (does not create it)."""
        return build_partition_path(
            self.output_dir,
            database,
            collection,
            date or datetime.now(timezone.utc),
            self.partition_pattern,
        )

    def write(
        self,
        table: pa.Table,
        partition_path: Path,
    ) -> Path:
        """Write *table* to ``{partition_path}/part-{uid}.parquet`` atomically.

        Each call generates a unique filename via UUID so incremental runs
        never collide with previously written files.

        Returns the path of the written file.
        """
        partition_path.mkdir(parents=True, exist_ok=True)

        dest = partition_path / f"part-{uuid4().hex[:8]}.parquet"

        self._atomic_write(dest, table)

        size = dest.stat().st_size
        self.log.info(
            "file_written",
            path=str(dest),
            rows=len(table),
            size=format_bytes(size),
            compression=self.compression,
            compression_level=self.compression_level
        )
        return dest

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _atomic_write(self, dest: Path, table: pa.Table) -> None:
        """Write to a .tmp file then rename — guarantees no partial files."""
        tmp = dest.with_suffix(".parquet.tmp")
        try:
            pq.write_table(
                table,
                tmp,
                compression=self.compression,
                compression_level=self.compression_level,
                use_dictionary=True,
                write_statistics=True,
            )
            tmp.rename(dest)
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise
