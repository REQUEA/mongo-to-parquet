"""Document normalization: BSON type conversion and schema handling."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa
import structlog
from bson import Decimal128, ObjectId

log = structlog.get_logger()


class DocumentTransformer:
    """Convert raw MongoDB documents into PyArrow-compatible records.

    Responsibilities
    ----------------
    - Flatten nested dicts up to *flatten_depth* levels (dot-notation keys).
    - Convert ``ObjectId`` → ``str``, ``Decimal128`` → ``float``.
    - Leave ``datetime``, ``str``, ``int``, ``float``, ``None`` untouched.
    - Infer a PyArrow schema from the first batch; enforce it on subsequent
      batches (behaviour on drift controlled by *on_schema_drift*).
    """

    def __init__(
        self,
        flatten_depth: int = 2,
        on_schema_drift: str = "merge",  # "merge" | "log" | "strict"
    ) -> None:
        self.flatten_depth = flatten_depth
        self.on_schema_drift = on_schema_drift
        self.schema: Optional[pa.Schema] = None
        self.skip_count: int = 0
        self.log = log.bind(component="transformer")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transform(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Return a flattened, type-converted copy of *doc*.

        Never modifies the original document.
        """
        result: Dict[str, Any] = {}
        self._flatten_into(doc, "", 0, result)
        return result

    def to_arrow(self, documents: List[Dict[str, Any]]) -> pa.Table:
        """Convert a list of already-transformed documents to a PyArrow Table.

        - First call: schema is inferred and stored.
        - Subsequent calls: table is cast to the stored schema.
          On cast failure the behaviour depends on *on_schema_drift*:

          ``merge``  — union the stored and new schemas; pad missing columns
                       with nulls; update stored schema (default).
          ``log``    — log a warning and return the table as-is.
          ``strict`` — raise ``ValueError``.
        """
        table = pa.Table.from_pylist(documents)

        if self.schema is None:
            self.schema = table.schema
            self.log.info("schema_inferred", fields=len(self.schema))
            return table

        try:
            return table.cast(self.schema)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
            if self.on_schema_drift == "strict":
                raise ValueError(f"Schema drift detected: {exc}") from exc
            if self.on_schema_drift == "merge":
                return self._merge_schema(table)
            # "log" mode: warn and return uncast table
            self.log.warning(
                "schema_drift",
                error=str(exc),
                action="returning_table_with_inferred_schema",
            )
            return table

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _merge_schema(self, table: pa.Table) -> pa.Table:
        """Union the stored schema with *table*'s schema and return an aligned table.

        - New fields in *table* are added to the stored schema.
        - Fields present in the stored schema but missing from *table* are
          padded with a null column of the stored type.
        - Type conflicts (e.g. int64 vs string for the same field) fall back to
          ``_manual_merge``, which keeps the stored type and fills conflicting
          columns with nulls.
        - Updates ``self.schema`` to the merged schema.
        """
        old_schema = self.schema

        try:
            merged = pa.unify_schemas([old_schema, table.schema])
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
            # Type conflict — keep stored types, add new fields as nullable
            merged = self._manual_merge(old_schema, table.schema)
            self.log.warning(
                "schema_drift_type_conflict",
                error=str(exc),
                action="keeping_stored_types_for_conflicting_fields",
            )

        # Build arrays aligned to merged schema
        arrays: Dict[str, pa.Array] = {}
        for field in merged:
            if field.name in table.schema.names:
                col = table.column(field.name)
                try:
                    arrays[field.name] = col.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    # Cannot cast this column — fill with nulls
                    arrays[field.name] = pa.nulls(len(table), type=field.type)
            else:
                arrays[field.name] = pa.nulls(len(table), type=field.type)

        added = len(merged) - len(old_schema)
        if added:
            self.log.info("schema_merged", added_fields=added, total_fields=len(merged))

        self.schema = merged
        return pa.table(arrays, schema=merged)

    @staticmethod
    def _manual_merge(a: pa.Schema, b: pa.Schema) -> pa.Schema:
        """Return a schema with all fields from *a* and *b*, preferring *a*'s types on conflict."""
        fields: Dict[str, pa.Field] = {f.name: f for f in a}
        for field in b:
            if field.name not in fields:
                fields[field.name] = field
            # else: keep type from *a*
        return pa.schema(list(fields.values()))

    def _flatten_into(
        self,
        doc: Dict[str, Any],
        prefix: str,
        depth: int,
        result: Dict[str, Any],
    ) -> None:
        """Flatten *doc* and type-convert values into *result* in a single pass.

        Combines the previous ``_flatten`` + ``_convert`` steps to avoid
        allocating an intermediate dict per document.
        """
        for key, value in doc.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict) and depth < self.flatten_depth:
                self._flatten_into(value, full_key, depth + 1, result)
            else:
                result[full_key] = self._convert(value)

    def _convert(self, value: Any) -> Any:
        """Recursively convert BSON types to plain Python equivalents."""
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, Decimal128):
            return float(value.to_decimal())
        if isinstance(value, list):
            return [self._convert(v) for v in value]
        if isinstance(value, dict):
            return {k: self._convert(v) for k, v in value.items()}
        return value
