"""Iceberg writer — writes Arrow tables to an Iceberg REST catalog (Nessie, no auth)."""
from __future__ import annotations

from typing import Optional

import pyarrow as pa
import structlog
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.transforms import DayTransform

log = structlog.get_logger()


class IcebergWriter:
    """Write PyArrow Tables to Iceberg tables via a REST catalog.

    Table naming: ``{namespace}.{database}__{collection}`` (double underscore).
    Partitioning: ``day(date_field)`` when *date_field* is provided.
    Connects to Nessie without authentication.
    S3 credentials are resolved via the standard boto3 chain
    (env vars, IAM role, ``~/.aws/credentials``).
    """

    def __init__(
        self,
        catalog_uri: str,
        warehouse: str,
        namespace: str = "default",
    ) -> None:
        catalog_props: dict[str, str] = {
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": warehouse,
        }

        self.catalog = load_catalog("rest", **catalog_props)
        self.namespace = namespace
        self.log = log.bind(component="iceberg_writer")
        self._ensure_namespace()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(
        self,
        arrow_table: pa.Table,
        database: str,
        collection: str,
        date_field: Optional[str] = None,
    ) -> None:
        """Append *arrow_table* to the Iceberg table for (database, collection).

        Creates the Iceberg table (with schema and partition spec) on first write.
        """
        identifier = f"{self.namespace}.{database}__{collection}"

        table = self._get_or_create_table(identifier, arrow_table.schema, date_field)
        table.append(arrow_table)

        self.log.info(
            "iceberg_write",
            identifier=identifier,
            rows=len(arrow_table),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_namespace(self) -> None:
        try:
            self.catalog.create_namespace(self.namespace)
        except NamespaceAlreadyExistsError:
            pass

    def _get_or_create_table(
        self,
        identifier: str,
        arrow_schema: pa.Schema,
        date_field: Optional[str],
    ):
        try:
            return self.catalog.load_table(identifier)
        except NoSuchTableError:
            pass

        # Create the table with the Arrow schema — pyiceberg converts it
        # internally and assigns field IDs automatically.
        table = self.catalog.create_table(
            identifier=identifier,
            schema=arrow_schema,
        )
        self.log.info("iceberg_table_created", identifier=identifier)

        # Add day partitioning if requested (requires the Iceberg field ID
        # that was assigned during create_table).
        if date_field:
            iceberg_schema = table.schema()
            field_id = None
            for field in iceberg_schema.fields:
                if field.name == date_field:
                    field_id = field.field_id
                    break

            if field_id is not None:
                with table.update_spec() as update:
                    update.add_field(
                        source_column_name=date_field,
                        transform=DayTransform(),
                    )
                self.log.info("iceberg_partition_added", date_field=date_field)
            else:
                self.log.warning(
                    "iceberg_date_field_not_found",
                    date_field=date_field,
                    identifier=identifier,
                )

        return table
