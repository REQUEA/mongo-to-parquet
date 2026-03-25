"""Iceberg writer — writes Arrow tables to an Iceberg REST catalog (Nessie, no auth)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pyarrow as pa
import structlog
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import AlwaysTrue
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
        s3_endpoint: Optional[str] = None,
        s3_access_key_id: Optional[str] = None,
        s3_secret_access_key: Optional[str] = None,
        s3_region: Optional[str] = None,
    ) -> None:
        catalog_props: dict[str, str] = {
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": warehouse,
        }
        if s3_endpoint:
            catalog_props["s3.endpoint"] = s3_endpoint
            # MinIO and most S3-compatible stores need path-style access
            catalog_props["s3.path-style-access"] = "true"
        if s3_access_key_id:
            catalog_props["s3.access-key-id"] = s3_access_key_id
        if s3_secret_access_key:
            catalog_props["s3.secret-access-key"] = s3_secret_access_key
        if s3_region:
            catalog_props["s3.region"] = s3_region

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
        identifier = f"{self.namespace}.{database}__{collection.lower()}"

        table = self._get_or_create_table(identifier, arrow_table.schema, date_field)
        self._evolve_schema(table, arrow_table.schema)
        table.append(arrow_table)

        self.log.info(
            "iceberg_write",
            identifier=identifier,
            rows=len(arrow_table),
        )

    def get_resume_date(
        self,
        database: str,
        collection: str,
        date_field: str,
    ) -> Optional[datetime]:
        """Return the max value of *date_field* already in the Iceberg table.

        Returns ``None`` if the table does not exist or is empty.
        """
        identifier = f"{self.namespace}.{database}__{collection.lower()}"
        try:
            table = self.catalog.load_table(identifier)
        except NoSuchTableError:
            return None

        scan = table.scan(selected_fields=(date_field,), row_filter=AlwaysTrue())
        arrow = scan.to_arrow()
        if len(arrow) == 0:
            return None

        col = arrow.column(date_field)
        max_val = pa.compute.max(col).as_py()
        self.log.info(
            "iceberg_resume_date",
            identifier=identifier,
            date_field=date_field,
            resume_after=str(max_val),
        )
        return max_val

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evolve_schema(self, table, arrow_schema: pa.Schema) -> None:
        """Add any new columns from *arrow_schema* that are missing in the Iceberg table."""
        iceberg_names = {field.name for field in table.schema().fields}
        new_columns = [f.name for f in arrow_schema if f.name not in iceberg_names]
        if not new_columns:
            return

        with table.update_schema() as update:
            update.union_by_name(arrow_schema)

        self.log.info(
            "iceberg_schema_evolved",
            new_fields=new_columns,
        )

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
