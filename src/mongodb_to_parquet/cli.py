"""CLI entry point — uses Typer.

All flags can also be set via env vars prefixed with MTP_ (batch_size →
MTP_BATCH_SIZE) or the legacy MONGO_URI / OUTPUT_DIR / LOG_LEVEL / LOG_FORMAT.

Pass --config <path.yaml> to load defaults from a YAML file; CLI flags
always take priority over config file values.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import typer
import yaml

from .extractor import MongoExtractor
from .transformer import DocumentTransformer
from .utils import parse_date, setup_logging
from .writer import ParquetWriter

app = typer.Typer(
    name="mongo-to-parquet",
    help="Export MongoDB collections to Parquet files.",
    no_args_is_help=True,
)


def _load_yaml_config(path: Path) -> dict:
    """Load and return config dict from a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


@app.command()
def export(
    config: Optional[Path] = typer.Option(
        None, "--config", "-C", help="Path to YAML config file (CLI flags override it)"
    ),
    uri: Optional[str] = typer.Option(
        None, envvar="MONGO_URI", help="MongoDB connection URI"
    ),
    databases: Optional[List[str]] = typer.Option(
        None, "--databases", "-d",
        help="Databases to include (default: all non-system databases)",
    ),
    collections: Optional[List[str]] = typer.Option(
        None, "--collections", "-c",
        help="Collections to export (repeat flag for multiple)",
    ),
    date_field: Optional[str] = typer.Option(
        None, help="Document field used for date partitioning"
    ),
    start_date: Optional[str] = typer.Option(
        None, help="ISO start date, inclusive (requires --date-field)"
    ),
    end_date: Optional[str] = typer.Option(
        None, help="ISO end date, inclusive (requires --date-field)"
    ),
    output_dir: Optional[Path] = typer.Option(
        None, envvar="OUTPUT_DIR", help="Root output directory"
    ),
    partition_pattern: Optional[str] = typer.Option(
        None,
        help=(
            "Custom partition path template, e.g. '{database}/{collection}/{year}-{month}'. "
            "Available variables: {database}, {collection}, {year}, {month}, {day}, {hour}."
        ),
    ),
    batch_size: Optional[int] = typer.Option(
        None, envvar="MTP_BATCH_SIZE", help="MongoDB cursor batch size (default: 1000)"
    ),
    compression: Optional[str] = typer.Option(
        None, help="Parquet compression codec: snappy, gzip, zstd (default: zstd)"
    ),
    compression_level: Optional[int] = typer.Option(
        None, help="Zstd compression level (default: 3)"
    ),
    overwrite: Optional[bool] = typer.Option(
        None, help="Overwrite existing Parquet files (default: false)"
    ),
    dry_run: Optional[bool] = typer.Option(
        None, help="Simulate without writing any files (default: false)"
    ),
    iceberg: bool = typer.Option(False, "--iceberg", help="Write to Iceberg table (REST catalog)"),
    catalog_uri: Optional[str] = typer.Option(
        None, "--catalog-uri", envvar="MTP_CATALOG_URI", help="Iceberg REST catalog URI"
    ),
    warehouse: Optional[str] = typer.Option(
        None, "--warehouse", envvar="MTP_WAREHOUSE",
        help="S3 warehouse URI, e.g. s3://bucket/warehouse",
    ),
    namespace: str = typer.Option("default", "--namespace", envvar="MTP_NAMESPACE",
                                  help="Iceberg namespace (default: default)"),
    s3_endpoint: Optional[str] = typer.Option(
        None, "--s3-endpoint", envvar="MTP_S3_ENDPOINT",
        help="S3-compatible endpoint URL (e.g. http://minio:9000)",
    ),
    s3_access_key_id: Optional[str] = typer.Option(
        None, "--s3-access-key-id", envvar="AWS_ACCESS_KEY_ID",
        help="S3 access key ID",
    ),
    s3_secret_access_key: Optional[str] = typer.Option(
        None, "--s3-secret-access-key", envvar="AWS_SECRET_ACCESS_KEY",
        help="S3 secret access key",
    ),
    s3_region: Optional[str] = typer.Option(
        None, "--s3-region", envvar="AWS_DEFAULT_REGION",
        help="S3 region (e.g. us-east-1)",
    ),
    log_level: Optional[str] = typer.Option(None, envvar="LOG_LEVEL"),
    log_format: Optional[str] = typer.Option(None, envvar="LOG_FORMAT"),
) -> None:
    """Export one or more MongoDB collections to Parquet files."""
    import structlog as _sl

    # ------------------------------------------------------------------
    # Load YAML config and merge (CLI flags take priority)
    # ------------------------------------------------------------------
    cfg = _load_yaml_config(config) if config else {}
    mongo_cfg = cfg.get("mongodb", {})
    export_cfg = cfg.get("export", {})
    filters_cfg = cfg.get("filters", {})
    transformer_cfg = cfg.get("transformer", {})
    logging_cfg = cfg.get("logging", {})
    iceberg_cfg = cfg.get("iceberg", {})

    # Required values: CLI → config → None (validated below)
    uri = uri or mongo_cfg.get("uri")
    collections = collections or export_cfg.get("collections") or []
    output_dir = output_dir or (
        Path(export_cfg["output_dir"]) if "output_dir" in export_cfg else None
    )

    # Iceberg options: CLI → config
    iceberg = iceberg or iceberg_cfg.get("enabled", False)
    catalog_uri = catalog_uri or iceberg_cfg.get("catalog_uri")
    warehouse = warehouse or iceberg_cfg.get("warehouse")
    namespace = namespace if namespace != "default" else iceberg_cfg.get("namespace", namespace)
    s3_endpoint = s3_endpoint or iceberg_cfg.get("s3_endpoint")
    s3_access_key_id = s3_access_key_id or iceberg_cfg.get("s3_access_key_id")
    s3_secret_access_key = s3_secret_access_key or iceberg_cfg.get("s3_secret_access_key")
    s3_region = s3_region or iceberg_cfg.get("s3_region")

    # Optional values: CLI → config → hardcoded default
    databases = databases or export_cfg.get("databases")
    date_field = date_field or filters_cfg.get("date_field")
    start_date = start_date or filters_cfg.get("start_date")
    end_date = end_date or filters_cfg.get("end_date")
    partition_pattern = partition_pattern or export_cfg.get("partition_pattern")

    batch_size = batch_size if batch_size is not None else export_cfg.get("batch_size", 1000)
    compression = compression if compression is not None else export_cfg.get("compression", "zstd")
    compression_level = compression_level if compression_level is not None else export_cfg.get("compression_level", 3)
    overwrite = overwrite if overwrite is not None else export_cfg.get("overwrite", False)
    dry_run = dry_run if dry_run is not None else export_cfg.get("dry_run", False)
    log_level = log_level if log_level is not None else logging_cfg.get("level", "INFO")
    log_format = log_format if log_format is not None else logging_cfg.get("format", "text")

    # Config-only params (not exposed as CLI flags)
    flatten_depth: int = transformer_cfg.get("flatten_depth", 2)
    on_schema_drift: str = transformer_cfg.get("on_schema_drift", "merge")
    connection_timeout_ms: int = mongo_cfg.get("connection_timeout_ms", 30_000)
    socket_timeout_ms: int = mongo_cfg.get("socket_timeout_ms", 60_000)
    read_preference: str = mongo_cfg.get("read_preference", "secondaryPreferred")
    max_pool_size: int = mongo_cfg.get("max_pool_size", 10)

    # ------------------------------------------------------------------
    # Validate required values (may come from config or CLI)
    # ------------------------------------------------------------------
    if not uri:
        typer.echo(
            "Error: --uri is required (or set mongodb.uri in config file)", err=True
        )
        raise typer.Exit(2)
    if not collections:
        typer.echo(
            "Error: --collections is required (or set export.collections in config file)",
            err=True,
        )
        raise typer.Exit(2)
    if iceberg:
        if not catalog_uri:
            typer.echo("Error: --catalog-uri is required in Iceberg mode", err=True)
            raise typer.Exit(2)
        if not warehouse:
            typer.echo("Error: --warehouse is required in Iceberg mode", err=True)
            raise typer.Exit(2)
    else:
        if not output_dir:
            typer.echo(
                "Error: --output-dir is required (or set export.output_dir in config file)",
                err=True,
            )
            raise typer.Exit(2)

    setup_logging(log_level, log_format)
    logger = _sl.get_logger()

    if iceberg and partition_pattern:
        logger.warning(
            "iceberg_mode_ignores_partition_pattern",
            msg="--partition-pattern is ignored in Iceberg mode",
        )

    # ------------------------------------------------------------------
    # Validate mutually-dependent options
    # ------------------------------------------------------------------
    if (start_date or end_date) and not date_field:
        typer.echo(
            "Error: --date-field is required when --start-date or --end-date is set.",
            err=True,
        )
        raise typer.Exit(2)

    parsed_start: Optional[datetime] = None
    parsed_end: Optional[datetime] = None
    try:
        if start_date:
            parsed_start = parse_date(start_date)
        if end_date:
            parsed_end = parse_date(end_date)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2)

    # ------------------------------------------------------------------
    # Build MongoDB query filter
    # ------------------------------------------------------------------
    query: dict = {}
    if date_field and (parsed_start or parsed_end):
        date_filter: dict = {}
        if parsed_start:
            date_filter["$gte"] = parsed_start
        if parsed_end:
            date_filter["$lte"] = parsed_end
        query[date_field] = date_filter

    # ------------------------------------------------------------------
    # Initialise components
    # ------------------------------------------------------------------
    extractor = MongoExtractor(
        uri=uri,
        connection_timeout_ms=connection_timeout_ms,
        socket_timeout_ms=socket_timeout_ms,
        read_preference=read_preference,
        max_pool_size=max_pool_size,
    )
    transformer = DocumentTransformer(
        flatten_depth=flatten_depth,
        on_schema_drift=on_schema_drift,
    )
    if iceberg:
        from .iceberg_writer import IcebergWriter
        writer = IcebergWriter(
            catalog_uri,
            warehouse,
            namespace,
            s3_endpoint=s3_endpoint,
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_region=s3_region,
        )
    else:
        writer = ParquetWriter(
            output_dir=output_dir,
            compression=compression,
            compression_level=compression_level,
            overwrite=overwrite,
            partition_pattern=partition_pattern,
        )

    total_docs = 0
    total_files = 0
    skip_count = 0

    try:
        extractor.connect()
        dbs = extractor.list_databases(include=databases or None)

        if not dbs:
            logger.warning("no_databases_found")
            raise typer.Exit(0)

        for db_name in dbs:
            cols = extractor.list_collections(db_name, include=list(collections))

            for col_name in cols:
                if date_field:
                    extractor.check_date_index(db_name, col_name, date_field)

                logger.info("export_start", database=db_name, collection=col_name)

                # In Iceberg mode, resume from the last written date to
                # avoid re-processing documents already in the table.
                col_query = dict(query)
                if iceberg and date_field:
                    resume_date = writer.get_resume_date(db_name, col_name, date_field)
                    if resume_date:
                        existing_filter = col_query.get(date_field, {})
                        existing_filter["$gt"] = resume_date
                        col_query[date_field] = existing_filter
                        logger.info(
                            "resuming_export",
                            database=db_name,
                            collection=col_name,
                            resume_after=str(resume_date),
                        )

                batch: list = []
                last_date: Optional[datetime] = None

                for doc in extractor.stream(db_name, col_name, col_query, batch_size=batch_size):
                    # Capture partition date from raw doc before transform
                    if date_field and date_field in doc:
                        raw_val = doc[date_field]
                        if isinstance(raw_val, datetime):
                            last_date = (
                                raw_val.replace(tzinfo=timezone.utc)
                                if raw_val.tzinfo is None
                                else raw_val
                            )

                    try:
                        batch.append(transformer.transform(doc))
                    except Exception as exc:
                        skip_count += 1
                        logger.warning(
                            "doc_transform_failed",
                            error=str(exc),
                            skip_count=skip_count,
                        )
                        continue

                    if len(batch) >= batch_size:
                        d, f = _flush_batch(
                            batch, last_date, transformer, writer,
                            db_name, col_name, dry_run, logger,
                            date_field=date_field if iceberg else None,
                            use_iceberg=iceberg,
                        )
                        total_docs += d
                        total_files += f
                        last_date = None

                # Flush remaining documents
                if batch:
                    d, f = _flush_batch(
                        batch, last_date, transformer, writer,
                        db_name, col_name, dry_run, logger,
                        date_field=date_field if iceberg else None,
                        use_iceberg=iceberg,
                    )
                    total_docs += d
                    total_files += f

                logger.info(
                    "export_done",
                    database=db_name,
                    collection=col_name,
                    docs=total_docs,
                )

    except (typer.Exit, KeyboardInterrupt):
        raise
    except Exception as exc:
        logger.error("fatal_error", error=str(exc), exc_info=True)
        raise typer.Exit(2)
    finally:
        extractor.close()

    if skip_count:
        logger.warning(
            "export_complete_with_skips",
            docs=total_docs,
            files=total_files,
            skipped=skip_count,
        )
        raise typer.Exit(1)

    logger.info("export_complete", docs=total_docs, files=total_files)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _flush_batch(
    batch: list,
    last_date: Optional[datetime],
    transformer: DocumentTransformer,
    writer,
    db_name: str,
    col_name: str,
    dry_run: bool,
    logger,
    *,
    date_field: Optional[str] = None,
    use_iceberg: bool = False,
) -> tuple[int, int]:
    """Transform a batch of documents, write to Parquet or Iceberg, return (docs, files)."""
    table = transformer.to_arrow(batch)
    # Free the Python dicts immediately after the Arrow table is built so the
    # batch list and the columnar table do not coexist in memory during the write.
    batch.clear()

    if use_iceberg:
        if dry_run:
            logger.info("dry_run_batch", mode="iceberg", rows=len(table))
            return len(table), 0
        writer.write(table, db_name, col_name, date_field=date_field)
        return len(table), 1

    partition_path = writer.get_partition_path(db_name, col_name, last_date)

    if dry_run:
        logger.info(
            "dry_run_batch",
            partition=str(partition_path),
            rows=len(table),
        )
        return len(table), 0

    writer.write(table, partition_path)
    return len(table), 1


def main() -> None:
    app()
