"""CLI command for MariaDB → Iceberg/Parquet export."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import typer
import yaml

from .mariadb_extractor import MariaDBExtractor
from .utils import setup_logging
from .writer import ParquetWriter


def _load_yaml_config(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def export_mariadb(
    config: Optional[Path] = typer.Option(
        None, "--config", "-C", help="Path to YAML config file"
    ),
    host: Optional[str] = typer.Option(
        None, "--host", envvar="MTP_MARIADB_HOST", help="MariaDB host"
    ),
    port: Optional[int] = typer.Option(
        None, "--port", envvar="MTP_MARIADB_PORT", help="MariaDB port (default: 3306)"
    ),
    user: Optional[str] = typer.Option(
        None, "--user", envvar="MTP_MARIADB_USER", help="MariaDB user"
    ),
    password: Optional[str] = typer.Option(
        None, "--password", envvar="MTP_MARIADB_PASSWORD", help="MariaDB password"
    ),
    databases: Optional[List[str]] = typer.Option(
        None, "--databases", "-d", help="Databases to include (default: all non-system)"
    ),
    tables: Optional[List[str]] = typer.Option(
        None, "--tables", "-t", help="Tables to export (repeat for multiple)"
    ),
    date_field: Optional[str] = typer.Option(
        None, help="Column used for date partitioning/filtering"
    ),
    start_date: Optional[str] = typer.Option(None, help="ISO start date, inclusive"),
    end_date: Optional[str] = typer.Option(None, help="ISO end date, inclusive"),
    output_dir: Optional[Path] = typer.Option(
        None, envvar="OUTPUT_DIR", help="Root output directory"
    ),
    partition_pattern: Optional[str] = typer.Option(None, help="Custom partition path template"),
    batch_size: Optional[int] = typer.Option(
        None, envvar="MTP_BATCH_SIZE", help="Row batch size (default: 5000)"
    ),
    compression: Optional[str] = typer.Option(None, help="Parquet compression (default: zstd)"),
    compression_level: Optional[int] = typer.Option(None, help="Zstd compression level"),
    overwrite: Optional[bool] = typer.Option(None, help="Overwrite existing files"),
    dry_run: Optional[bool] = typer.Option(None, help="Simulate without writing"),
    iceberg: bool = typer.Option(False, "--iceberg", help="Write to Iceberg table"),
    no_resume: bool = typer.Option(False, "--no-resume", help="Skip resume check"),
    catalog_uri: Optional[str] = typer.Option(
        None, "--catalog-uri", envvar="MTP_CATALOG_URI"
    ),
    warehouse: Optional[str] = typer.Option(
        None, "--warehouse", envvar="MTP_WAREHOUSE"
    ),
    namespace: str = typer.Option("default", "--namespace", envvar="MTP_NAMESPACE"),
    s3_endpoint: Optional[str] = typer.Option(
        None, "--s3-endpoint", envvar="MTP_S3_ENDPOINT"
    ),
    s3_access_key_id: Optional[str] = typer.Option(
        None, "--s3-access-key-id", envvar="AWS_ACCESS_KEY_ID"
    ),
    s3_secret_access_key: Optional[str] = typer.Option(
        None, "--s3-secret-access-key", envvar="AWS_SECRET_ACCESS_KEY"
    ),
    s3_region: Optional[str] = typer.Option(
        None, "--s3-region", envvar="AWS_DEFAULT_REGION"
    ),
    table_workers: Optional[int] = typer.Option(
        None, "--table-workers", envvar="MTP_TABLE_WORKERS",
        help="Number of tables to export in parallel (default: 1)",
    ),
    log_level: Optional[str] = typer.Option(None, envvar="LOG_LEVEL"),
    log_format: Optional[str] = typer.Option(None, envvar="LOG_FORMAT"),
) -> None:
    """Export MariaDB tables to Parquet files or Iceberg tables."""
    import structlog as _sl

    # ------------------------------------------------------------------
    # Load YAML config and merge
    # ------------------------------------------------------------------
    cfg = _load_yaml_config(config) if config else {}
    mariadb_cfg = cfg.get("mariadb", {})
    export_cfg = cfg.get("export", {})
    filters_cfg = cfg.get("filters", {})
    logging_cfg = cfg.get("logging", {})
    iceberg_cfg = cfg.get("iceberg", {})

    host = host or mariadb_cfg.get("host")
    port = port if port is not None else mariadb_cfg.get("port", 3306)
    user = user or mariadb_cfg.get("user")
    password = password if password is not None else mariadb_cfg.get("password", "")
    databases = databases or export_cfg.get("databases")
    tables = tables or export_cfg.get("tables") or []

    output_dir = output_dir or (
        Path(export_cfg["output_dir"]) if "output_dir" in export_cfg else None
    )

    iceberg = iceberg or iceberg_cfg.get("enabled", False)
    catalog_uri = catalog_uri or iceberg_cfg.get("catalog_uri")
    warehouse = warehouse or iceberg_cfg.get("warehouse")
    namespace = namespace if namespace != "default" else iceberg_cfg.get("namespace", namespace)
    s3_endpoint = s3_endpoint or iceberg_cfg.get("s3_endpoint")
    s3_access_key_id = s3_access_key_id or iceberg_cfg.get("s3_access_key_id")
    s3_secret_access_key = s3_secret_access_key or iceberg_cfg.get("s3_secret_access_key")
    s3_region = s3_region or iceberg_cfg.get("s3_region")
    no_resume = no_resume or iceberg_cfg.get("no_resume", False)

    date_field = date_field or filters_cfg.get("date_field")
    start_date = start_date or filters_cfg.get("start_date")
    end_date = end_date or filters_cfg.get("end_date")
    partition_pattern = partition_pattern or export_cfg.get("partition_pattern")

    table_workers = table_workers if table_workers is not None else export_cfg.get("table_workers", 1)
    batch_size = batch_size if batch_size is not None else export_cfg.get("batch_size", 5000)
    compression = compression if compression is not None else export_cfg.get("compression", "zstd")
    compression_level = compression_level if compression_level is not None else export_cfg.get("compression_level", 3)
    overwrite = overwrite if overwrite is not None else export_cfg.get("overwrite", False)
    dry_run = dry_run if dry_run is not None else export_cfg.get("dry_run", False)
    log_level = log_level if log_level is not None else logging_cfg.get("level", "INFO")
    log_format = log_format if log_format is not None else logging_cfg.get("format", "text")

    connection_timeout: int = mariadb_cfg.get("connection_timeout", 30)
    read_timeout: int = mariadb_cfg.get("read_timeout", 300)

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------
    if not host:
        typer.echo("Error: --host is required (or set mariadb.host in config)", err=True)
        raise typer.Exit(2)
    if not user:
        typer.echo("Error: --user is required (or set mariadb.user in config)", err=True)
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
            typer.echo("Error: --output-dir is required", err=True)
            raise typer.Exit(2)

    if (start_date or end_date) and not date_field:
        typer.echo("Error: --date-field is required when using date filters", err=True)
        raise typer.Exit(2)

    setup_logging(log_level, log_format)
    logger = _sl.get_logger()

    # ------------------------------------------------------------------
    # Initialise components
    # ------------------------------------------------------------------
    extractor = MariaDBExtractor(
        host=host,
        user=user,
        password=password,
        port=port,
        connection_timeout=connection_timeout,
        read_timeout=read_timeout,
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

        # Build list of (db, table) pairs
        tasks: list[tuple[str, str]] = []
        for db_name in dbs:
            tbls = extractor.list_tables(db_name, include=list(tables) if tables else None)
            for tbl_name in tbls:
                tasks.append((db_name, tbl_name))

        def _export_table(db_tbl: tuple[str, str]) -> tuple[int, int, int]:
            db_n, tbl_n = db_tbl
            return _process_table(
                extractor=extractor,
                writer=writer,
                db_name=db_n,
                table_name=tbl_n,
                date_field=date_field,
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                dry_run=dry_run,
                iceberg=iceberg,
                no_resume=no_resume,
                logger=logger,
            )

        if table_workers > 1 and len(tasks) > 1:
            logger.info("parallel_tables", workers=table_workers, tables=len(tasks))
            with ThreadPoolExecutor(max_workers=table_workers) as pool:
                for d, f, s in pool.map(_export_table, tasks):
                    total_docs += d
                    total_files += f
                    skip_count += s
        else:
            for task in tasks:
                d, f, s = _export_table(task)
                total_docs += d
                total_files += f
                skip_count += s

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
            rows=total_docs,
            files=total_files,
            skipped=skip_count,
        )
        raise typer.Exit(1)

    logger.info("export_complete", rows=total_docs, files=total_files)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _process_table(
    extractor: MariaDBExtractor,
    writer,
    db_name: str,
    table_name: str,
    date_field: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    batch_size: int,
    dry_run: bool,
    iceberg: bool,
    no_resume: bool,
    logger,
) -> tuple[int, int, int]:
    """Export a single table. Returns (rows, files, skipped)."""
    if date_field:
        extractor.check_date_index(db_name, table_name, date_field)

    logger.info("export_start", database=db_name, table=table_name)

    # Resume support for Iceberg mode
    effective_start = start_date
    if iceberg and date_field and not no_resume:
        resume_date = writer.get_resume_date(db_name, table_name, date_field)
        if resume_date:
            # Use resume date as start (exclusive via $gt equivalent)
            # Convert to ISO string for SQL comparison
            resume_str = resume_date.isoformat() if isinstance(resume_date, datetime) else str(resume_date)
            if not effective_start or resume_str > effective_start:
                effective_start = resume_str
                logger.info(
                    "resuming_export",
                    database=db_name,
                    table=table_name,
                    resume_after=resume_str,
                )

    tbl_docs = 0
    tbl_files = 0
    tbl_skipped = 0

    for arrow_table in extractor.stream(
        db_name, table_name,
        date_field=date_field,
        start_date=effective_start,
        end_date=end_date,
        batch_size=batch_size,
    ):
        rows = len(arrow_table)

        if iceberg:
            if dry_run:
                logger.info("dry_run_batch", mode="iceberg", rows=rows)
                tbl_docs += rows
                continue
            try:
                writer.write(arrow_table, db_name, table_name, date_field=date_field)
                tbl_docs += rows
                tbl_files += 1
            except Exception as exc:
                tbl_skipped += rows
                logger.warning("batch_write_failed", error=str(exc), rows=rows)
        else:
            # For Parquet mode, extract date from last row for partitioning
            last_date = None
            if date_field and date_field in arrow_table.column_names:
                col = arrow_table.column(date_field)
                last_val = col[-1].as_py()
                if isinstance(last_val, datetime):
                    last_date = (
                        last_val.replace(tzinfo=timezone.utc)
                        if last_val.tzinfo is None
                        else last_val
                    )

            partition_path = writer.get_partition_path(db_name, table_name, last_date)

            if dry_run:
                logger.info("dry_run_batch", partition=str(partition_path), rows=rows)
                tbl_docs += rows
                continue

            try:
                writer.write(arrow_table, partition_path)
                tbl_docs += rows
                tbl_files += 1
            except Exception as exc:
                tbl_skipped += rows
                logger.warning("batch_write_failed", error=str(exc), rows=rows)

    logger.info(
        "export_done",
        database=db_name,
        table=table_name,
        rows=tbl_docs,
        files=tbl_files,
    )
    return tbl_docs, tbl_files, tbl_skipped
