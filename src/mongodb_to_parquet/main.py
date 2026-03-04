#!/usr/bin/env python3
"""
MongoDB to Parquet Exporter
===========================

Production-ready script for exporting MongoDB data to Parquet format with
intelligent resource management, error handling, and incremental export support.

Author: IoT Data Engineering Team
License: MIT
"""

import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from pythonjsonlogger import jsonlogger
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm


class MongoToParquetExporter:
    """Main exporter class for MongoDB to Parquet conversion."""

    def __init__(self, config_path: str):
        """Initialize the exporter with configuration."""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.client: Optional[MongoClient] = None
        self.checkpoint = self._load_checkpoint()
        self.stats = defaultdict(lambda: defaultdict(int))
        self.start_time = time.time()

        # Resource limits
        self.system_memory = psutil.virtual_memory().total
        self.max_memory_bytes = int(
            self.system_memory * self.config["performance"]["max_memory_percent"]
        )
        self.batch_size = self._calculate_batch_size()

        self.logger.info(
            "Exporter initialized",
            extra={
                "system_memory_gb": round(self.system_memory / (1024**3), 2),
                "max_memory_gb": round(self.max_memory_bytes / (1024**3), 2),
                "batch_size": self.batch_size,
                "config_path": config_path,
            },
        )

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)

            # Set defaults for auto values
            if config["performance"]["batch_size"] == "auto":
                config["performance"]["batch_size"] = None

            if config["performance"]["max_workers"] == "auto":
                config["performance"]["max_workers"] = psutil.cpu_count(logical=False) or 2

            return config
        except Exception as e:
            print(f"Error loading config: {e}", file=sys.stderr)
            sys.exit(1)

    def _setup_logging(self) -> logging.Logger:
        """Setup structured JSON logging."""
        logger = logging.getLogger("mongo_to_parquet")
        logger.setLevel(self.config["logging"]["level"])
        logger.handlers = []  # Clear existing handlers

        # JSON formatter
        if self.config["logging"]["format"] == "json":
            formatter = jsonlogger.JsonFormatter(
                "%(asctime)s %(name)s %(levelname)s %(message)s",
                rename_fields={"asctime": "timestamp", "levelname": "level"},
            )
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        # File handler
        if self.config["logging"]["file"]:
            os.makedirs(os.path.dirname(self.config["logging"]["file"]) or ".", exist_ok=True)
            file_handler = logging.FileHandler(self.config["logging"]["file"])
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Console handler
        if self.config["logging"]["console"]:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def _calculate_batch_size(self) -> int:
        """Calculate optimal batch size based on available memory."""
        if self.config["performance"]["batch_size"] is not None:
            return self.config["performance"]["batch_size"]

        # Estimate: assume average document size of 1KB for IoT data
        avg_doc_size = 1024
        # Use 10% of max memory for batch
        batch_memory = self.max_memory_bytes * 0.1
        batch_size = int(batch_memory / avg_doc_size)

        # Clamp between 1000 and 100000
        return max(1000, min(100000, batch_size))

    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load checkpoint file for resuming exports."""
        if not self.config["incremental"]["enabled"]:
            return {}

        checkpoint_file = self.config["incremental"]["checkpoint_file"]
        if os.path.exists(checkpoint_file) and self.config["incremental"]["resume_on_restart"]:
            try:
                with open(checkpoint_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load checkpoint: {e}", file=sys.stderr)

        return {}

    def _save_checkpoint(self):
        """Save current export progress to checkpoint file."""
        if not self.config["incremental"]["enabled"]:
            return

        checkpoint_file = self.config["incremental"]["checkpoint_file"]
        try:
            os.makedirs(os.path.dirname(checkpoint_file) or ".", exist_ok=True)
            with open(checkpoint_file, "w") as f:
                json.dump(self.checkpoint, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def _connect_mongodb(self):
        """Establish connection to MongoDB replica set."""
        try:
            mongo_config = self.config["mongodb"]

            # Map read preference string to PyMongo constant
            read_pref_map = {
                "primary": ReadPreference.PRIMARY,
                "primaryPreferred": ReadPreference.PRIMARY_PREFERRED,
                "secondary": ReadPreference.SECONDARY,
                "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
                "nearest": ReadPreference.NEAREST,
            }

            self.client = MongoClient(
                mongo_config["uri"],
                serverSelectionTimeoutMS=mongo_config["connection_timeout_ms"],
                socketTimeoutMS=mongo_config["socket_timeout_ms"],
                maxPoolSize=mongo_config["max_pool_size"],
                readPreference=read_pref_map.get(
                    mongo_config["read_preference"], ReadPreference.SECONDARY_PREFERRED
                ),
            )

            # Test connection
            self.client.admin.command("ping")

            # Get replica set status
            replica_status = self.client.admin.command("replSetGetStatus")

            self.logger.info(
                "MongoDB connection established",
                extra={
                    "replica_set": replica_status.get("set"),
                    "members": len(replica_status.get("members", [])),
                    "read_preference": mongo_config["read_preference"],
                },
            )

        except ServerSelectionTimeoutError as e:
            self.logger.error(f"MongoDB connection timeout: {e}")
            raise
        except PyMongoError as e:
            self.logger.error(f"MongoDB connection error: {e}")
            raise

    def _get_databases_to_export(self) -> List[str]:
        """Get list of databases to export based on configuration."""
        all_dbs = self.client.list_database_names()
        exclude_dbs = set(self.config["export"]["exclude_databases"])

        if "*" in self.config["export"]["databases"]:
            # Export all except excluded and system databases
            databases = [db for db in all_dbs if db not in exclude_dbs]
        else:
            databases = [
                db for db in self.config["export"]["databases"]
                if db in all_dbs and db not in exclude_dbs
            ]

        self.logger.info(
            "Databases selected for export",
            extra={"count": len(databases), "databases": databases},
        )

        return databases

    def _get_collections_to_export(self, database: str) -> List[str]:
        """Get list of collections to export for a given database."""
        db = self.client[database]
        all_collections = db.list_collection_names()

        # Get collection config for this database or default "*"
        db_config = self.config["export"]["collections"].get(
            database, self.config["export"]["collections"].get("*", {})
        )

        if not db_config:
            return []

        collections_filter = db_config.get("collections", ["*"])
        exclude_collections = set(db_config.get("exclude_collections", []))

        if "*" in collections_filter:
            collections = [col for col in all_collections if col not in exclude_collections]
        else:
            collections = [
                col for col in collections_filter
                if col in all_collections and col not in exclude_collections
            ]

        return collections

    def _build_query_filter(self, collection_name: str) -> Dict[str, Any]:
        """Build MongoDB query filter based on configuration."""
        filter_config = self.config["filters"]
        query = {}

        # Date filtering
        if filter_config["date_field"] and (
            filter_config["start_date"] or filter_config["end_date"]
        ):
            date_field = filter_config["date_field"]
            date_filter = {}

            if filter_config["start_date"]:
                start_date = self._parse_date(filter_config["start_date"])
                date_filter["$gte"] = start_date

            if filter_config["end_date"]:
                end_date = self._parse_date(filter_config["end_date"])
                date_filter["$lt"] = end_date

            if date_filter:
                query[date_field] = date_filter

        # Custom filters
        custom_filters = filter_config.get("custom_filters", {})
        collection_filter = custom_filters.get(collection_name) or custom_filters.get("*", {})

        if collection_filter:
            query.update(collection_filter)

        return query

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object."""
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Ensure timezone aware
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        raise ValueError(f"Could not parse date: {date_str}")

    def _get_output_path(
        self, database: str, collection: str, document: Dict[str, Any]
    ) -> Path:
        """Generate output path for Parquet file based on date partitioning."""
        base_path = Path(self.config["export"]["output_dir"])

        # Extract date from document
        date_field = self.config["filters"]["date_field"]
        if date_field and date_field in document:
            doc_date = document[date_field]
            if isinstance(doc_date, datetime):
                year = doc_date.year
                month = f"{doc_date.month:02d}"
                day = f"{doc_date.day:02d}"
            else:
                # Fallback to current date if not datetime
                now = datetime.now()
                year = now.year
                month = f"{now.month:02d}"
                day = f"{now.day:02d}"
        else:
            # No date field, use current date
            now = datetime.now()
            year = now.year
            month = f"{now.month:02d}"
            day = f"{now.day:02d}"

        # Build path: db/collection/year=YYYY/month=MM/day=DD/
        partition_path = (
            base_path / database / collection / f"year={year}" / f"month={month}" / f"day={day}"
        )

        return partition_path

    def _should_skip_partition(self, partition_path: Path) -> bool:
        """Check if partition should be skipped based on incremental settings."""
        if not self.config["incremental"]["enabled"]:
            return False

        if self.config["incremental"]["overwrite_existing"]:
            return False

        # Check if partition already has Parquet files
        if partition_path.exists():
            parquet_files = list(partition_path.glob("*.parquet"))
            if parquet_files:
                self.logger.debug(
                    "Skipping existing partition",
                    extra={"partition": str(partition_path), "files": len(parquet_files)},
                )
                return True

        return False

    def _export_collection(self, database: str, collection: str) -> Dict[str, int]:
        """Export a single collection to Parquet files."""
        collection_stats = {
            "documents_processed": 0,
            "documents_exported": 0,
            "bytes_written": 0,
            "files_created": 0,
            "partitions_skipped": 0,
            "errors": 0,
        }

        try:
            db = self.client[database]
            coll = db[collection]

            # Build query
            query = self._build_query_filter(collection)

            # Get total count
            if not self.config["dry_run"]["enabled"]:
                total_docs = coll.count_documents(query)
            else:
                total_docs = min(
                    coll.count_documents(query),
                    self.config["dry_run"]["max_documents"],
                )

            if total_docs == 0:
                self.logger.info(
                    "No documents to export",
                    extra={"database": database, "collection": collection},
                )
                return collection_stats

            self.logger.info(
                "Starting collection export",
                extra={
                    "database": database,
                    "collection": collection,
                    "total_documents": total_docs,
                    "query": query,
                },
            )

            # Create cursor with batch processing
            cursor = coll.find(query, no_cursor_timeout=self.config["performance"]["no_cursor_timeout"])
            cursor.batch_size(self.batch_size)

            # Group documents by partition
            partition_buffers = defaultdict(list)
            processed_docs = 0

            with tqdm(
                total=total_docs,
                desc=f"{database}.{collection}",
                unit="docs",
                disable=not self.config["logging"]["console"],
            ) as pbar:

                for doc in cursor:
                    # Check memory usage
                    if psutil.virtual_memory().percent > 90:
                        self.logger.warning(
                            "High memory usage detected, flushing buffers",
                            extra={"memory_percent": psutil.virtual_memory().percent},
                        )
                        self._flush_partition_buffers(
                            database, collection, partition_buffers, collection_stats
                        )
                        partition_buffers.clear()

                    # Get partition path
                    partition_path = self._get_output_path(database, collection, doc)

                    # Skip if partition already exists and not overwriting
                    if self._should_skip_partition(partition_path):
                        collection_stats["partitions_skipped"] += 1
                        continue

                    # Add to buffer
                    partition_buffers[partition_path].append(doc)
                    processed_docs += 1
                    pbar.update(1)

                    # Flush buffer if it exceeds target size
                    if len(partition_buffers[partition_path]) >= self.batch_size:
                        self._write_partition(
                            partition_path,
                            partition_buffers[partition_path],
                            collection_stats,
                        )
                        partition_buffers[partition_path].clear()

                    # Dry run limit
                    if (
                        self.config["dry_run"]["enabled"]
                        and processed_docs >= self.config["dry_run"]["max_documents"]
                    ):
                        break

            # Flush remaining buffers
            self._flush_partition_buffers(database, collection, partition_buffers, collection_stats)

            collection_stats["documents_processed"] = processed_docs

            self.logger.info(
                "Collection export completed",
                extra={
                    "database": database,
                    "collection": collection,
                    "stats": collection_stats,
                },
            )

        except Exception as e:
            self.logger.error(
                "Collection export failed",
                extra={
                    "database": database,
                    "collection": collection,
                    "error": str(e),
                },
                exc_info=True,
            )
            collection_stats["errors"] += 1

            if self.config["error_handling"]["fail_fast"]:
                raise

        return collection_stats

    def _flush_partition_buffers(
        self,
        database: str,
        collection: str,
        partition_buffers: Dict[Path, List[Dict]],
        collection_stats: Dict[str, int],
    ):
        """Flush all partition buffers to Parquet files."""
        for partition_path, documents in partition_buffers.items():
            if documents:
                self._write_partition(partition_path, documents, collection_stats)

    def _write_partition(
        self, partition_path: Path, documents: List[Dict[str, Any]], stats: Dict[str, int]
    ):
        """Write documents to Parquet file."""
        if self.config["dry_run"]["enabled"]:
            stats["documents_exported"] += len(documents)
            self.logger.debug(
                "Dry run: would write partition",
                extra={"partition": str(partition_path), "documents": len(documents)},
            )
            return

        try:
            # Create directory
            partition_path.mkdir(parents=True, exist_ok=True)

            # Convert documents to PyArrow table
            # Remove _id field as it's not serializable to Parquet
            clean_docs = []
            for doc in documents:
                clean_doc = {k: v for k, v in doc.items() if k != "_id"}
                # Convert datetime to timestamp
                for key, value in clean_doc.items():
                    if isinstance(value, datetime):
                        clean_doc[key] = value
                clean_docs.append(clean_doc)

            # Create PyArrow table
            table = pa.Table.from_pylist(clean_docs)

            # Generate unique filename
            timestamp = int(time.time() * 1000)
            filename = partition_path / f"data_{timestamp}.parquet"

            # Write Parquet file
            pq.write_table(
                table,
                filename,
                compression=self.config["performance"]["compression"],
                compression_level=self.config["performance"]["compression_level"],
                use_dictionary=True,
                write_statistics=True,
            )

            file_size = os.path.getsize(filename)

            stats["documents_exported"] += len(documents)
            stats["bytes_written"] += file_size
            stats["files_created"] += 1

            self.logger.debug(
                "Partition written",
                extra={
                    "partition": str(partition_path),
                    "filename": filename.name,
                    "documents": len(documents),
                    "size_mb": round(file_size / (1024**2), 2),
                },
            )

            # Validate if enabled
            if self.config["validation"]["verify_parquet_files"]:
                self._validate_parquet_file(filename, len(documents))

        except Exception as e:
            self.logger.error(
                "Failed to write partition",
                extra={
                    "partition": str(partition_path),
                    "error": str(e),
                    "documents": len(documents),
                },
                exc_info=True,
            )
            stats["errors"] += 1

            if self.config["error_handling"]["log_failed_documents"]:
                self._log_failed_documents(documents, str(e))

            if not self.config["error_handling"]["continue_on_error"]:
                raise

    def _validate_parquet_file(self, filepath: Path, expected_count: int):
        """Validate Parquet file integrity."""
        try:
            table = pq.read_table(filepath)
            actual_count = len(table)

            if self.config["validation"]["validate_document_count"]:
                if actual_count != expected_count:
                    self.logger.error(
                        "Document count mismatch",
                        extra={
                            "file": str(filepath),
                            "expected": expected_count,
                            "actual": actual_count,
                        },
                    )
                    if self.config["validation"]["fail_on_validation_error"]:
                        raise ValueError(f"Document count mismatch in {filepath}")

        except Exception as e:
            self.logger.error(
                "Parquet validation failed",
                extra={"file": str(filepath), "error": str(e)},
            )
            if self.config["validation"]["fail_on_validation_error"]:
                raise

    def _log_failed_documents(self, documents: List[Dict], error: str):
        """Log failed documents to error file."""
        error_file = self.config["error_handling"]["error_log_file"]
        try:
            os.makedirs(os.path.dirname(error_file) or ".", exist_ok=True)
            with open(error_file, "a") as f:
                for doc in documents:
                    error_record = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "error": error,
                        "document": doc,
                    }
                    f.write(json.dumps(error_record, default=str) + "\n")
        except Exception as e:
            self.logger.error(f"Failed to log failed documents: {e}")

    def _log_metrics(self):
        """Log current progress metrics."""
        if not self.config["logging"]["enable_metrics"]:
            return

        elapsed_time = time.time() - self.start_time

        total_docs_processed = sum(
            db_stats["documents_processed"]
            for db_stats in self.stats.values()
            for collection_stats in db_stats.values()
        )

        total_docs_exported = sum(
            collection_stats["documents_exported"]
            for db_stats in self.stats.values()
            for collection_stats in db_stats.values()
        )

        total_bytes_written = sum(
            collection_stats["bytes_written"]
            for db_stats in self.stats.values()
            for collection_stats in db_stats.values()
        )

        docs_per_second = total_docs_processed / elapsed_time if elapsed_time > 0 else 0
        mb_per_second = (total_bytes_written / (1024**2)) / elapsed_time if elapsed_time > 0 else 0

        self.logger.info(
            "Export metrics",
            extra={
                "elapsed_seconds": round(elapsed_time, 2),
                "documents_processed": total_docs_processed,
                "documents_exported": total_docs_exported,
                "bytes_written": total_bytes_written,
                "mb_written": round(total_bytes_written / (1024**2), 2),
                "docs_per_second": round(docs_per_second, 2),
                "mb_per_second": round(mb_per_second, 2),
                "memory_percent": psutil.virtual_memory().percent,
            },
        )

    def export(self):
        """Main export orchestration method."""
        try:
            # Connect to MongoDB
            self._connect_mongodb()

            # Get databases to export
            databases = self._get_databases_to_export()

            if not databases:
                self.logger.warning("No databases to export")
                return

            # Metrics logging timer
            last_metrics_time = time.time()
            metrics_interval = self.config["logging"]["metrics_interval_seconds"]

            # Export each database
            for database in databases:
                collections = self._get_collections_to_export(database)

                if not collections:
                    self.logger.info(
                        "No collections to export",
                        extra={"database": database},
                    )
                    continue

                self.logger.info(
                    "Starting database export",
                    extra={"database": database, "collections": len(collections)},
                )

                for collection in collections:
                    # Log metrics periodically
                    if time.time() - last_metrics_time >= metrics_interval:
                        self._log_metrics()
                        last_metrics_time = time.time()

                    # Export collection
                    collection_stats = self._export_collection(database, collection)
                    self.stats[database][collection] = collection_stats

                    # Update checkpoint
                    if self.config["incremental"]["enabled"]:
                        if database not in self.checkpoint:
                            self.checkpoint[database] = {}
                        self.checkpoint[database][collection] = {
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "stats": collection_stats,
                        }
                        self._save_checkpoint()

            # Final metrics
            self._log_metrics()

            # Print summary
            self._print_summary()

        except KeyboardInterrupt:
            self.logger.warning("Export interrupted by user")
            self._save_checkpoint()
            sys.exit(1)

        except Exception as e:
            self.logger.error(f"Export failed: {e}", exc_info=True)
            self._save_checkpoint()
            sys.exit(1)

        finally:
            if self.client:
                self.client.close()
                self.logger.info("MongoDB connection closed")

    def _print_summary(self):
        """Print export summary statistics."""
        total_stats = {
            "databases": len(self.stats),
            "collections": sum(len(db_stats) for db_stats in self.stats.values()),
            "documents_processed": sum(
                collection_stats["documents_processed"]
                for db_stats in self.stats.values()
                for collection_stats in db_stats.values()
            ),
            "documents_exported": sum(
                collection_stats["documents_exported"]
                for db_stats in self.stats.values()
                for collection_stats in db_stats.values()
            ),
            "bytes_written": sum(
                collection_stats["bytes_written"]
                for db_stats in self.stats.values()
                for collection_stats in db_stats.values()
            ),
            "files_created": sum(
                collection_stats["files_created"]
                for db_stats in self.stats.values()
                for collection_stats in db_stats.values()
            ),
            "errors": sum(
                collection_stats["errors"]
                for db_stats in self.stats.values()
                for collection_stats in db_stats.values()
            ),
        }

        elapsed_time = time.time() - self.start_time

        # Calculate compression ratio if data was written
        compression_ratio = 0
        if total_stats["bytes_written"] > 0:
            # Estimate original size (assuming ~1KB per document)
            estimated_original_size = total_stats["documents_exported"] * 1024
            compression_ratio = estimated_original_size / total_stats["bytes_written"]

        summary = {
            "status": "completed" if total_stats["errors"] == 0 else "completed_with_errors",
            "duration_seconds": round(elapsed_time, 2),
            "duration_human": self._format_duration(elapsed_time),
            "statistics": {
                **total_stats,
                "mb_written": round(total_stats["bytes_written"] / (1024**2), 2),
                "gb_written": round(total_stats["bytes_written"] / (1024**3), 3),
                "compression_ratio": round(compression_ratio, 2),
            },
            "output_directory": self.config["export"]["output_dir"],
            "dry_run": self.config["dry_run"]["enabled"],
        }

        self.logger.info("Export summary", extra=summary)

        # Console summary
        if self.config["logging"]["console"]:
            print("\n" + "=" * 80)
            print("EXPORT SUMMARY")
            print("=" * 80)
            print(f"Status: {summary['status']}")
            print(f"Duration: {summary['duration_human']}")
            print(f"Databases: {total_stats['databases']}")
            print(f"Collections: {total_stats['collections']}")
            print(f"Documents Processed: {total_stats['documents_processed']:,}")
            print(f"Documents Exported: {total_stats['documents_exported']:,}")
            print(f"Data Written: {summary['statistics']['gb_written']:.3f} GB")
            print(f"Files Created: {total_stats['files_created']:,}")
            print(f"Compression Ratio: {summary['statistics']['compression_ratio']:.2f}x")
            if total_stats["errors"] > 0:
                print(f"Errors: {total_stats['errors']}")
            print("=" * 80)

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)

        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")

        return " ".join(parts)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="MongoDB to Parquet Exporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (override config)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    args = parser.parse_args()

    # Check if config file exists
    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    # Initialize exporter
    exporter = MongoToParquetExporter(args.config)

    # Override dry-run if specified
    if args.dry_run:
        exporter.config["dry_run"]["enabled"] = True
        exporter.logger.info("Dry-run mode enabled via command line")

    # Run export
    exporter.export()


if __name__ == "__main__":
    main()
