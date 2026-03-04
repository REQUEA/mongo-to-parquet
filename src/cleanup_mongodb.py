#!/usr/bin/env python3
"""
MongoDB Cleanup Script
======================

Safely delete exported data from MongoDB after verification.

⚠️  WARNING: This script DELETES data from MongoDB. Use with extreme caution!

Usage:
    # Dry run (recommended first)
    python cleanup_mongodb.py -c config.yaml --dry-run

    # Actual deletion (requires --confirm flag)
    python cleanup_mongodb.py -c config.yaml --confirm
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Dict, List

import yaml
from pymongo import MongoClient
from tqdm import tqdm


class MongoDBCleaner:
    """Safely clean up exported data from MongoDB."""

    def __init__(self, config_path: str, dry_run: bool = True, confirm: bool = False):
        """Initialize cleaner."""
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.dry_run = dry_run
        self.confirm = confirm
        self.client = None
        self.stats = []

    def connect_mongodb(self):
        """Connect to MongoDB."""
        self.client = MongoClient(self.config["mongodb"]["uri"])
        self.client.admin.command("ping")

        if self.dry_run:
            print("✓ Connected to MongoDB (DRY RUN MODE - No data will be deleted)")
        else:
            print("✓ Connected to MongoDB")

    def cleanup_collection(self, database: str, collection: str) -> Dict:
        """Clean up a single collection."""
        result = {
            "database": database,
            "collection": collection,
            "documents_deleted": 0,
            "space_freed_mb": 0,
            "status": "success",
            "error": None,
        }

        try:
            db = self.client[database]
            coll = db[collection]

            # Get collection stats before deletion
            stats_before = db.command("collStats", collection)
            size_before = stats_before.get("size", 0)

            # Build same query used in export
            query = self._build_query_filter(collection)

            # Count documents to delete
            count_to_delete = coll.count_documents(query)

            if count_to_delete == 0:
                print(f"  No documents to delete in {database}.{collection}")
                return result

            print(f"\n{database}.{collection}:")
            print(f"  Documents to delete: {count_to_delete:,}")
            print(f"  Current size: {size_before / (1024**2):.2f} MB")
            print(f"  Query: {query}")

            if self.dry_run:
                print(f"  [DRY RUN] Would delete {count_to_delete:,} documents")
                result["documents_deleted"] = count_to_delete
                result["space_freed_mb"] = size_before / (1024**2)
            else:
                if not self.confirm:
                    print("  Skipping deletion (--confirm flag not provided)")
                    return result

                # Confirm deletion
                response = input(f"  Delete {count_to_delete:,} documents? (yes/no): ")
                if response.lower() != "yes":
                    print("  Skipped by user")
                    result["status"] = "skipped"
                    return result

                # Delete documents
                print("  Deleting documents...")
                delete_result = coll.delete_many(query)
                result["documents_deleted"] = delete_result.deleted_count

                # Get stats after deletion
                stats_after = db.command("collStats", collection)
                size_after = stats_after.get("size", 0)

                space_freed = (size_before - size_after) / (1024**2)
                result["space_freed_mb"] = space_freed

                print(f"  ✓ Deleted {delete_result.deleted_count:,} documents")
                print(f"  ✓ Freed {space_freed:.2f} MB")

                # Compact collection if significant space freed
                if space_freed > 100:  # More than 100 MB
                    print("  Compacting collection...")
                    try:
                        db.command("compact", collection)
                        print("  ✓ Collection compacted")
                    except Exception as e:
                        print(f"  ⚠ Compact failed: {e}")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            print(f"  ✗ Error: {e}")

        return result

    def _build_query_filter(self, collection_name: str) -> Dict:
        """Build same query filter used in export."""
        filter_config = self.config["filters"]
        query = {}

        if filter_config["date_field"] and (
            filter_config["start_date"] or filter_config["end_date"]
        ):
            date_field = filter_config["date_field"]
            date_filter = {}

            if filter_config["start_date"]:
                start_date = datetime.fromisoformat(filter_config["start_date"].replace("Z", "+00:00"))
                date_filter["$gte"] = start_date

            if filter_config["end_date"]:
                end_date = datetime.fromisoformat(filter_config["end_date"].replace("Z", "+00:00"))
                date_filter["$lt"] = end_date

            if date_filter:
                query[date_field] = date_filter

        return query

    def cleanup_all(self):
        """Clean up all exported collections."""
        self.connect_mongodb()

        if not self.dry_run and not self.confirm:
            print("\n⚠️  WARNING: You must provide --confirm flag to actually delete data")
            print("Run with --dry-run first to see what would be deleted\n")
            sys.exit(1)

        if not self.dry_run:
            print("\n" + "=" * 80)
            print("⚠️  WARNING: THIS WILL PERMANENTLY DELETE DATA FROM MONGODB")
            print("=" * 80)
            print("This operation cannot be undone!")
            print("Make sure you have:")
            print("  1. Verified Parquet exports with verify_export.py")
            print("  2. Backed up your MongoDB data")
            print("  3. Tested the deletion with --dry-run first")
            print()

            response = input("Type 'DELETE' to confirm: ")
            if response != "DELETE":
                print("Deletion cancelled")
                sys.exit(0)

        databases = self._get_databases_to_cleanup()

        print(f"\nProcessing {len(databases)} databases...")

        for database in databases:
            collections = self._get_collections_to_cleanup(database)

            for collection in collections:
                result = self.cleanup_collection(database, collection)
                self.stats.append(result)

        self._print_summary()

    def _get_databases_to_cleanup(self) -> List[str]:
        """Get list of databases to clean up."""
        all_dbs = self.client.list_database_names()
        exclude_dbs = set(self.config["export"]["exclude_databases"])

        if "*" in self.config["export"]["databases"]:
            return [db for db in all_dbs if db not in exclude_dbs]
        else:
            return [
                db for db in self.config["export"]["databases"]
                if db in all_dbs and db not in exclude_dbs
            ]

    def _get_collections_to_cleanup(self, database: str) -> List[str]:
        """Get list of collections to clean up."""
        db = self.client[database]
        all_collections = db.list_collection_names()

        db_config = self.config["export"]["collections"].get(
            database, self.config["export"]["collections"].get("*", {})
        )

        if not db_config:
            return []

        collections_filter = db_config.get("collections", ["*"])
        exclude_collections = set(db_config.get("exclude_collections", []))

        if "*" in collections_filter:
            return [col for col in all_collections if col not in exclude_collections]
        else:
            return [
                col for col in collections_filter
                if col in all_collections and col not in exclude_collections
            ]

    def _print_summary(self):
        """Print cleanup summary."""
        print("\n" + "=" * 80)
        if self.dry_run:
            print("CLEANUP SUMMARY (DRY RUN)")
        else:
            print("CLEANUP SUMMARY")
        print("=" * 80)

        total_deleted = sum(r["documents_deleted"] for r in self.stats)
        total_space_freed = sum(r["space_freed_mb"] for r in self.stats)

        success_count = sum(1 for r in self.stats if r["status"] == "success")
        error_count = sum(1 for r in self.stats if r["status"] == "error")

        print(f"Collections processed: {len(self.stats)}")
        print(f"Total documents {'would be ' if self.dry_run else ''}deleted: {total_deleted:,}")
        print(f"Total space {'would be ' if self.dry_run else ''}freed: {total_space_freed:.2f} MB")
        print(f"Successful: {success_count}")

        if error_count > 0:
            print(f"Errors: {error_count}")

        if not self.dry_run:
            print("\n✓ Cleanup completed successfully")
            print("\nNext steps:")
            print("  1. Run compact on all databases to fully reclaim space:")
            print("     db.runCommand({ compact: 'collection_name' })")
            print("  2. Consider running repairDatabase if needed")

        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Clean up exported MongoDB data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This script permanently deletes data from MongoDB!

Recommended workflow:
  1. python cleanup_mongodb.py --dry-run     # See what would be deleted
  2. python verify_export.py                  # Verify Parquet exports
  3. mongodump --out backup/                  # Backup MongoDB
  4. python cleanup_mongodb.py --confirm      # Actually delete data
        """,
    )
    parser.add_argument(
        "-c", "--config", default="config.yaml", help="Path to configuration file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate deletion without actually deleting data",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required flag to actually delete data (not needed for dry-run)",
    )

    args = parser.parse_args()

    # Force dry-run if confirm not provided
    dry_run = args.dry_run or not args.confirm

    cleaner = MongoDBCleaner(args.config, dry_run=dry_run, confirm=args.confirm)
    cleaner.cleanup_all()


if __name__ == "__main__":
    main()
