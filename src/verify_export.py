#!/usr/bin/env python3
"""
Verification Script for MongoDB to Parquet Export
================================================

This script verifies the exported Parquet files against MongoDB collections
to ensure data integrity and completeness.

Usage:
    python verify_export.py -c config.yaml
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

import pyarrow.parquet as pq
import yaml
from pymongo import MongoClient
from tqdm import tqdm


class ExportVerifier:
    """Verifies exported Parquet files against MongoDB."""

    def __init__(self, config_path: str):
        """Initialize verifier with configuration."""
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.client = None
        self.verification_results = []

    def connect_mongodb(self):
        """Connect to MongoDB."""
        self.client = MongoClient(self.config["mongodb"]["uri"])
        self.client.admin.command("ping")
        print("✓ Connected to MongoDB")

    def verify_collection(self, database: str, collection: str) -> Dict:
        """Verify a single collection."""
        print(f"\nVerifying {database}.{collection}...")

        result = {
            "database": database,
            "collection": collection,
            "status": "success",
            "mongodb_count": 0,
            "parquet_count": 0,
            "mismatch": False,
            "errors": [],
        }

        try:
            # Get MongoDB count
            db = self.client[database]
            coll = db[collection]

            # Build same query used in export
            query = self._build_query_filter(collection)
            mongo_count = coll.count_documents(query)
            result["mongodb_count"] = mongo_count

            # Count Parquet files
            parquet_base = Path(self.config["export"]["output_dir"]) / database / collection

            if not parquet_base.exists():
                result["status"] = "missing"
                result["errors"].append("Parquet directory not found")
                return result

            parquet_files = list(parquet_base.rglob("*.parquet"))

            if not parquet_files:
                result["status"] = "missing"
                result["errors"].append("No Parquet files found")
                return result

            # Count documents in Parquet files
            parquet_count = 0
            for parquet_file in tqdm(parquet_files, desc="Reading Parquet files", leave=False):
                try:
                    table = pq.read_table(parquet_file)
                    parquet_count += len(table)
                except Exception as e:
                    result["errors"].append(f"Error reading {parquet_file.name}: {e}")

            result["parquet_count"] = parquet_count

            # Compare counts
            if mongo_count != parquet_count:
                result["mismatch"] = True
                result["status"] = "mismatch"
                diff = mongo_count - parquet_count
                result["errors"].append(
                    f"Count mismatch: MongoDB has {diff} {'more' if diff > 0 else 'fewer'} documents"
                )
            else:
                print(f"✓ Counts match: {mongo_count:,} documents")

        except Exception as e:
            result["status"] = "error"
            result["errors"].append(str(e))

        return result

    def _build_query_filter(self, collection_name: str) -> Dict:
        """Build same query filter used in export."""
        from datetime import datetime

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

    def verify_all(self):
        """Verify all exported collections."""
        self.connect_mongodb()

        databases = self._get_databases_to_verify()

        total_collections = 0
        for database in databases:
            collections = self._get_collections_to_verify(database)
            total_collections += len(collections)

        print(f"\nVerifying {total_collections} collections across {len(databases)} databases...\n")

        for database in databases:
            collections = self._get_collections_to_verify(database)

            for collection in collections:
                result = self.verify_collection(database, collection)
                self.verification_results.append(result)

        self._print_summary()

    def _get_databases_to_verify(self) -> List[str]:
        """Get list of databases to verify."""
        all_dbs = self.client.list_database_names()
        exclude_dbs = set(self.config["export"]["exclude_databases"])

        if "*" in self.config["export"]["databases"]:
            return [db for db in all_dbs if db not in exclude_dbs]
        else:
            return [
                db for db in self.config["export"]["databases"]
                if db in all_dbs and db not in exclude_dbs
            ]

    def _get_collections_to_verify(self, database: str) -> List[str]:
        """Get list of collections to verify for a database."""
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
        """Print verification summary."""
        print("\n" + "=" * 80)
        print("VERIFICATION SUMMARY")
        print("=" * 80)

        total = len(self.verification_results)
        success = sum(1 for r in self.verification_results if r["status"] == "success")
        mismatch = sum(1 for r in self.verification_results if r["status"] == "mismatch")
        missing = sum(1 for r in self.verification_results if r["status"] == "missing")
        errors = sum(1 for r in self.verification_results if r["status"] == "error")

        print(f"Total collections verified: {total}")
        print(f"✓ Success: {success}")

        if mismatch > 0:
            print(f"⚠ Count mismatch: {mismatch}")
        if missing > 0:
            print(f"✗ Missing exports: {missing}")
        if errors > 0:
            print(f"✗ Errors: {errors}")

        print("\nDetailed Results:")
        for result in self.verification_results:
            status_symbol = {
                "success": "✓",
                "mismatch": "⚠",
                "missing": "✗",
                "error": "✗",
            }[result["status"]]

            print(f"\n{status_symbol} {result['database']}.{result['collection']}")
            print(f"  MongoDB: {result['mongodb_count']:,} documents")
            print(f"  Parquet: {result['parquet_count']:,} documents")

            if result["errors"]:
                for error in result["errors"]:
                    print(f"  Error: {error}")

        print("\n" + "=" * 80)

        # Save results to file
        with open("verification_results.json", "w") as f:
            json.dump(self.verification_results, f, indent=2)
        print("\nDetailed results saved to: verification_results.json")

        # Exit with error code if any issues
        if mismatch > 0 or missing > 0 or errors > 0:
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Verify MongoDB to Parquet exports")
    parser.add_argument(
        "-c", "--config", default="config.yaml", help="Path to configuration file"
    )
    args = parser.parse_args()

    verifier = ExportVerifier(args.config)
    verifier.verify_all()


if __name__ == "__main__":
    main()
