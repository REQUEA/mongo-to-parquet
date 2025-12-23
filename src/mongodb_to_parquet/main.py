import json
import logging
import pymongo
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import argparse
import concurrent.futures

# =====================================================
# CONFIG
# =====================================================

class AppConfig:
    def __init__(self, path: str):
        with open(path) as f:
            self.raw = json.load(f)

        self.mongodb_host = self.raw["mongodb_host"]
        self.mongodb_port = self.raw["mongodb_port"]
        self.output_dir = self.raw.get("output_dir", "./output")
        self.compression = self.raw.get("compression", "zstd")

        self.start_date = self._parse_date(self.raw.get("start_date"))
        self.end_date = self._parse_date(self.raw.get("end_date"))

        self.include_databases = set(self.raw.get("include_databases", []))
        self.exclude_databases = set(self.raw.get("exclude_databases", []))

        if self.include_databases and self.exclude_databases:
            raise ValueError("Cannot specify both include_databases and exclude_databases")

        self.date_collections = self.raw.get("date_collections", {})

    def _parse_date(self, value):
        if not value:
            return None
        return datetime.fromisoformat(value)

# =====================================================
# LOGGING
# =====================================================

def create_logger():
    logger = logging.getLogger("mongo_to_parquet")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("mongodb_to_parquet.log")
    formatter = logging.Formatter(
        '{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# =====================================================
# MONGO CONNECTION
# =====================================================

class MongoConnection:
    def __init__(self, cfg: AppConfig):
        self.client = pymongo.MongoClient(
            cfg.mongodb_host,
            cfg.mongodb_port,
            serverSelectionTimeoutMS=5000
        )

    def list_databases(self):
        return self.client.list_database_names()

    def get_database(self, name):
        return self.client[name]

    def start_session(self):
        return self.client.start_session()

# =====================================================
# PARQUET WRITER
# =====================================================

class ParquetWriterService:
    ROW_GROUP_SIZE = 4000_000

    def __init__(self, output_dir, compression, logger):
        self.output_dir = output_dir
        self.compression = compression
        self.logger = logger

    def _enrich_partitions(self, doc, date_field):
        if not date_field:
            return doc
        dt = doc.get(date_field)
        if isinstance(dt, datetime):
            doc["year"] = dt.year
            doc["month"] = dt.month
            doc["day"] = dt.day
        return doc

    def write_collection(self, cursor, db_name, collection_name, date_field):
        base_path = Path(self.output_dir) / db_name / collection_name
        base_path.mkdir(parents=True, exist_ok=True)

        buffer = []
        schema = None
        total = 0

        for doc in cursor:
            doc.pop("_id", None)
            doc = self._enrich_partitions(doc, date_field)
            buffer.append(doc)

            if len(buffer) >= self.ROW_GROUP_SIZE:
                total += self._flush(buffer, base_path, schema)
                schema = schema or pa.Table.from_pylist(buffer).schema
                buffer.clear()

        if buffer:
            total += self._flush(buffer, base_path, schema)

        return total

    def _flush(self, buffer, path, schema):
        table = pa.Table.from_pylist(buffer, schema=schema)
        filename = f"part-{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"

        pq.write_table(
            table,
            path / filename,
            compression=self.compression
        )

        self.logger.info(f"Wrote {len(buffer)} rows to {path / filename}")
        return len(buffer)

# =====================================================
# EXPORT JOB
# =====================================================

class ExportJob:
    def __init__(self, cfg: AppConfig, mongo: MongoConnection, logger):
        self.cfg = cfg
        self.mongo = mongo
        self.logger = logger
        self.writer = ParquetWriterService(cfg.output_dir, cfg.compression, logger)

    def run(self):
        all_db_names = set(self.mongo.list_databases())

        if self.cfg.include_databases:
            db_names = all_db_names & self.cfg.include_databases
        elif self.cfg.exclude_databases:
            db_names = all_db_names - self.cfg.exclude_databases
        else:
            db_names = all_db_names

        if not db_names:
            self.logger.warning("No databases to process.")
            return

        self.logger.info(f"Databases to process: {db_names}")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(4, len(db_names))
        ) as executor:
            futures = [
                executor.submit(self._process_database, db_name)
                for db_name in db_names
            ]
            concurrent.futures.wait(futures)

    def _process_database(self, db_name):
        self.logger.info(f"START DB {db_name}")
        db = self.mongo.get_database(db_name)

        for coll_name, date_field in self.cfg.date_collections.items():
            if coll_name in db.list_collection_names():
                self._process_collection(db, coll_name, date_field)
            else:
                self.logger.info(f"Collection {coll_name} not found in {db_name}, skipping.")

        self.logger.info(f"END DB {db_name}")

    def _process_collection(self, db, coll_name, date_field):
        query = {}
        if date_field:
            if self.cfg.start_date:
                query.setdefault(date_field, {})["$gte"] = self.cfg.start_date
            if self.cfg.end_date:
                query.setdefault(date_field, {})["$lte"] = self.cfg.end_date

        self.logger.info(f"START {db.name}.{coll_name} | query={query}")

        with self.mongo.start_session() as session:
            cursor = db[coll_name].find(
                query,
                no_cursor_timeout=True,
                batch_size=10_000,
                session=session
            )
            try:
                total = self.writer.write_collection(cursor, db.name, coll_name, date_field)
            finally:
                cursor.close()

        self.logger.info(f"END {db.name}.{coll_name} | documents={total}")

# =====================================================
# MAIN
# =====================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", required=True, help="Path to config JSON")
    args = parser.parse_args()

    cfg = AppConfig(args.config)
    logger = create_logger()
    mongo = MongoConnection(cfg)

    job = ExportJob(cfg, mongo, logger)
    job.run()
