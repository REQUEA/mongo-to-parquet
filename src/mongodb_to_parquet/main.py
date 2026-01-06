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

        self.batch_size = self.raw.get("bacth_size", 10_000)
        self.row_group_size = self.raw.get("row_group_size", 400_000)


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
    def __init__(self, output_dir, compression, logger, row_group_size):
        self.output_dir = output_dir
        self.compression = compression
        self.logger = logger
        self.row_group_size = row_group_size

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
        writers = {}
        total = 0
    
        for doc in cursor:
            doc.pop("_id", None)
            doc = self._enrich_partitions(doc, date_field)
    
            year = doc.get("year", "unknown")
    
            base_path = (
                Path(self.output_dir)
                / db_name
                / collection_name
                / f"year={year}"
            )
            base_path.mkdir(parents=True, exist_ok=True)
    
            if year not in writers:
                writers[year] = {
                    "buffer": [],
                    "writer": None,
                    "schema": None,
                    "rows": 0,
                    "path": base_path
                }
    
            writers[year]["buffer"].append(doc)
    
            if len(writers[year]["buffer"]) >= self.row_group_size:
                w = writers[year]
                w["writer"], w["schema"], written, w["rows"] = self._flush(
                    w["buffer"],
                    w["path"],
                    w["writer"],
                    w["schema"],
                    w["rows"]
                )
                total += written
                w["buffer"].clear()
    
        # flush final
        for w in writers.values():
            if w["buffer"]:
                w["writer"], w["schema"], written, w["rows"] = self._flush(
                    w["buffer"],
                    w["path"],
                    w["writer"],
                    w["schema"],
                    w["rows"]
                )
                total += written
    
            if w["writer"]:
                w["writer"].close()
    
        return total



#    def write_collection(self, cursor, db_name, collection_name, date_field):
#        base_path = Path(self.output_dir) / db_name / collection_name
#        base_path.mkdir(parents=True, exist_ok=True)
#
#        buffer = []
#        writer = None
#        schema = None
#        total = 0
#        rows_in_file = 0
#
#        for doc in cursor:
#            doc.pop("_id", None)
#            doc = self._enrich_partitions(doc, date_field)
#            buffer.append(doc)
#
#            if len(buffer) >= self.row_group_size:
#                writer, schema, written, rows_in_file = self._flush(
#                    buffer,
#                    base_path,
#                    writer,
#                    schema,
#                    rows_in_file
#                )
#                total += written
#                buffer.clear()
#
#        if buffer:
#            writer, schema, written, rows_in_file = self._flush(
#                buffer,
#                base_path,
#                writer,
#                schema,
#                rows_in_file
#            )
#            total += written
#
#        if writer:
#            writer.close()
#
#        return total

    def _new_writer(self, path, schema):
        filename = f"part-{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        return pq.ParquetWriter(
            path / filename,
            schema,
            compression=self.compression
        )

    def _flush(self, buffer, path, writer, schema, rows_in_file):
        if schema is None:
            table = pa.Table.from_pylist(buffer)
            schema = table.schema
            writer = self._new_writer(path, schema)
            rows_in_file = 0
        else:
            table = pa.Table.from_pylist(buffer, schema=schema)

        for batch in table.to_batches():
            writer.write_batch(batch)

        rows_in_file += len(buffer)

        if rows_in_file >= self.row_group_size * 10:
            writer.close()
            writer = self._new_writer(path, schema)
            rows_in_file = 0

        self.logger.info(
            f"Wrote {len(buffer)} rows to {path}"
        )

        return writer, schema, len(buffer), rows_in_file


# =====================================================
# EXPORT JOB
# =====================================================

class ExportJob:
    def __init__(self, cfg: AppConfig, mongo: MongoConnection, logger):
        self.cfg = cfg
        self.mongo = mongo
        self.logger = logger
        self.BATCH_SIZE = cfg.batch_size
        self.ROW_GROUP_SIZE = cfg.row_group_size
        self.writer = ParquetWriterService(cfg.output_dir, cfg.compression, logger, self.ROW_GROUP_SIZE)

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
            max_workers=6
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
                batch_size=self.BATCH_SIZE,
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
