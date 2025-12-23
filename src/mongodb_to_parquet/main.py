import pymongo
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging
import os
from pathlib import Path
from datetime import datetime
import argparse
import concurrent.futures


OUTPUT_DIR = "./output"
LOG_FILE = "./mongodb_to_parquet.log"
METADATA_FILE = "metadata.json"

BATCH_SIZE = 10_000           
ROW_GROUP_SIZE = 100_000    
NOW = datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M%S")


def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)

def parse_args():
    parser = argparse.ArgumentParser(description="Export MongoDB to Parquet")
    parser.add_argument("--config", "-c", required=True)
    return parser.parse_args()


def create_logger():
    logger = logging.getLogger("mongo_to_parquet")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter(
        '{"ts":"%(asctime)s","level":"%(levelname)s","function":"%(funcName)s","msg":"%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_mongo_client(cfg):
    return pymongo.MongoClient(
        cfg["mongodb_host"],
        cfg["mongodb_port"],
        serverSelectionTimeoutMS=5000
    )

def build_query(date_field, start_date, end_date):
    if not date_field:
        return {}

    query = {}
    if start_date:
        query.setdefault(date_field, {})["$gte"] = start_date
    if end_date:
        query.setdefault(date_field, {})["$lte"] = end_date
    return query

def enrich_with_partitions(doc, date_field):
    if not date_field:
        return doc

    dt = doc.get(date_field)
    if isinstance(dt, datetime):
        doc["year"] = dt.year
        doc["month"] = dt.month
        doc["day"] = dt.day
    return doc


def write_parquet_stream(cursor, output_path, compression, date_field, logger):
    buffer = []
    total_written = 0
    schema = None

    os.makedirs(output_path, exist_ok=True)

    for doc in cursor:
        doc.pop("_id", None)
        doc = enrich_with_partitions(doc, date_field)
        buffer.append(doc)

        if schema is None and buffer:
            table = pa.Table.from_pylist(buffer)
            schema = table.schema

        if len(buffer) >= ROW_GROUP_SIZE:
            table = pa.Table.from_pylist(buffer, schema=schema)
            filename = f"part-{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
            filepath = os.path.join(output_path, filename)
            pq.write_table(table, filepath, compression=compression)
            total_written += len(buffer)
            buffer.clear()
            logger.info(f"Wrote {total_written} documents to {filepath}")

    if buffer:
        table = pa.Table.from_pylist(buffer, schema=schema)
        filename = f"part-{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        filepath = os.path.join(output_path, filename)
        pq.write_table(table, filepath, compression=compression)
        total_written += len(buffer)
        logger.info(f"Wrote final {len(buffer)} documents to {filepath}")

    return total_written


def process_collection(db, collection_name, collection_cfg, logger, metadata, output_dir, compression):
    collection = db[collection_name]

    date_field = collection_cfg.get("date_field")
    start_date = collection_cfg.get("start_date")
    end_date = collection_cfg.get("end_date")

    if start_date:
        start_date = datetime.fromisoformat(start_date)
    if end_date:
        end_date = datetime.fromisoformat(end_date)

    query = build_query(date_field, start_date, end_date)
    output_path = os.path.join(output_dir, db.name, collection_name)

    logger.info(f"Starting collection {db.name}.{collection_name}")

    with db.client.start_session() as session:
        cursor = collection.find(
            query,
            no_cursor_timeout=True,
            batch_size=BATCH_SIZE,
            session=session
        )
        try:
            total_written = write_parquet_stream(cursor, output_path, compression, date_field, logger)
        finally:
            cursor.close()

    metadata[db.name][collection_name] = {
        "estimated_total": collection.estimated_document_count(),
        "documents_copied": total_written
    }

    logger.info(f"Finished collection {db.name}.{collection_name}, total_written={total_written}")


def process_database(db_name, db_cfg, client, logger, metadata, output_dir, compression):
    db = client[db_name]
    metadata[db_name] = {}

    collections_cfg = db_cfg.get("collections", {})

    logger.info(f"Starting database {db_name}")

    for collection_name, collection_cfg in collections_cfg.items():
        process_collection(db, collection_name, collection_cfg, logger, metadata, output_dir, compression)

    logger.info(f"Finished database {db_name}")

def main():
    args = parse_args()
    cfg = load_config(args.config)
    logger = create_logger()

    client = get_mongo_client(cfg)
    output_dir = cfg.get("output_dir", OUTPUT_DIR)
    compression = cfg.get("compression", "zstd")

    dbs = cfg.get("databases", {})
    metadata = {}
    MAX_WORKERS = 6 if len(dbs) > 6 else len(dbs)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for db_name, db_cfg in dbs.items():
            futures.append(
                executor.submit(
                    process_database,
                    db_name,
                    db_cfg,
                    client,
                    logger,
                    metadata,
                    output_dir,
                    compression
                )
            )

        concurrent.futures.wait(futures)

    metadata_path = Path(output_dir) / "executions" / f"{TIMESTAMP}_{METADATA_FILE}"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info("All databases processed successfully.")
