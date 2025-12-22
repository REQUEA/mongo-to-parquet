import pymongo
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import json
import os
import argparse
from datetime import datetime
from pymongo.errors import ConnectionFailure



OUTPUT_DIR = "./output"
LOG_FILE = "./mongodb_to_parquet.log"
METADATA_FILE = "executions/metadata.json"

BATCH_SIZE = 10_000

NOW = datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M%S")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Export MongoDB collections to Parquet"
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to configuration JSON file",
    )
    return parser.parse_args()


def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)


def create_logger():
    logger = logging.getLogger("mongo_to_parquet")
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter(
        '{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


def get_mongo_client(cfg):
    try:
        return pymongo.MongoClient(
            cfg["mongodb_host"],
            cfg["mongodb_port"],
            serverSelectionTimeoutMS=5000,
        )
    except ConnectionFailure as e:
        raise RuntimeError(f"MongoDB connection failed: {e}")



def build_query(date_field, start_date, end_date):
    if not date_field:
        return {}

    query = {}
    cond = {}
    if start_date:
        cond["$gte"] = start_date
    if end_date:
        cond["$lte"] = end_date

    if cond:
        query[date_field] = cond
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


def write_batch(docs, output_path, partitioned):
    table = pa.Table.from_pylist(docs)

    if partitioned:
        pq.write_to_dataset(
            table,
            root_path=output_path,
            partition_cols=["year", "month", "day"],
            existing_data_behavior="overwrite_or_ignore",
            compression="zstd",
        )
    else:
        os.makedirs(output_path, exist_ok=True)
        pq.write_table(
            table,
            os.path.join(output_path, "data.parquet"),
            compression="zstd",
        )


def process_collection(
    db,
    collection_name,
    collection_cfg,
    logger,
    metadata,
    output_dir,
    batch_size
):
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
    partitioned = bool(date_field)

    batch = []
    total_written = 0

    logger.info(
        json.dumps(
            {
                "db": db.name,
                "collection": collection_name,
                "event": "start_collection",
            }
        )
    )
    with db.client.start_session() as session:
        cursor = (
            collection.find(
                query,
                no_cursor_timeout=True,
                session=session
            )
            .batch_size(batch_size)
        )
        try:
            for doc in cursor:
                doc.pop("_id", None)
                doc = enrich_with_partitions(doc, date_field)
                batch.append(doc)

                if len(batch) >= batch_size:
                    write_batch(batch, output_path, partitioned)
                    total_written += len(batch)
                    batch.clear()

                    logger.info(
                        json.dumps(
                            {
                                "db": db.name,
                                "collection": collection_name,
                                "written": total_written,
                            }
                        )
                    )

            if batch:
                write_batch(batch, output_path, partitioned)
                total_written += len(batch)
        
        finally:
            cursor.close()

    metadata[db.name][collection_name] = {
        "estimated_total": collection.estimated_document_count(),
        "documents_copied": total_written,
        "partitioned": partitioned,
    }

    logger.info(
        json.dumps(
            {
                "db": db.name,
                "collection": collection_name,
                "event": "end_collection",
                "total_written": total_written,
            }
        )
    )


def format_batch_size(batch_size):
    return f"{batch_size:_}"



def main():

    args = parse_args()

    cfg = load_config(args.config)
    logger = create_logger()
    client = get_mongo_client(cfg)
    output_dir = cfg.get("output_dir", OUTPUT_DIR)
    batch_size = cfg.get("batch_size", BATCH_SIZE)
    batch_size = format_batch_size(batch_size)

    dbs = cfg.get("databases") or client.list_database_names()
    metadata = {}

    for db_name, db_cfg in dbs.items():
        db = client[db_name]
        metadata[db_name] = {}

        collections_cfg = db_cfg.get("collections", {})

        for collection_name, collection_cfg in collections_cfg.items():
            process_collection(
                db,
                collection_name,
                collection_cfg,
                logger,
                metadata,
                output_dir,
                batch_size
            )
    
        metadata_path = os.path.join(output_dir, db_name, f"{TIMESTAMP}_{METADATA_FILE}")

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    logger.info(json.dumps({"event": "job_finished"}))
