import psutil
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
BATCH_SIZE_DEFAULT = 10_000
NOW = datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M%S")

def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)

def parse_args():
    parser = argparse.ArgumentParser(description="Export MongoDB collections to Parquet")
    parser.add_argument("--config", "-c", required=True, help="Path to configuration JSON file")
    return parser.parse_args()

def create_logger():
    logger = logging.getLogger("mongo_to_parquet")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter('{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def get_mongo_client(cfg):
    try:
        return pymongo.MongoClient(cfg["mongodb_host"], cfg["mongodb_port"], serverSelectionTimeoutMS=5000)
    except pymongo.errors.ConnectionFailure as e:
        raise RuntimeError(f"MongoDB connection failed: {e}")

def get_average_document_size(collection):
    sample_docs = collection.find().limit(1000)  # 1000 documents pour échantillon
    total_size = sum([len(str(doc).encode('utf-8')) for doc in sample_docs])
    avg_size = total_size / 1000 if total_size > 0 else 0
    return avg_size

def get_dynamic_batch_size(collection, target_memory_mb=100):
    avg_size = get_average_document_size(collection)
    
    if avg_size == 0:
        return BATCH_SIZE_DEFAULT  # Par défaut, si la taille est trop petite ou nulle
    
    target_memory_bytes = target_memory_mb * 1024 * 1024  # Convertir en octets
    dynamic_batch_size = target_memory_bytes // avg_size
    
    return max(1, min(dynamic_batch_size, 100_000))

def get_available_memory():
    mem = psutil.virtual_memory()
    return mem.available / (1024 * 1024)  # Convertir en Mo

def get_dynamic_batch_size_with_memory(collection):
    available_memory_mb = get_available_memory()
    print(f"Available memory: {available_memory_mb} MB")
    return get_dynamic_batch_size(collection, target_memory_mb=available_memory_mb // 2)  # Utiliser la moitié de la mémoire

def process_collection(db, collection_name, collection_cfg, logger, metadata, output_dir, batch_size=None, compression="zstd"):
    collection = db[collection_name]

    if batch_size is None:
        batch_size = get_dynamic_batch_size_with_memory(collection)

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

    logger.info(json.dumps({"db": db.name, "collection": collection_name, "event": "start_collection"}))
    
    with db.client.start_session() as session:
        cursor = collection.find(query, no_cursor_timeout=True, session=session).batch_size(batch_size)
        try:
            for doc in cursor:
                doc.pop("_id", None)
                doc = enrich_with_partitions(doc, date_field)
                batch.append(doc)

                if len(batch) >= batch_size:
                    write_batch(batch, output_path, partitioned, compression)
                    total_written += len(batch)
                    batch.clear()

                    logger.info(json.dumps({"db": db.name, "collection": collection_name, "written": total_written}))
            
            if batch:
                write_batch(batch, output_path, partitioned, compression)
                total_written += len(batch)

        finally:
            cursor.close()

    metadata[db.name][collection_name] = {"estimated_total": collection.estimated_document_count(), "documents_copied": total_written}
    logger.info(json.dumps({"db": db.name, "collection": collection_name, "event": "end_collection", "total_written": total_written}))

def write_batch(docs, output_path, partitioned, compression):
    table = pa.Table.from_pylist(docs)

    if partitioned:
        pq.write_to_dataset(
            table,
            root_path=output_path,
            partition_cols=["year"],  # Partitionner par année uniquement
            existing_data_behavior="overwrite_or_ignore",
            compression=compression,
        )
    else:
        os.makedirs(output_path, exist_ok=True)
        pq.write_table(
            table,
            os.path.join(output_path, "data.parquet"),
            compression=compression,
        )

def process_database(db, db_cfg, logger, metadata, output_dir, batch_size, compression):
    metadata[db.name] = {}
    collections_cfg = db_cfg.get("collections", {})
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for collection_name, collection_cfg in collections_cfg.items():
            futures.append(
                executor.submit(
                    process_collection,
                    db,
                    collection_name,
                    collection_cfg,
                    logger,
                    metadata,
                    output_dir,
                    batch_size,
                    compression
                )
            )
        
        concurrent.futures.wait(futures)

def main():
    args = parse_args()
    cfg = load_config(args.config)
    logger = create_logger()
    client = get_mongo_client(cfg)
    output_dir = cfg.get("output_dir", OUTPUT_DIR)
    batch_size = cfg.get("batch_size", BATCH_SIZE_DEFAULT)
    compression = cfg.get("compression", "zstd")

    dbs = cfg.get("databases") or client.list_database_names()
    metadata = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for db_name, db_cfg in dbs.items():
            db = client[db_name]
            futures.append(
                executor.submit(
                    process_database,
                    db,
                    db_cfg,
                    logger,
                    metadata,
                    output_dir,
                    batch_size,
                    compression
                )
            )

        concurrent.futures.wait(futures)

    metadata_path = Path(output_dir) / "executions" / f"{TIMESTAMP}_{METADATA_FILE}"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(json.dumps({"event": "job_finished"}))

