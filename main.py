import pymongo
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import json
import os
from datetime import datetime
from pymongo.errors import ConnectionError



CONFIG_FILE = 'config.json'
LOG_FILE = 'mongodb_to_parquet.log'
METADATA_FILE = 'metadata.json'

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    return config



def get_mongo_client(host, port, db_name=None):
    """Se connecter à la base de données MongoDB."""
    try:
        client = pymongo.MongoClient(host, port)
        if db_name:
            db = client[db_name]
            return db
        return client
    except ConnectionError as e:
        logging.error(f"Erreur de connexion à MongoDB : {e}")
        raise



def create_logger():
    """Crée un logger pour les logs."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    return logger



def filter_documents(collection, date_field, start_date=None, end_date=None):
    """Filtrer les documents MongoDB selon la date."""
    query = {}
    if date_field:
        if start_date:
            query[date_field] = {'$gte': start_date}
        if end_date:
            query[date_field] = {'$lte': end_date}
    return collection.find(query)



def save_to_parquet(documents, db_name, collection_name, date_field=None):
    """Sauvegarder les documents sous forme de fichiers Parquet avec PyArrow."""
    path = f'./output/{db_name}/{collection_name}/'
    os.makedirs(path, exist_ok=True)

    table_data = []
    for doc in documents:
        doc.pop('_id', None)
        table_data.append(doc)

    if not table_data:
        return

    schema = pa.schema([
        (key, pa.string()) for key in table_data[0].keys()
    ])

    table = pa.Table.from_pandas(pd.DataFrame(table_data), schema=schema)

    if date_field:
        for row in table.to_pandas().iterrows():
            row = row[1]
            date = row[date_field]
            if isinstance(date, datetime):
                year, month, day = date.year, date.month, date.day
                file_path = f'{path}/{year}/{month}/{day}/data.parquet'
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                pq.write_table(table, file_path)
    else:
        file_path = f'{path}/data.parquet'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        pq.write_table(table, file_path)



def save_metadata(metadata):
    """Sauvegarder les métadonnées dans un fichier JSON."""
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)



def process_mongodb_to_parquet():
    config = load_config()
    logger = create_logger()

    client = get_mongo_client(config['mongodb_host'], config['mongodb_port'])
    
    # Get all databases if not configured
    dbs_to_process = config.get('databases', [])
    if not dbs_to_process:
        dbs_to_process = client.list_database_names()

    metadata = {}

    for db_name in dbs_to_process:
        db = client[db_name]
        logger.info(f"Traitement de la base de données: {db_name}")
        
        # Get all collections if not configured
        collections_to_process = config.get('collections', [])
        if not collections_to_process:
            collections_to_process = db.list_collection_names()
        
        for collection_name in collections_to_process:
            collection = db[collection_name]
            logger.info(f"Traitement de la collection: {collection_name}")
            
            date_field = config.get('date_field')
            start_date = config.get('start_date')
            end_date = config.get('end_date')

            if start_date:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            documents = filter_documents(collection, date_field, start_date, end_date)
            
            documents_list = list(documents)
            if not documents_list:
                logger.info(f"Aucun document trouvé pour {db_name}.{collection_name}.")
                continue

            save_to_parquet(documents_list, db_name, collection_name, date_field)
            
            metadata[db_name] = metadata.get(db_name, {})
            metadata[db_name][collection_name] = {
                "total_documents": len(documents_list),
                "documents_copied": len(documents_list)
            }
            
            logger.info(f"Documents copiés pour {db_name}.{collection_name}: {len(documents_list)}")

    save_metadata(metadata)
    logger.info(f"Processus terminé. Métadonnées sauvegardées dans {METADATA_FILE}")


if __name__ == '__main__':
    process_mongodb_to_parquet()

