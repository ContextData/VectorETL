import logging
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.client = None
        self.db = None
        self.collection = None

    def connect(self):
        logger.info("Connecting to MongoDB...")
        try:
            self.client = MongoClient(self.config['mongodb_uri'])
            self.db = self.client[self.config['database_name']]
            self.collection = self.db[self.config['collection_name']]

            self.client.admin.command('ismaster')
            logger.info("Connected to MongoDB successfully.")
        except ConnectionFailure:
            logger.error("Server not available")
            raise

    def create_index_if_not_exists(self):
        if self.collection is None:
            self.connect()

        index_name = f"{self.config['vector_field']}_index"
        if index_name not in self.collection.index_information():
            logger.info(f"Creating index: {index_name}")
            self.collection.create_index([(self.config['vector_field'], "2dsphere")], name=index_name)
            logger.info(f"Index {index_name} created successfully.")

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to MongoDB...")
        if self.collection is None:
            self.connect()

        self.create_index_if_not_exists()

        documents = []
        for _, row in df.iterrows():
            document = {
                'id': str(row['df_uuid']),
                self.config['vector_field']: row['embeddings'],
                'metadata': {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if col not in ['df_uuid', 'embeddings'] and pd.notna(row[col])
                }
            }
            if domain:
                document['metadata']['domain'] = domain
            documents.append(document)

        try:
            result = self.collection.insert_many(documents)
            logger.info(f"Successfully inserted {len(result.inserted_ids)} documents.")
        except Exception as e:
            logger.error(f"Error inserting documents: {str(e)}")
            raise

        logger.info("Completed writing embeddings to MongoDB.")
