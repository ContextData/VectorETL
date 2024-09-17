import logging
import pandas as pd
from pymilvus import MilvusClient, DataType
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MilvusTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.client = None
        # self.collection = None

    def connect(self):
        logger.info("Connecting to Milvus...")
        try:
            self.client = MilvusClient(uri=self.config['host'], token=self.config["api_key"])

            logger.info("Connected to Milvus successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Milvus: {str(e)}")
            raise

    def create_index_if_not_exists(self, dimension):
        if self.client is None:
            self.connect()

        collection_name = self.config['collection_name']
        dim = self.config['vector_dim']

        if self.client.has_collection(collection_name):
            logger.info(f"Collection '{collection_name}' already exists.")
        else:

            # Create Schema
            schema = self.client.create_schema(
                auto_id=False,
                enable_dynamic_field=True,
            )

            # Add fields to schema
            schema.add_field(field_name="id", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
            schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=dimension)

            # Add indexes
            index_params = self.client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="AUTOINDEX",
                metric_type="COSINE",
                params={"nlist": 128}
            )

            # Create Collection
            self.client.create_collection(
                collection_name=self.config['collection_name'],
                schema=schema,
                index_params=index_params
            )

            logger.info(f"Created collection '{collection_name}'.")


    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Milvus...")

        if self.client is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))


        entities = []
        for _, row in df.iterrows():
            entity = {
                'id': str(row['df_uuid']),
                'vector': row['embeddings'],
                'metadata': {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if col not in ['df_uuid', 'embeddings'] and pd.notna(row[col])
                }
            }
            if domain:
                entity['metadata']['domain'] = domain
            entities.append(entity)

        try:
            insert_result = self.client.insert(collection_name=self.config['collection_name'], data=entities)
            logger.info(f"Successfully inserted {insert_result['insert_count']} entities into Milvus.")
        except Exception as e:
            logger.error(f"Failed to insert entities into Milvus: {str(e)}")
            raise

        logger.info("Completed writing embeddings to Milvus.")

