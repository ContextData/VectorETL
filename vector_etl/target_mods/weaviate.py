import logging
import pandas as pd
import weaviate
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeaviateTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.client = None

    def connect(self):
        logger.info("Connecting to Weaviate...")
        self.client = weaviate.Client(
            url=self.config["weaviate_url"],
            auth_client_secret=weaviate.AuthApiKey(api_key=self.config["weaviate_api_key"]),
        )
        logger.info("Connected to Weaviate successfully.")

    def create_index_if_not_exists(self, dimension):
        #dimension not needed to create Weaviate class but included nevertheless
        if self.client is None:
            self.connect()

        class_name = self.config["class_name"]
        if not self.client.schema.exists(class_name):
            logger.info(f"Creating Weaviate class: {class_name}")
            class_obj = {
                "class": class_name,
                "vectorizer": "none",
                "vectorIndexType": "hnsw",
                "properties": [
                    # {
                    #     "name": "text",
                    #     "dataType": ["text"],
                    # },
                    # {
                    #     "name": "domain",
                    #     "dataType": ["string"],
                    # }
                ]
            }
            self.client.schema.create_class(class_obj)

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Weaviate...")
        if self.client is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))

        class_name = self.config["class_name"]

        # dropping id column if it exists as it is a reserved column in weaviate
        id_columns = [col for col in df.columns if col.lower() == 'id']
        df = df.drop(id_columns, axis=1, errors='ignore')

        with self.client.batch as batch:
            for _, row in df.iterrows():
                if len(columns) > 0:
                    # columns.append("__concat_final")
                    metadata = {col: str(row[col]) for col in columns}


                    if domain:
                        metadata["domain"] = domain

                    params = {
                        "uuid": str(row["df_uuid"]),
                        "data_object": metadata,
                        "class_name": class_name,
                        "vector": row["embeddings"]
                    }
                else:
                    metadata = {
                        col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                        for col in df.columns if
                        col not in ["df_uuid", "embeddings", "__concat_final"] and pd.notna(row[col])
                    }
                    if domain:
                        metadata["domain"] = domain

                    params = {
                        "uuid": str(row["df_uuid"]),
                        "data_object": metadata,
                        "class_name": class_name,
                        "vector": row["embeddings"]
                    }

                batch.add_data_object(**params)

        logger.info("Completed writing embeddings to Weaviate.")
