import logging
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PineconeTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.client = None
        self.index = None

    def connect(self):
        logger.info("Connecting to Pinecone...")
        self.client = Pinecone(api_key=self.config["pinecone_api_key"], source_tag="contextdata")
        # pc = Pinecone(api_key=self.config["pinecone_api_key"], source_tag="contextdata")

        # self.index = pc.Index(self.config["index_name"])
        logger.info("Connected to Pinecone successfully.")

    def create_index_if_not_exists(self, dimension):
        if self.client is None:
            self.connect()

        pinecone_indexes = self.client.list_indexes()
        # existing_indexes = list(map(lambda x: x['name'], pinecone_indexes))

        if self.config["index_name"] not in list(map(lambda x: x['name'], pinecone_indexes)):
            logger.info(f"Creating Pinecone index: {self.config['index_name']}")
            self.client.create_index(
                name=self.config["index_name"],
                dimension=dimension,#self.config.get("dimension", 1536),
                metric=self.config.get("metric", "cosine"),
                spec=ServerlessSpec(
                    cloud=self.config.get("cloud", "aws"),
                    region=self.config.get("region", "us-east-1")
                )
            )
        self.index = self.client.Index(self.config["index_name"])

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Pinecone...")
        if self.index is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))#self.connect()

        upload_data = []
        for _, row in df.iterrows():
            if len(columns) > 0:
                columns.append("__concat_final")
                metadata_data = {col: str(row[col]) for col in columns}

                data = {
                    "id": str(row["df_uuid"]),
                    "values": row["embeddings"],
                    "metadata": metadata_data
                }
                columns.remove("__concat_final")
            elif len(columns) == 0:
                data = {
                    "id": str(row["df_uuid"]),
                    "values": row["embeddings"],
                    "metadata": {
                        col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                        for col in df.columns if col not in ["df_uuid", "embeddings"] and pd.notna(row[col])
                    }
                }
            if domain:
                data["metadata"]["domain"] = domain
            upload_data.append(data)

        self.index.upsert(vectors=upload_data)
        logger.info("Completed writing embeddings to Pinecone.")
