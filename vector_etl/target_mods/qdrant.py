import logging
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QdrantTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.client = None

    def connect(self):
        logger.info("Connecting to Qdrant...")
        self.client = QdrantClient(url=self.config["qdrant_url"], api_key=self.config["qdrant_api_key"])
        logger.info("Connected to Qdrant successfully.")

    def create_index_if_not_exists(self, dimension):
        if self.client is None:
            self.connect()

        collection_name = self.config["collection_name"]
        collections = self.client.get_collections()
        if collection_name not in [c.name for c in collections.collections]:
            logger.info(f"Creating Qdrant collection: {collection_name}")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=dimension,# self.config.get("dimension", 1536),
                    distance=Distance.COSINE
                )
            )

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Qdrant...")
        if self.client is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))

        collection_name = self.config["collection_name"]

        for _, row in df.iterrows():
            if len(columns) > 0:
                columns.append("__concat_final")
                metadata_data = {col: str(row[col]) for col in columns}

                metadata = {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if col not in ["df_uuid", "embeddings"] and pd.notna(row[col])
                }
                if domain:
                    metadata["domain"] = domain

                point = PointStruct(
                    id=str(row["df_uuid"]),
                    vector=row["embeddings"],
                    payload=metadata_data
                )
                self.client.upsert(
                    collection_name=collection_name,
                    wait=True,
                    points=[point]
                )
            else:
                metadata = {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if col not in ["df_uuid", "embeddings"] and pd.notna(row[col])
                }
                if domain:
                    metadata["domain"] = domain

                point = PointStruct(
                    id=str(row["df_uuid"]),
                    vector=row["embeddings"],
                    payload=metadata
                )
                self.client.upsert(
                    collection_name=collection_name,
                    wait=True,
                    points=[point]
                )

        logger.info("Completed writing embeddings to Qdrant.")
