import logging
import pandas as pd
import singlestoredb as s2
import json
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SingleStoreTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        logger.info("Connecting to SingleStore...")
        connection_url = f"{self.config['singlestore_username']}:{self.config['singlestore_password']}@{self.config['singlestore_host']}:{self.config['singlestore_port']}/{self.config['singlestore_database_name']}"
        self.connection = s2.connect(connection_url)
        logger.info("Connected to SingleStore successfully.")

    def write_data(self, df, domain=None):
        logger.info("Writing embeddings to SingleStore...")
        if self.connection is None:
            self.connect()

        table = self.config["singlestore_table"]

        with self.connection.cursor() as cursor:
            for _, row in df.iterrows():
                metadata = {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if col not in ["df_uuid", "embeddings", "__concat_final"] and pd.notna(row[col])
                }
                if domain:
                    metadata["domain"] = domain

                embedding = str(row["embeddings"])
                cursor.execute(
                    f"INSERT INTO {table} VALUES (%s, JSON_ARRAY_PACK(%s), %s)",
                    (str(row["df_uuid"]), embedding, json.dumps(metadata))
                )

        self.connection.commit()
        logger.info("Completed writing embeddings to SingleStore.")
