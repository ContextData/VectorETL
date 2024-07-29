import logging
import pandas as pd
import lancedb
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LanceDBTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.db = None
        self.table = None

    def connect(self):
        logger.info("Connecting to LanceDB...")
        self.db = lancedb.connect(
            uri=f"db://{self.config['project_name']}",
            api_key=self.config["lancedb_api_key"],
            region="us-east-1"
        )
        logger.info("Connected to LanceDB successfully.")

    def create_index_if_not_exists(self, data):
        if self.db is None:
            self.connect()

        table_name = self.config["table_name"]
        try:
            self.table = self.db.open_table(table_name)
            self.table.add(data=data, mode="append", on_bad_vectors="error")
        except Exception:
            logger.info(f"Creating LanceDB table: {table_name}")
            # schema = {
            #     "vector": "vector",
            #     "id": "string",
            #     "text": "string",
            #     "domain": "string"
            # }
            self.table = self.db.create_table(table_name, data=data)


    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to LanceDB...")

        df = df.rename(columns={'embeddings': 'vector', '__concat_final': 'concat_final'})

        if domain:
            df['domain'] = domain

        if len(columns) == 0:
            df_columns = df.columns.tolist()
            result = df[df_columns].apply(lambda row: row.to_dict(), axis=1).tolist()
        elif len(columns) > 0:
            columns += ['vector', 'concat_final', 'domain']
            result = df[columns].apply(lambda row: row.to_dict(), axis=1).tolist()


        if self.db is None:
            self.create_index_if_not_exists(result)
        else:
            lancedb_table.add(data=result, mode="append", on_bad_vectors="error")

        logger.info("Completed writing embeddings to LanceDB.")
