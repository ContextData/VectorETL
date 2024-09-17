import pandas as pd
import logging
from .base import BaseSource
import psycopg2
import mysql.connector
import snowflake.connector
from simple_salesforce import Salesforce

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        if self.config["db_type"] == 'postgres':
            self.connection = psycopg2.connect(
                host=self.config["host"],
                database=self.config["database_name"],
                user=self.config["username"],
                password=self.config["password"],
                port=self.config["port"]
            )
        elif self.config["db_type"] == 'mysql':
            self.connection = mysql.connector.connect(
                host=self.config["host"],
                database=self.config["database_name"],
                user=self.config["username"],
                password=self.config["password"],
                port=self.config["port"]
            )
        elif self.config["db_type"] == 'snowflake':
            self.connection = snowflake.connector.connect(
                account=self.config["account"],
                database=self.config["database_name"].upper(),
                user=self.config["username"],
                password=self.config["password"],
                warehouse=self.config["warehouse_name"].upper(),
                schema=self.config["schema"].upper()
            )
        elif self.config["db_type"] == 'salesforce':
            self.connection = Salesforce(
                username=self.config["username"],
                password=self.config["password"],
                security_token=self.config["security_token"]
            )
        else:
            raise ValueError("Invalid database type")

        self.cursor = self.connection.cursor()
        logger.info(f"Connected to {self.config['db_type']} database")

    def fetch_data(self):
        if not self.connection:
            self.connect()

        query = self.config.get("query", "")
        batch_size = self.config.get("batch_size", 1000)

        if self.config["db_type"] in ['postgres', 'mysql']:
            batched_query = f"{query} LIMIT {batch_size} OFFSET %s"
        elif self.config["db_type"] == 'snowflake':
            batched_query = f"{query} LIMIT {batch_size} OFFSET %s"
        elif self.config["db_type"] == 'salesforce':
            # Salesforce uses OFFSET in the query itself
            batched_query = f"{query} LIMIT {batch_size} OFFSET %s"

        offset = 0
        while True:
            self.cursor.execute(batched_query, (offset,))
            batch = self.cursor.fetchall()

            if not batch:
                break

            columns = [desc[0] for desc in self.cursor.description]
            df_batch = pd.DataFrame(batch, columns=columns)
            logger.info(f"========== Retrieved batch of {len(df_batch)} rows ===========")

            yield df_batch

            offset += batch_size

        self.cursor.close()
        self.connection.close()

    def get_db_watermark(self):
        # Implement logic to get the latest watermark
        pass

    def update_db_watermark(self, new_watermark):
        # Implement logic to update the watermark
        pass
