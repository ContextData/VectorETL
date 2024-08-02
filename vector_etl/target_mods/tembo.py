import logging
import pandas as pd
from urllib.parse import urlparse
import psycopg2
import json
from psycopg2.extras import execute_values
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TemboTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        logger.info("Connecting to Tembo database...")
        self.connection = psycopg2.connect(
            host=self.config["host"],
            database=self.config["database_name"],
            user=self.config["username"],
            password=self.config["password"],
            port=self.config["port"]
        )

    def create_index_if_not_exists(self, dimension):
        if self.connection is None:
            self.connect()

        schema_name = self.config["schema_name"]
        table_name = self.config["table_name"]

        table_list_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        cursor = self.connection.cursor()
        cursor.execute(table_list_query)
        tables = cursor.fetchall()
        tables_flat = [item for sublist in tables for item in sublist]

        if table_name not in tables_flat:
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                                    id UUID PRIMARY KEY, 
                                    embedding vector({dimension}),
                                    metadata jsonb);
                                    """
            with self.connection.cursor() as cursor:
                cursor.execute(create_schema_query)
                cursor.execute(create_table_query)
            self.connection.commit()

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Tembo...")
        if self.connection is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))

        schema_name = self.config["schema_name"]
        table_name = self.config["table_name"]

        data = []
        for _, row in df.iterrows():
            if len(columns) > 0:
                columns.append("__concat_final")
                metadata = {col: str(row[col]) for col in columns}

                if domain:
                    metadata["domain"] = domain

                data.append((str(row["df_uuid"]), row["embeddings"], metadata))
            else:
                metadata = {
                    col: str(row[col]) if isinstance(row[col], list) else str(row[col])
                    for col in df.columns if
                    col not in ["df_uuid", "embeddings"] and pd.notna(row[col])
                }
                if domain:
                    metadata["domain"] = domain

                data.append((str(row["df_uuid"]), row["embeddings"], metadata))

        insert_data = [(id, embedding, json.dumps(metadata)) for id, embedding, metadata in data]

        insert_query = f"""INSERT INTO {schema_name}.{table_name} (id, embedding, metadata)
                           VALUES %s
                            """

        with self.connection.cursor() as cur:
            execute_values(cur, insert_query, insert_data)
            self.connection.commit()

        self.connection.close()
        logger.info("Completed writing embeddings to Tembo.")
