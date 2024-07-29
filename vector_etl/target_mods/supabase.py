import logging
import pandas as pd
import vecs
from urllib.parse import urlparse
import psycopg2
from .base import BaseTarget

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabaseTarget(BaseTarget):
    def __init__(self, config):
        self.config = config
        self.vx = None
        self.collection = None

    def connect(self):
        logger.info("Connecting to Supabase...")
        self.vx = vecs.create_client(self.config["supabase_uri"])
        logger.info("Connected to Supabase successfully.")

    def create_index_if_not_exists(self, dimension):
        if self.vx is None:
            self.connect()

        index_name = self.config["index_name"]
        try:
            self.collection = self.vx.get_collection(name=index_name)
        except Exception:
            logger.info(f"Creating Supabase collection: {index_name}")
            self.collection = self.vx.create_collection(
                name=index_name,
                dimension=dimension#self.config.get("dimension", 1536)
            )

    def update_vector_size(self, table_name, vector_size):
        pg_uri = urlparse(self.config["supabase_uri"])
        connection = psycopg2.connect(
            database=pg_uri.path[1:],
            user=pg_uri.username,
            password=pg_uri.password,
            host=pg_uri.hostname,
            port=pg_uri.port
        )

        sql = f"""
        ALTER TABLE vecs.{table_name}
        ALTER COLUMN vec TYPE vector({vector_size});
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
        connection.close()

    def write_data(self, df, columns, domain=None):
        logger.info("Writing embeddings to Supabase...")
        if self.vx is None:
            self.create_index_if_not_exists(len(df['embeddings'].iat[0]))

        index_name = self.config["index_name"]
        self.update_vector_size(index_name, len(df['embeddings'].iat[0]))
        docs = self.vx.get_collection(name=index_name)

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

        docs.upsert(records=data)
        logger.info("Completed writing embeddings to Supabase.")
