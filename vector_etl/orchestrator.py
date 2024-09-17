import logging
import pandas as pd
import uuid
import requests
from vector_etl.source_mods import get_source_class
from vector_etl.embedding_mods import get_embedding_model
from vector_etl.target_mods import get_target_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLOrchestrator:
    def __init__(self, source_config, embedding_config, target_config, embed_columns):
        self.source = get_source_class(source_config)
        self.embedding = get_embedding_model(embedding_config)
        self.target = get_target_database(target_config)
        self.source_config = source_config
        self.embedding_config = embedding_config
        self.target_config = target_config
        self.embed_columns = embed_columns

    def run(self):
        logger.info("Starting ETL process...")

        try:

            if self.source_config['source_data_type'] == 'database':

                # Fetch data from source in batches
                for df_batch in self.fetch_data():
                    if df_batch.empty: #len(df_batch) == 0: #df_batch.empty:
                        logger.info("No new data to process in this batch. Continuing...")
                        continue

                    # Process and embed data
                    df_batch = self.process_and_embed_data(df_batch)

                    # Write data to target
                    self.write_to_target(df_batch)

            else:
                df = self.fetch_data()
                if df.empty:
                    logger.info("No new data to process. Exiting.")
                    return

                df = self.process_and_embed_data(df)
                self.write_to_target(df)

            logger.info("ETL process completed successfully.")

        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

    def fetch_data(self):
        logger.info(f"Fetching data from {self.source_config['source_data_type']}...")
        return self.source.fetch_data()

    def process_and_embed_data(self, df):
        logger.info("Processing and embedding data...")

        # Generate concatenated column
        df = self.generate_concatenation(df)

        # Split data into chunks if necessary
        chunk_size = self.source_config.get('chunk_size', 1000)
        chunk_overlap = self.source_config.get('chunk_overlap', 0)
        df = self.split_dataframe_column(df, chunk_size, chunk_overlap)

        # Generate embeddings
        df = self.embedding.embed(df)

        return df

    def write_to_target(self, df):
        logger.info(f"Writing data to {self.target_config['target_database']}...")
        domain = self.source_config.get('table', self.source_config.get('source_data_type'))

        self.target.write_data(df, self.embed_columns, domain)

    def generate_concatenation(self, df):
        def format_column_name(name):
            return ''.join(word.capitalize() for word in name.split('_'))

        if len(self.embed_columns) > 0:
            df['__concat_final'] = df.apply(
                lambda row: ' ~ '.join(f"{format_column_name(col)}: {row[col]}"
                                       for col in self.embed_columns if pd.notna(row[col])),
                                       axis=1
            )
        else:
            df_columns = df.columns.to_list()
            df['__concat_final'] = df.apply(
                lambda row: ' ~ '.join(f"{format_column_name(col)}: {row[col]}"
                                       for col in df_columns if pd.notna(row[col])),
                axis=1
            )

        df['df_uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
        return df

    def split_dataframe_column(self, df, chunk_size, chunk_overlap):
        logger.info("Splitting dataframe into chunks...")

        def split_text(text, size, overlap):
            if not isinstance(text, str):
                return []
            return [text[i:i + size] for i in range(0, len(text), size - overlap) if text[i:i + size]]

        new_rows = []
        for _, row in df.iterrows():
            chunks = split_text(row['__concat_final'], chunk_size, chunk_overlap)
            for chunk in chunks:
                if chunk:
                    new_row = row.copy()
                    new_row['__concat_final'] = chunk
                    new_rows.append(new_row)

        return pd.DataFrame(new_rows, columns=df.columns)


def run_etl_process(source_config, embedding_config, target_config, embed_columns):
    orchestrator = ETLOrchestrator(source_config, embedding_config, target_config, embed_columns)
    orchestrator.run()

def run_etl_process_py(config):
    # Replace the config loading from file with the direct config dict
    source_config = config['source']
    embedding_config = config['embedding']
    target_config = config['target']
    embed_columns = config.get('embed_columns', [])

    orchestrator = ETLOrchestrator(source_config,
                                   embedding_config,
                                   target_config,
                                   embed_columns)
    orchestrator.run()
