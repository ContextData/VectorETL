import cohere
import pandas as pd
import logging
from .base import BaseEmbedding

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CohereEmbedding(BaseEmbedding):
    def __init__(self, config):
        self.config = config
        self.client = cohere.Client(config['api_key'])

    def embed(self, df, embed_column='__concat_final'):
        logger.info("Starting the Cohere embedding process...")

        text_data = df[embed_column].str.strip().tolist()

        response = self.client.embed(
            texts=text_data,
            model=self.config['model_name'],
            input_type="classification"
        )

        df['embeddings'] = response.embeddings

        logger.info("Completed Cohere embedding process.")
        return df
