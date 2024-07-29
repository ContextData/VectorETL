import pandas as pd
import logging
from .base import BaseEmbedding
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OpenAIEmbedding(BaseEmbedding):
    def __init__(self, config):
        self.config = config
        self.client = OpenAI(api_key=config['api_key'])

    def embed(self, df, embed_column='__concat_final'):
        logger.info("Starting the OpenAI embedding process...")

        text_data = df[embed_column].str.strip().tolist()
        default_value = 'default'
        cleaned_data = [x if x else default_value for x in text_data]

        batch_size = 100
        embeddings = []

        for i in range(0, len(cleaned_data), batch_size):
            batch_data = cleaned_data[i:i + batch_size]

            response = self.client.embeddings.create(
                input=batch_data,
                model=self.config['model_name'],
                encoding_format="float"
            )

            batch_embeddings = [item.embedding for item in response.data]
            embeddings.extend(batch_embeddings)

        df['embeddings'] = embeddings

        logger.info("Completed OpenAI embedding process.")
        return df
