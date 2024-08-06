import requests
import pandas as pd
import logging
from .base import BaseEmbedding

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HuggingFaceEmbedding(BaseEmbedding):
    def __init__(self, config):
        self.config = config
        self.api_url = "https://api-inference.huggingface.co/pipeline/feature-extraction/" + config['model_name']
        self.headers = {"Authorization": f"Bearer {config['api_key']}"}

    def embed(self, df, embed_column='__concat_final'):
        logger.info("Starting the Hugging Face embedding process...")

        text_data = df[embed_column].str.strip().tolist()
        embeddings = []

        for text in text_data:
            response = requests.post(self.api_url, headers=self.headers, json={"inputs": text})
            if response.status_code == 200:
                embeddings.append(response.json())
            else:
                logger.error(f"Error in Hugging Face API call: {response.text}")
                embeddings.append(None)

        df['embeddings'] = embeddings

        logger.info("Completed Hugging Face embedding process.")
        return df
