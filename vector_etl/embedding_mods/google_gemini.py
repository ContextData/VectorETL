import google.generativeai as genai
import pandas as pd
import logging
from .base import BaseEmbedding

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleGeminiEmbedding(BaseEmbedding):
    def __init__(self, config):
        self.config = config
        genai.configure(api_key=config['api_key'])

    def embed(self, df, embed_column='__concat_final'):
        logger.info("Starting the Google Gemini embedding process...")

        text_data = df[embed_column].str.strip().tolist()

        response = genai.embed_content(
            content=text_data,
            model='models/' + self.config['model_name']
        )

        embeddings = [item for item in response['embedding']]
        df['embeddings'] = embeddings

        logger.info("Completed Google Gemini embedding process.")
        return df
