import pytest
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np
from vector_etl.embedding_mods.openai import OpenAIEmbedding
from vector_etl.embedding_mods.cohere import CohereEmbedding

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        '__concat_final': ['This is a test sentence.', 'Another test sentence.'],
        'other_column': [1, 2]
    })

@pytest.fixture
def openai_config():
    return {
        'api_key': 'test_api_key',
        'model_name': 'text-embedding-ada-002'
    }

@pytest.fixture
def cohere_config():
    return {
        'api_key': 'test_api_key',
        'model_name': 'embed-english-v2.0'
    }

def test_openai_embedding(sample_df, openai_config):
    with patch('openai.OpenAI') as mock_openai:
        mock_client = Mock()
        mock_openai.return_value = mock_client
        mock_client.embeddings.create.return_value.data = [
            Mock(embedding=np.random.rand(1536)) for _ in range(len(sample_df))
        ]

        embedding = OpenAIEmbedding(openai_config)
        result_df = embedding.embed(sample_df)

        assert 'embeddings' in result_df.columns
        assert len(result_df) == len(sample_df)
        assert all(isinstance(emb, np.ndarray) for emb in result_df['embeddings'])
        assert all(len(emb) == 1536 for emb in result_df['embeddings'])

def test_cohere_embedding(sample_df, cohere_config):
    with patch('cohere.Client') as mock_client:
        mock_client.return_value.embed.return_value.embeddings = [
            np.random.rand(4096) for _ in range(len(sample_df))
        ]

        embedding = CohereEmbedding(cohere_config)
        result_df = embedding.embed(sample_df)

        assert 'embeddings' in result_df.columns
        assert len(result_df) == len(sample_df)
        assert all(isinstance(emb, np.ndarray) for emb in result_df['embeddings'])
        assert all(len(emb) == 4096 for emb in result_df['embeddings'])

