import pytest
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np
from vector_etl.target_mods.pinecone import PineconeTarget
from vector_etl.target_mods.qdrant import QdrantTarget

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'df_uuid': ['uuid1', 'uuid2'],
        'embeddings': [np.random.rand(1536), np.random.rand(1536)],
        'text': ['This is a test sentence.', 'Another test sentence.'],
        'metadata': [{'key1': 'value1'}, {'key2': 'value2'}]
    })

@pytest.fixture
def pinecone_config():
    return {
        'pinecone_api_key': 'test_api_key',
        'index_name': 'test_index'
    }

@pytest.fixture
def qdrant_config():
    return {
        'qdrant_url': 'http://localhost:6333',
        'qdrant_api_key': 'test_api_key',
        'collection_name': 'test_collection'
    }

def test_pinecone_target_connect(pinecone_config):
    with patch('pinecone.Pinecone') as mock_pinecone:
        mock_index = Mock()
        mock_pinecone.return_value.Index.return_value = mock_index

        target = PineconeTarget(pinecone_config)
        target.connect()

        mock_pinecone.assert_called_once_with(api_key='test_api_key')
        mock_pinecone.return_value.Index.assert_called_once_with('test_index')

def test_pinecone_target_write_data(sample_df, pinecone_config):
    with patch('pinecone.Pinecone') as mock_pinecone:
        mock_index = Mock()
        mock_pinecone.return_value.Index.return_value = mock_index

        target = PineconeTarget(pinecone_config)
        target.connect()
        target.write_data(sample_df, 'test_domain')

        mock_index.upsert.assert_called_once()
        call_args = mock_index.upsert.call_args[1]['vectors']
        assert len(call_args) == len(sample_df)
        assert all('id' in item and 'values' in item and 'metadata' in item for item in call_args)

def test_qdrant_target_connect(qdrant_config):
    with patch('qdrant_client.QdrantClient') as mock_qdrant:
        target = QdrantTarget(qdrant_config)
        target.connect()

        mock_qdrant.assert_called_once_with(url='http://localhost:6333', api_key='test_api_key')

def test_qdrant_target_write_data(sample_df, qdrant_config):
    with patch('qdrant_client.QdrantClient') as mock_qdrant:
        mock_client = Mock()
        mock_qdrant.return_value = mock_client

        target = QdrantTarget(qdrant_config)
        target.connect()
        target.write_data(sample_df, 'test_domain')

        mock_client.upsert.assert_called_once()
        call_args = mock_client.upsert.call_args[1]
        assert call_args['collection_name'] == 'test_collection'
        assert len(call_args['points']) == len(sample_df)
