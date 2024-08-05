import pytest
from unittest.mock import Mock, patch
import pandas as pd
from io import BytesIO
from vector_etl.source_mods.s3_loader import S3Source
from vector_etl.source_mods.database_loader import DatabaseSource
from vector_etl.source_mods.local_file import LocalFileSource

@pytest.fixture
def s3_config():
    return {
        'aws_access_key_id': 'test_key',
        'aws_secret_access_key': 'test_secret',
        'bucket_name': 'test_bucket',
        'prefix': 'test_prefix/',
        'chunk_size': 1000,
        'chunk_overlap': 200
    }

@pytest.fixture
def db_config():
    return {
        'db_type': 'postgres',
        'host': 'localhost',
        'database_name': 'test_db',
        'username': 'test_user',
        'password': 'test_pass',
        'port': 5432,
        'query': 'SELECT * FROM test_table',
        'chunk_size': 1000,
        'chunk_overlap': 200
    }

@pytest.fixture
def local_file_config():
    return {
        'file_path': '/path/to/test_file.csv',
        'chunk_size': 1000,
        'chunk_overlap': 200
    }

def test_s3_source_connect(s3_config):
    with patch('boto3.client') as mock_client:
        source = S3Source(s3_config)
        source.connect()
        mock_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret'
        )

def test_s3_source_list_files(s3_config):
    with patch('boto3.client') as mock_client:
        mock_paginator = Mock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'test_prefix/file1.csv'}, {'Key': 'test_prefix/file2.csv'}]}
        ]
        mock_client.return_value.get_paginator.return_value = mock_paginator

        source = S3Source(s3_config)
        source.connect()
        files = source.list_files()

        assert files == ['test_prefix/file1.csv', 'test_prefix/file2.csv']

def test_database_source_connect(db_config):
    with patch('psycopg2.connect') as mock_connect:
        source = DatabaseSource(db_config)
        source.connect()
        mock_connect.assert_called_once_with(
            host='localhost',
            database='test_db',
            user='test_user',
            password='test_pass',
            port=5432
        )

def test_database_source_fetch_data(db_config):
    with patch('psycopg2.connect') as mock_connect:
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]
        mock_cursor.description = [('id',), ('name',)]

        source = DatabaseSource(db_config)
        df, _ = source.fetch_data()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ['id', 'name']

def test_local_file_source_connect(local_file_config):
    with patch('os.path.exists', return_value=True):
        source = LocalFileSource(local_file_config)
        source.connect()

def test_local_file_source_read_file(local_file_config):
    with patch('builtins.open', return_value=BytesIO(b'id,name\n1,test\n2,test2')):
        source = LocalFileSource(local_file_config)
        file_content = source.read_file('/path/to/test_file.csv')
        assert isinstance(file_content, BytesIO)

