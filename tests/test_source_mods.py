import pytest
from unittest.mock import Mock, patch
import pandas as pd
from io import BytesIO
from vector_etl.source_mods.s3_loader import S3Source
from vector_etl.source_mods.database_loader import DatabaseSource
from vector_etl.source_mods.local_file import LocalFileSource
from vector_etl.source_mods.airtable_loader import AirTableSource
from vector_etl.source_mods.hubspot_loader import HubSpotSource
from vector_etl.source_mods.intercom_loader import InterComSource
from vector_etl.source_mods.digital_ocean_spaces_loader import DigitalOceanSpaceSource

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
def digital_ocean_config():
    return {
        'aws_access_key_id': 'test_key',
        'endpoint_url':'text_endpoint',
        'region_name':'text_region',
        'aws_secret_access_key': 'test_secret',
        'bucket_name': 'test_bucket',
        'prefix': 'test_prefix/',
        'chunk_size': 1000,
        'chunk_overlap': 200
    }
    



@pytest.fixture
def airtable_config():
    return {
        "url":"airttable.com/sales",
        "baseId":"sales",
        "auth_token":"673989fhuhefiw0903",
        "tableIdOrName":"survey" 
    }
    


    

@pytest.fixture
def hubspot_config():
    return{
            "archive":"",
            "limit": "",
            "access_token": "",
            "crm_object":"crm_object",
        }
    

    
@pytest.fixture
def intercom_config():
    return{
            "token":"",
            "records": "intercom.teams",
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
        
  


def test_airtable_connect(airtable_config):
    
    with patch('requests.get') as  mock_connect:
        source =  AirTableSource(airtable_config)
        source.connect()
        mock_connect.assert_called_once_with(
        url="Apache Cassandra",
        baseId="cassandra_astra",
        tableIdOrName= "",
        auth_token="secure-connect-contextdata.zip",
       
        )
        

def test_airtable_fetch_data(airtable_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [ {
            "Address": "333 Post St",
            "Name": "Union Square",
            "Visited": True
        }
        ]
          
          source =  AirTableSource(airtable_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          

          

def test_intercom_connect(intercom_config):
    
    with patch('requests.get') as  mock_connect:
        source = InterComSource(intercom_config)
        source.connect()
        mock_connect.assert_called_once_with(
        secret_key="",
        )    


def test_hubspot_fetch_data(hubspot_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [{}]
          
          source =  HubSpotSource(hubspot_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          



def test_hubspot_connect(hubspot_config):
    
    with patch('requests.get') as  mock_connect:
        source = HubSpotSource(hubspot_config)
        source.connect()
        mock_connect.assert_called_once_with(
        secret_key="",
        )    


def test_intercom_fetch_data(intercom_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [{}]
          
          source =  InterComSource(intercom_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          
          


def test_digital_ocean_source_connect(s3_config):
    with patch('boto3.client') as mock_client:
        source = DigitalOceanSpaceSource(s3_config)
        source.connect()
        mock_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            endpoint_url='text_endpoint',
            region_name='text_region',
        )

def test_s3_digital_ocean_list_files(s3_config):
    with patch('boto3.client') as mock_client:
        mock_paginator = Mock()
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': 'test_prefix/file1.csv'}, {'Key': 'test_prefix/file2.csv'}]}
        ]
        mock_client.return_value.get_paginator.return_value = mock_paginator

        source = DigitalOceanSpaceSource(s3_config)
        source.connect()
        files = source.list_files()

        assert files == ['test_prefix/file1.csv', 'test_prefix/file2.csv']
          
          
          




