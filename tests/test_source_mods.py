import pytest
from unittest.mock import Mock, patch
import pandas as pd
from io import BytesIO
from vector_etl.source_mods.s3_loader import S3Source
from vector_etl.source_mods.database_loader import DatabaseSource
from vector_etl.source_mods.local_file import LocalFileSource
from vector_etl.source_mods.google_bigquery import GoogleBigQuerySource
from vector_etl.source_mods.airtable_loader import AirTableSource
from vector_etl.source_mods.hubspot_loader import HubSpotSource
from vector_etl.source_mods.intercom_loader import InterComSource
from vector_etl.source_mods.paystack_loader import PayStackSource
from vector_etl.source_mods.zoho_crm_loader import ZohoCrmSource
from vector_etl.source_mods.zoho_desk_loader import ZohoDeskSource
from vector_etl.source_mods.flutterwave_loader import FlutterWaveSource
from vector_etl.source_mods.gmail_loader import GmailSource

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
def google_bigquery_config():
    return {
    "source_data_type": "Google BigQuery",
    "google_application_credentials": "",
     "query": "SELECT * FROM chipotle_stores LIMIT 10"
        
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
def gmail_config():
    return {
        'credentials': 'credentials.json', ## path to gmail crendtials
        'gmail.label': 'IMPORTANT'  # Specify the label in the config
    }
    
    
    
    
@pytest.fixture
def zohodesk_config():
    return{
            "grant_type":"",
            "client_id": "",
            "client_secret": "",
            "code": "",
            "limit":"",
            "records":"desk.team",
            "accounts_url":""
        }


@pytest.fixture
def zohocrm_config():
    return{
            "grant_type":"",
            "client_id": "",
            "client_secret": "",
            "code": "",
            "per_page":"10",
            "records":"module.Call",
            "accounts_url":""
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
def paystack_config():
    return{
            "paystack_secret_key":"",
            "records": "paystack.transactions",
        }
    


@pytest.fixture
def flutterwave_config():
    return{
            "secret_key":"",
            "records": "flutterwave.payout-subaccounts",
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
        
  
def test_google_bigquery_connect(google_bigquery_config):
    with patch('bigquery.connect') as  mock_connect:
        source = GoogleBigQuerySource(google_bigquery_config)
        source.connect()
        mock_connect.assert_called_once_with(
            source_data_type="Google BigQuery",
    google_application_credentials="",
     query="SELECT * FROM chipotle_stores LIMIT 10"
        )
        

def test_google_bigquery_fetch_data(google_bigquery_config):
      with patch('bigquery.connect') as  mock_connect:
          mock_connect.result.to_dataframe.return_value = pd.DataFrame()
          source =  GoogleBigQuerySource(google_bigquery_config)
          df = source.fetch_data()
          assert isinstance(df, pd.DataFrame)


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
          
          
def test_zohodesk_connect(zohodesk_config):
    
    with patch('requests.get') as  mock_connect:
        source = ZohoDeskSource(zohodesk_config)
        source.connect()
        mock_connect.assert_called_once_with(
        grant_type="",
        client_id = "",
        client_secret="",
        code="",
        accounts_url=""
        ) 


def test_zohodesk_fetch_data(zohodesk_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [ {
            "Address": "333 Post St",
            "Name": "Union Square",
            "Visited": True
        }
        ]
          
          source =  ZohoDeskSource(zohodesk_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          
def test_zohocrm_connect(zohocrm_config):
    
    with patch('requests.get') as  mock_connect:
        source = ZohoCrmSource(zohocrm_config)
        source.connect()
        mock_connect.assert_called_once_with(
        grant_type="",
        client_id = "",
        client_secret="",
        code="",
        accounts_url=""
        ) 


def test_zohocrm_fetch_data(zohocrm_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [ {  }
        ]
          
          source =  ZohoCrmSource(zohocrm_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)

def test_paystack_connect(paystack_config):
    
    with patch('requests.get') as  mock_connect:
        source = PayStackSource(paystack_config)
        source.connect()
        mock_connect.assert_called_once_with(
        paystack_secret_key="",
        ) 


def test_paystack_fetch_data(paystack_config):
      with patch('Paystack') as  mock_connect:
          mock_connect.return_value = [{}]
          
          source =  PayStackSource(paystack_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          

def test_intercom_connect(intercom_config):
    
    with patch('requests.get') as  mock_connect:
        source = InterComSource(intercom_config)
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
          
          

def test_flutterwave_connect(flutterwave_config):
    
    with patch('requests.get') as  mock_connect:
        source =  FlutterWaveSource(flutterwave_config)
        source.connect()
        mock_connect.assert_called_once_with(
        secret_key="",
        )    


def test_flutterwave_fetch_data(flutterwave_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [{}]
          
          source =  FlutterWaveSource(flutterwave_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)
          



def test_gmail_connect(gmail_config):
    
    with patch('InstalledAppFlow.from_client_secrets_file') as  mock_connect:
        source =  GmailSource(gmail_config)
        source.connect()
        mock_connect.assert_called_once_with(
        credentials="credential.json",
        )    


def test_gmail_fetch_data(gmail_config):
      with patch('requests.get') as  mock_connect:
          mock_connect.return_value = [{}]
          source =  GmailSource(gmail_config)
          df = source.fetch_data()

          assert isinstance(df, pd.DataFrame)



