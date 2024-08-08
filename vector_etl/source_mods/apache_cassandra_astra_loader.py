import pandas as pd
from cassandra.cluster import Cluster
import logging
from base import BaseSource
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ApacheCassandraAstraSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.cluster = None
        self.auth_provider = None
        self.session = None
        self.cloud_config = None
        self.keyspace = self.config['keyspace']
        self.connect()  # Initialize connection here
  
    def connect(self):
        if self.config["db_type"] == 'cassandra_astra':
            self.cloud_config = {'secure_connect_bundle': self.config['secure_connect_bundle']}
            self.auth_provider = PlainTextAuthProvider(self.config['clientId'], self.config['secret'])
            self.cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider,protocol_version=3)
            self.session = self.cluster.connect(self.keyspace)
        else:
            raise ValueError("Invalid database type")

    def fetch_data(self):
        if not self.session:
            raise Exception("Session is not initialized. Ensure you call connect() first.")
            
        query = self.config.get("query", "")
        prepared_statement = self.session.prepare(query)
        
        try:
            db_data = self.session.execute(prepared_statement)
            if db_data:
                # Convert to Pandas DataFrame
                df = pd.DataFrame(list(db_data))
                return df
            else:
                logger.error(f"No data returned: {e}")
                return None
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None

