import os 
from google.cloud import bigquery
from .base import BaseSource
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleBigQuerySource(BaseSource):
    def __init__(self,config):
         self.config = config
         self.client = None
         self.connect()
         
            
    def connect(self):
        if self.config["db_type"] == 'google_bigquery':
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config['GOOGLE_APPLICATION_CREDENTIALS']
            self.client = bigquery.Client()
     
    def fetch_data(self):
        if self.client:
            try:
                query_job = self.client.query(f"""{self.config.get("query"," ")}""") 
                if query_job:
                    dfrows = query_job.result().to_dataframe() 
                    return dfrows
                else:
                      logger.error(f"No data returned: {e}")
                      return None
            except Exception as e:
                 logger.error(f"An error occurred: {e}")
                 return None
                
                
 
config = {"query":"SELECT * FROM bigquery-public-data.america_health_rankings.ahr LIMIT 100",
          "GOOGLE_APPLICATION_CREDENTIALS":"contextData_bigquery_cred.json",
          "db_type":'google_bigquery'
          }




