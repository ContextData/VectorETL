import requests
from .base import BaseSource
<<<<<<< HEAD
=======
from pprint import pprint
>>>>>>> f842a37 (updated base module  import)
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirTableSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.url = f"https://api.airtable.com/v0/{self.config['baseId']}/{self.config['tableIdOrName']}"
        self.auth_token = config['auth_token']
          
    def connect(self):
        headers = {
            "Authorization": f"Bearer {self.auth_token}"
        }
        try:
            response = requests.get(self.url,headers=headers)
            data = response.json()['records']
            return data
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None
    
    def fetch_data(self):
        records = self.connect()
        df_data = [data['fields'] for data in records ]
        airtable_df = pd.DataFrame(df_data)
        
        return airtable_df




