from .base import BaseSource
import requests
from pprint import pprint
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FlutterWaveSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.url = None
        self.secret_key = self.config['secret_key']
        
        
    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)
        
        
    def connect(self,url):
        headers = {"Authorization": f"Bearer {self.secret_key}",
           "Content-type":"application/json",
           "Intercom-Version":"2.11"}
        response = requests.get(url=url,headers=headers)
        
        return response
        
        
    def fetch_data(self):
        
        if self.config['records'] == 'flutterwave.transfers':
            logger.info(" Transfers \n")
            self.url = f"https://api.flutterwave.com/v3/transfers"
            
            response = self.connect(self.url).json()['data']
        
        elif self.config['records'] == 'flutterwave.transactions':
            logger.info(" transactions \n")
            self.url = f"https://api.flutterwave.com/v3/transactions"
            
            response = self.connect(self.url).json()['data']
            
        
        elif self.config['records'] == 'flutterwave.beneficiaries':
            logger.info(" Transfers \n")
            self.url = f"https://api.flutterwave.com/v3/beneficiaries"
            
            response = self.connect(self.url).json()['data']
            
        
        elif self.config['records'] == 'flutterwave.subaccounts':
            logger.info(" subaccounts \n")
            self.url = f"https://api.flutterwave.com/v3/subaccounts"
            
            response = self.connect(self.url).json()['data']
            
        elif self.config['records'] == 'flutterwave.payout-subaccounts':
            logger.info(" payout-subaccounts \n")
            self.url = f"https://api.flutterwave.com/v3/payout-subaccounts"
            
            response = self.connect(self.url).json()['data']
            
        elif self.config['records'] == 'flutterwave.subscriptions':
            logger.info(" subscriptions \n")
            self.url = f"https://api.flutterwave.com/v3/subscriptions"
            
            response = self.connect(self.url).json()['data']
            
        
        elif self.config['records'] == 'flutterwave.payment-plans':
            logger.info(" payment-plans \n")
            self.url = f"https://api.flutterwave.com/v3/payment-plans"
            
            response = self.connect(self.url).json()['data']
        
        
      
            
        
        try:    
            flattened_data = [self.flatten_dict(item) for item in response]
            pprint(flattened_data,indent=4)
              
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")   
            
            
            






