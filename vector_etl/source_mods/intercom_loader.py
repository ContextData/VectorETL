from base import BaseSource
import requests
from pprint import pprint
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InterComSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.url = None
        self.token = self.config['token']
        
        
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
        headers = {"Authorization": f"Bearer {self.token}",
           "Content-type":"application/json",
           "Intercom-Version":"2.11"}
        response = requests.get(url=url,headers=headers)
        
        return response
        
        
    def fetch_data(self):
        
        if self.config['records'] == 'intercom.articles':
            logger.info(" articles \n")
            self.url = f"https://api.intercom.io/articles"
            
            response = self.connect(self.url).json()['data']
        
           
        if self.config['records'] == 'intercom.companies.scroll':
            logger.info(" Companies \n")
            self.url = f"https://api.intercom.io/companies/scroll"
            
            response = self.connect(self.url).json()['data']
            
        
        if self.config['records'] == 'intercom.contacts':
            logger.info(" Contacts \n")
            self.url = f"https://api.intercom.io/contacts"
            
            response = self.connect(self.url).json()['data']
            
            
            
        if self.config['records'] == 'intercom.conversations':
            logger.info(" conversations \n")
            self.url = f"https://api.intercom.io/conversations"
            
            response = self.connect(self.url).json()['conversations']
            
            
            
        if self.config['records'] == 'intercom.collections':
            logger.info(" collection \n")
            self.url = f"https://api.intercom.io/help_center/collections"
            
            response = self.connect(self.url).json()['data']
            
            
        if self.config['records'] == 'intercom.news_items':
            logger.info(" news items \n")
            self.url = f"https://api.intercom.io/news/news_items"
            
            response = self.connect(self.url).json()['data']
            
        
        if self.config['records'] == 'intercom.segments':
            logger.info(" segments \n")
            self.url = f"https://api.intercom.io/segments"
            
            response = self.connect(self.url).json()['segments']
            
        
         
        if self.config['records'] == 'intercom.subscription_types':
            logger.info(" subscription_types \n")
            self.url = f"https://api.intercom.io/subscription_types"
            
            response = self.connect(self.url).json()['data']
            
            
        if self.config['records'] == 'intercom.teams':
            logger.info(" Teams \n")
            self.url = f"https://api.intercom.io/teams"
            
            response = self.connect(self.url).json()['teams']
            
        if self.config['records'] == 'intercom.ticket_types':
            logger.info(" ticket_types \n")
            self.url = f"https://api.intercom.io/ticket_types"
            
            response = self.connect(self.url).json()['data']
            
        
        try:    
            flattened_data = [self.flatten_dict(item) for item in response]
            pprint(flattened_data,indent=4)
              
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")   
            



