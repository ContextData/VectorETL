from base import BaseSource
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from pprint import pprint



class MailChimpMarketingSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.api_key = self.config['api_key']
        self.server_prefix = self.config['server_prefix']
        
    
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
        
        
    def connect(self):
        try:
            client =  MailchimpMarketing.Client()
            client.set_config({
            "api_key":self.api_key,
            "server": self.server_prefix
            })
            return client
        except ApiClientError as error:
            print("Error: {}".format(error.text))
        
        
        
    
    
    def fetch_data(self):
        client = self.connect()
        if self.config['records'] == "campaign":
           response = client.campaigns.list()['campaigns']
        
        elif self.config['records'] == "campaignFolders":
           response = client.campaignFolders.list()['folders'] 
           
        
        elif self.config['records'] == "ConnectedSites":
           response = client.connectedSites.list()['sites'] 
           
        
        elif self.config['records'] == "conversations":
           response = client.ecommerce.stores()['conversations'] 
           
        
        elif self.config['records'] == "ecommerce":
           response = client.conversations.list()['stores'] 
        
        elif self.config['records'] == "facebookAds":
           response = client.facebookAds.list()['facebook_ads']
           
        elif self.config['records'] == "landingpages":
           response = client.landingPages.get_all()['landing_pages'] 
           
        
        elif self.config['records'] == "reports":
           response = client.reports.get_all_campaign_reports()['reports'] 
           
        
    
        try:    
            flattened_data = [self.flatten_dict(item) for item in response]
            pprint(flattened_data,indent=4)
              
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
        except ApiClientError as error:
            logger.error(f"HTTP error occurred: {error.text}")   
            
        





