from .base import BaseSource
import pandas as pd
import requests
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HubSpotSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.endpoints = None
        self.access_token = self.config["access_token"]
    
    def connect(self,url):
        headers = {
            "authorization":f"Bearer {self.access_token}"
        }
        try:
            response = requests.get(url=url,headers=headers)
            logger.info(f"Status {response.status_code}")
            return response.json()
        except Exception as e:
              logger.error(f"An error occurred: {e}")
        
    def fetch_data(self):
       
       if self.config['crm_object'] == "crm.companies":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/companies?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Companies \n")
           
       elif  self.config['crm_object'] == "crm.contacts":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/contacts?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Contacts \n")
           
       elif  self.config['crm_object'] == "crm.tickets":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/tickets?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Tickets \n")
           
       elif  self.config['crm_object'] == "crm.deals":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/deals?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Deals \n")
           
       elif  self.config['crm_object'] == "crm_object":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/products?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"products \n")
           
         
       elif  self.config['crm_object'] == "crm.invoices":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/invoices?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"invoices \n")
           
       elif  self.config['crm_object'] == "crm.carts":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/carts?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Carts \n")
    
       elif  self.config['crm_object'] == "crm.tasks":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/tasks?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Tasks \n")
           
       elif  self.config['crm_object'] == "crm.payments":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/commerce_payments?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Payments \n")
           
       elif  self.config['crm_object'] == "crm.orders":
           self.endpoints = f"https://api.hubapi.com/crm/v3/objects/orders?limit={self.config['limit']}&archived={self.config['archive']}"
           logger.info(f"Orders \n")
                  
       else:
            raise ValueError(f"Unsupported Crm object type: check the object name {self.config['crm_object']}")
        
        
       response = self.connect(self.endpoints)
       
       if 'results' in response:
            print(response)
            results = [results['properties'] for results in response['results']]    
            df  = pd.DataFrame(results)
            logger.info(f" data \n {df}")
            return df
       else:
          raise ValueError(response['message'])
        
        
    
    

    
    
    
    
    
    
    
    
 
   
    
   
    

    
    
 
        
        
        
        

        
        
        




        
        









