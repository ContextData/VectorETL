from .base import BaseSource
import logging
import pandas as pd
from paystackapi.paystack import Paystack


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PayStackSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.paystack_secret_key = self.config['paystack_secret_key']
        
        
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
        
        paystack = Paystack(secret_key=self.paystack_secret_key)
      
        return paystack
        
        
    def fetch_data(self):
        
        if self.config['records'] == 'paystack.transactions':
            logger.info(" Transactions \n")
            response = self.connect().transaction.list()['data']
            
        elif self.config['records'] == 'paystack.transactions.split':
            logger.info(" Transactions split \n")
            response = self.connect().transactionSplit.list()['data']
            
        elif self.config['records'] == 'paystack.invoice':
            logger.info(" invoice \n")
            response = self.connect().invoice.list()['data']
        
        elif self.config['records'] == 'paystack.product':
            logger.info(" product  \n")
            response = self.connect().product.list()['data']
        
        elif self.config['records'] == 'paystack.customer':
            logger.info(" customer \n")
            response = self.connect().customer.list()['data']
        
        elif self.config['records'] == 'paystack.plan':
            logger.info(" plan \n")
            response = self.connect().plan.list()['data']
        
        elif self.config['records'] == 'paystack.subaccount':
            logger.info(" subaccount \n")
            response = self.connect().subaccount().list()['data']
        
            
        elif self.config['records'] == 'paystack.subscription':
            logger.info(" subaccount \n")
            response = self.connect().subscription.list()['data']
            
        
        elif self.config['records'] == 'paystack.transfer':
            logger.info(" transfer \n")
            response = self.connect().transfer.list()['data']
        
        
        elif self.config['records'] == 'paystack.bulkcharge':
            logger.info(" bulkcharge \n")
            response = self.connect().bulkcharge.list()['data']
            
        
        elif self.config['records'] == 'paystack.refund':
            logger.info(" refund \n")
            response = self.connect().refund.list()['data']
            
          
        try:    
            flattened_data = [self.flatten_dict(item) for item in response]
              
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
        except Exception as http_err:
            logger.error(f"HTTP error occurred: {http_err}")  
            







