from .base import BaseSource
import pandas as pd
import requests
import logging
from pprint import pprint
import os
import json


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZohoDeskSource(BaseSource):
    def __init__(self,config):
        self.config = config
        self.token = None
        self.url = None
        self.grant_type = self.config['grant_type']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.code = self.config['code']
        self.accounts_url = self.config['accounts_url']
        
        
    def flatten_dict(self, d, parent_key='', sep='_'):

        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
        
        
    def connect(self):
        
        data = {
            "grant_type":self.grant_type,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": self.code
        }
        try:
            if os.path.exists("token.json"):
                with open("token.json",'r') as token_file:
                    token_data = json.load(token_file)
                    self.token = token_data.get("access_token")
                    return self.token
            else:  
                response = requests.post(url=self.accounts_url, data=data)
                logger.info(f"Status {response.status_code}")
                with open("token.json", 'w') as token_file:
                    json.dump({"access_token": response.json()["access_token"]}, token_file)
   
                logger.info("New token fetched and saved.")
                tokens = response.json()["access_token"]
                return tokens
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"connection Error {http_err}")
            
        
        
    def fetch_data(self):
        
        self.token = self.connect()
        if self.config['records'] == 'desk.agents':
            logger.info("Agents \n")
            self.url = f"https://desk.zoho.com/api/v1/agents?limit={self.config['limit']}"
            
            
        elif self.config['records'] == 'desk.team':
            logger.info("Teams \n")
            self.url = f"https://desk.zoho.com/api/v1/teams"
            headers = {"Authorization":f"Zoho-oauthtoken {self.token}"}
        
            response = requests.get(url=self.url,headers=headers).json()['teams']
            
            flattened_data = [self.flatten_dict(item) for item in response]
            
                    
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
            
        
             
        elif self.config['records'] == 'desk.ticket':
            logger.info("Ticket \n")
            self.url = f"""https://desk.zoho.com/api/v1/tickets?include=contacts,
            assignee,departments,team,isRead&limit={self.config['limit']}"""
            
        
        elif self.config['records'] == 'desk.contacts':
            logger.info("Contact \n")
            self.url = f"https://desk.zoho.com/api/v1/contacts?limit={self.config['limit']}"
            
           
        try:  
            headers = {"Authorization":f"Zoho-oauthtoken {self.token}"}
            
            response = requests.get(url=self.url,headers=headers)
            
            flattened_data = [self.flatten_dict(item) for item in response]
            
                    
            df  = pd.DataFrame(flattened_data )
            
            logger.info(f" data \n {df}")
            
            return df
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")            
       
        
        
        









