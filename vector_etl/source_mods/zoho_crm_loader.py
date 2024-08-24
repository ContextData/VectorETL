from base import BaseSource
import pandas as pd
import requests
import logging
from pprint import pprint
import os
import json


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





class ZohoCrmSource(BaseSource):
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
        if self.config['records'] == 'module.Contacts':
            logger.info("Contact \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Contacts?fields=Acount_Name,
            First_Name,Lead_Source,Home,Fax,Skype_ID,Asst_Phone,Phone,
            Title,Department,Twitter,Last_Name,Contact_Name,Phone,Email,Reporting_To,
            Mailing_Street,Mailing_City,Mailing_State,Mailing_Zip,Mailing_Country,
            Description,Contact_Owner,Lead_Source,Date_of_Birth,Contact_Image  
            &converted=true&per_page={self.config['per_page']}"""
            
        elif self.config['records'] == 'module.Accounts':
            logger.info("Accounts \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Accounts?fields=Account_Owner,Account_Name,Account_Site,Parent_Account,
            Account_Number,Account_Type,Industry,Annual_Revenue,Rating,Phone,Fax,Website,Ticker_Symbol,OwnerShip,Employees,Sic_Code,
            Billing_Street,Billing_City,Billing_State,Billing_Code,Billing_Country,Shipping_Street,Shipping_City,Shipping_State,Shipping_Code,
            Shipping_Country,Description 
            &converted=true&per_page={self.config['per_page']}"""
            
       
            
            
        elif self.config['records'] == 'module.Leads':
            logger.info("Leads \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Leads?fields=Lead_Owner,First_Name,Title,Mobile,Lead_Source,
            Industry,Annual_Revenue,Company,Last_Name,Email,Fax,Website,Lead_Status,Rating,Skype_ID,
            Description,Twitter,City,Street,State,Country,Zip_Code,No_of_Employees 
            &converted=true&per_page={self.config['per_page']}"""
            
        elif self.config['records'] == 'module.Deals':
            logger.info("Deals \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Deals?fields=Deal_Owner,Deal_Name,Account_Name,
            Type,Next_Step,Lead_Source,Contact_Name,Amount,Closing_Date,Stage,Probability,Expected_Revenue,
            Campaign_Source,Description 
            &converted=true&per_page={self.config['per_page']}"""
        
        
            
        elif self.config['records'] == 'module.Campaigns':
            logger.info("Campaigns \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Campaigns?fields=Campaign_Owner,Campaign_Name,Start_Date,
            Expected_Revenue,Actual_Cost,Number_sent,Type,Status,End_Date,Budgeted_Cost,Expected_Response,Description
            &converted=true&per_page={self.config['per_page']}"""
            
        
        
            
        elif self.config['records'] == 'module.Tasks':
            logger.info("Tasks \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Tasks?fields=Task_Owner,Subject,Due_Date,Contact,Deal,Status,Priority,Reminder,
            Repeat,Description 
            &converted=true&per_page={self.config['per_page']}"""
            
       
    
        elif self.config['records'] == 'module.Calls':
            logger.info("Calls \n")
            self.url = f"""https://www.zohoapis.com/crm/v5/Calls?fields=Call_To,Related_To,Call_Type,Outgoing_Call_Status,
            Call_Start_Time,Call_Owner,Subject,Created_By,Modified_By,Call_Purpose,Call_Agenda&converted=true&per_page={self.config['per_page']}"""
            
            
        elif self.config['records'] == 'module':
            logger.info("Calls \n")
            self.url = "https://www.zohoapis.com/crm/v5/settings/modules,"
            
        headers = {"Authorization":f"Zoho-oauthtoken {self.token}"}
        
        response = requests.get(url=self.url,headers=headers).json()['data']
        
        flattened_data = [self.flatten_dict(item) for item in response]
        
                   
        df  = pd.DataFrame(flattened_data )
        
        logger.info(f" data \n {df}")
        
        return df
        
        
        




