from zenpy import Zenpy
import pandas as pd
import logging
from .base import BaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZendeskSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.client = None

    def connect(self):
        creds = {
            'email': self.config['user_email'],
            'token': self.config['access_token'],
            'subdomain': self.config['subdomain']
        }
        self.client = Zenpy(**creds)
        logger.info("Connected to Zendesk")

    def fetch_data(self):
        if not self.client:
            self.connect()

        table = self.config['table']
        start_date = self.config.get('start_date')

        if start_date:
            data = getattr(self.client, table).incremental(start_time=start_date)
        else:
            data = getattr(self.client, table)()

        df = pd.DataFrame([item.to_dict() for item in data])
        return df
