import stripe
import pandas as pd
from datetime import datetime, timedelta
import logging
import uuid
import requests
from .base import BaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StripeSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.STRIPE_ENDPOINTS = {
            "balance_transactions": "https://api.stripe.com/v1/balance_transactions?limit=100",
            "charges": "https://api.stripe.com/v1/charges?limit=100",
            "customers": "https://api.stripe.com/v1/customers?limit=100",
            "events": "https://api.stripe.com/v1/events?limit=100",
            "refunds": "https://api.stripe.com/v1/refunds?limit=100",
            "disputes": "https://api.stripe.com/v1/disputes?limit=100",
        }

    def connect(self):
        # Stripe doesn't require a persistent connection
        pass

    def fetch_data(self):
        start_date = None #self._get_start_date()
        url = self.STRIPE_ENDPOINTS[self.config["table"]]
        if start_date:
            url += f"&created[gte]={int(start_date.timestamp())}"

        headers = {"Authorization": f"Bearer {self.config['access_token']}"}
        response = requests.get(url, headers=headers)
        json_data = response.json()
        df = pd.json_normalize(json_data["data"])
        return self._prepare_dataframe(df)


    def _prepare_dataframe(self, df):
        def format_column_name(name):
            return ' '.join(word.capitalize() for word in name.split('_'))

        columns = df.columns.tolist()
        df['__concat_final'] = df.apply(lambda row: ' ~ '.join(f"{format_column_name(col)}: {row[col]}" for col in columns if row[col] is not None), axis=1)
        df['df_uuid'] = [uuid.uuid4() for _ in range(len(df.index))]
        return df

