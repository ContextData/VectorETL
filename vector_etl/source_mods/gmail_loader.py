import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pprint import pprint
from .base import BaseSource
import pandas as pd

class GmailSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    def connect(self):
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", self.SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials'], self.SCOPES
                )
                creds = flow.run_local_server(port=0)
                with open("token.json", "w") as token:
                    token.write(creds.to_json())
        return creds

    def fetch_data(self):
        creds = self.connect()
        service = build("gmail", "v1", credentials=creds)

        # Extract the label from config
        label = self.config.get('gmail.label').upper()

        messages = self.get_messages(service, label)
        
        if messages:
            email_data = self.parse_messages(service, messages, label)
            df = pd.DataFrame(email_data)
            
            return df
        else:
            print("No messages found.")
            return None

    def get_messages(self, service, label):
        try:
            results = service.users().messages().list(userId="me", labelIds=[label]).execute()
            return results.get("messages", [])
        except HttpError as error:
            print(f"An error occurred while fetching messages for label {label}: {error}")
            return None

    def parse_messages(self, service, messages, label):
        email_data = {
            "id": [],
            "threadId": [],
            "label": [],
            "subject": [],
            "from": [],
            "snippet": [],
            "body": [],
        }

        for message in messages:
            msg = service.users().messages().get(userId="me", id=message["id"]).execute()
            headers = msg["payload"]["headers"]

            subject, sender = self.get_header_info(headers)
            snippet = msg.get("snippet", "")
            body = self.get_body(msg)

            email_data["id"].append(message["id"])
            email_data["threadId"].append(message["threadId"])
            email_data["label"].append(label)
            email_data["subject"].append(subject)
            email_data["from"].append(sender)
            email_data["snippet"].append(snippet)
            email_data["body"].append(body)

        return email_data

    def get_header_info(self, headers):
        subject = None
        sender = None
        for header in headers:
            if header["name"] == "Subject":
                subject = header["value"]
            if header["name"] == "From":
                sender = header["value"]
        return subject, sender

    def get_body(self, msg):
        if "data" in msg["payload"]["body"]:
            return base64.urlsafe_b64decode(msg["payload"]["body"]["data"]).decode("utf-8")
        else:
            return ""
