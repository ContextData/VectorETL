import logging
import pandas as pd
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
from .base import BaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


class GoogleDriveSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.service = None

    def connect(self):
        logger.info("Connecting to Google Drive...")
        creds = None
        token_path = self.config.get('token_path', 'token.json')

        if os.path.exists(token_path):
            try:
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            except ValueError:
                logger.warning("Existing token is invalid. Reauthorizing...")
                os.remove(token_path)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.warning(f"Failed to refresh token: {e}. Reauthorizing...")
                    creds = None

            if not creds:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config['credentials_path'], SCOPES)
                creds = flow.run_local_server(port=0)

            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        self.service = build('drive', 'v3', credentials=creds)
        logger.info("Connected to Google Drive successfully.")

    def fetch_data(self):
        if not self.service:
            self.connect()

        folder_id = self.config['folder_id']
        file_type = self.config['file_type']
        chunk_size = self.config.get('chunk_size', 1000)
        chunk_overlap = self.config.get('chunk_overlap', 0)

        query = f"'{folder_id}' in parents and mimeType contains '{file_type}'"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        df = pd.DataFrame()
        new_files = []

        for file in files:
            file_id = file['id']
            file_name = file['name']

            if file_id not in self.config.get('loaded_files', []):
                new_files.append(file_id)

                request = self.service.files().get_media(fileId=file_id)
                fh = BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()

                fh.seek(0)

                if file_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                    temp_df = pd.read_excel(fh)
                elif file_type == 'text/csv':
                    temp_df = pd.read_csv(fh)
                elif file_type in ['text/plain', 'application/pdf']:
                    # Implement text processing logic here
                    pass

                if not df.empty:
                    df = pd.concat([df, temp_df], ignore_index=True)
                else:
                    df = temp_df

        if not df.empty:
            df = self.split_dataframe_column(df, chunk_size, chunk_overlap)

        return df, new_files

    def split_dataframe_column(self, df, chunk_size, chunk_overlap, column='__concat_final'):
        logger.info("Splitting dataframe into chunks...")

        def split_text(text, size, overlap):
            if not isinstance(text, str):
                return []
            return [text[i:i + size] for i in range(0, len(text), size - overlap) if text[i:i + size]]

        new_rows = []
        for _, row in df.iterrows():
            chunks = split_text(row[column], chunk_size, chunk_overlap)
            for chunk in chunks:
                if chunk:
                    new_row = row.copy()
                    new_row[column] = chunk
                    new_rows.append(new_row)

        return pd.DataFrame(new_rows, columns=df.columns)
