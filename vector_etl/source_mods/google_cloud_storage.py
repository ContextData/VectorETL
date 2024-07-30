import logging
from io import BytesIO
import os
from google.cloud import storage
from .file_loader import FileBaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleCloudStorageSource(FileBaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.client = None
        self.bucket_name = config['bucket_name']
        self.prefix = config.get('prefix', '')
        self.file_type = config.get('file_type', '')

    def connect(self):
        logger.info("Connecting to Google Cloud Storage...")
        self.client = storage.Client.from_service_account_json(self.config['credentials_path'])
        logger.info("Connected to Google Cloud Storage successfully.")

    def list_files(self):
        if not self.client:
            self.connect()

        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.prefix)
        return [blob.name for blob in blobs if blob.name.endswith(self.file_type)]

    def read_file(self, file_path):
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(file_path)
        return BytesIO(blob.download_as_bytes())

    def download_file(self, file_path):
        bucket = self.client.get_bucket(self.bucket_name)
        blob = bucket.blob(file_path)

        download_folder = 'tempfile_downloads'
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info("Downloading files from Google Cloud Storage...")
        local_file_path = os.path.join(download_folder, file_path.split('/')[-1])
        blob.download_to_filename(local_file_path)

    def delete_directory(self, path):
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(path)
