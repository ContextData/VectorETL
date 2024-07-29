import boto3
import logging
from io import BytesIO
import os
from .file_loader import FileBaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Source(FileBaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.s3_client = None
        self.bucket_name = config['bucket_name']
        self.prefix = config.get('prefix', '')
        self.file_type = config.get('file_type', '')

    def connect(self):
        logger.info("Connecting to Amazon S3...")
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key']
        )
        logger.info("Connected to Amazon S3 successfully.")

    def list_files(self):
        if not self.s3_client:
            self.connect()

        paginator = self.s3_client.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith(self.file_type):
                    files.append(obj['Key'])

        return files

    def read_file(self, file_path):
        downloaded_files = []

        local_file_path = os.path.join(os.getcwd(), file_path.split('/')[-1])
        self.s3_client.download_file(self.bucket_name, file_path, local_file_path)
        downloaded_files.append(file_path)
        logger.info(f"Downloaded {file_path} to {os.getcwd()}")

        return downloaded_files


    def download_file(self, file_path):
        if not self.s3_client:
            self.connect()

        download_folder = 'tempfile_downloads'
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info("Downloading files from S3...")

        local_file_path = os.path.join(download_folder, file_path.split('/')[-1])
        self.s3_client.download_file(self.bucket_name, file_path, local_file_path)
        logger.info(f"Downloaded {file_path} to {os.getcwd()}")


    def delete_directory(self, path):

        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(path)