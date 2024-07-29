import logging
import os
from io import BytesIO
import shutil
from .file_loader import FileBaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalFileSource(FileBaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = config['file_path']
        self.directory = os.path.dirname(self.file_path)
        self.file_pattern = os.path.basename(self.file_path)

    def connect(self):
        # For local files, we don't need to establish a connection
        # But we'll verify that the directory exists
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"The directory {self.directory} does not exist.")
        logger.info(f"Verified existence of local directory: {self.directory}")

    def list_files(self):
        logger.info(f"Listing files in {self.directory} matching pattern {self.file_pattern}")
        matching_files = []
        for filename in os.listdir(self.directory):
            logger.info(f"Filename: {filename}")
            if filename.startswith(self.file_pattern) or self.file_pattern == '*':
                matching_files.append(os.path.join(self.directory, filename))
        return matching_files

    def read_file(self, file_path):
        logger.info(f"Reading file: {file_path}")
        with open(file_path, 'rb') as file:
            return BytesIO(file.read())

    def download_file(self, file_path):

        download_folder = 'tempfile_downloads'
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info("Copying file(s) to local directory...")
        shutil.copy(file_path, download_folder)

    def delete_directory(self, path):

        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(path)