import logging
import os
from io import BytesIO, BufferedIOBase
import shutil
from box_sdk_gen import BoxClient, BoxDeveloperTokenAuth
from .file_loader import FileBaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BoxSource(FileBaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.client = None
        self.folder_path = config['folder_path']
        self.access_token = config['access_token']
        self.file_type = config.get('file_type', '')

    def connect(self):
        logger.info("Connecting to Box...")
        auth: BoxDeveloperTokenAuth = BoxDeveloperTokenAuth(token=self.access_token)
        self.client: BoxClient = BoxClient(auth=auth)
        logger.info("Connected to Box successfully.")

    def list_files(self):
        if not self.client:
            self.connect()

        logger.info(f"Listing files in Box folder: {self.folder_path}")

        path_parts = self.folder_path.strip('/').split('/')

        # Start from the root folder
        current_folder_id = '0'

        # Traverse the path
        for part in path_parts:
            items = self.client.folders.get_folder_items(current_folder_id).entries
            found = False
            for item in items:
                if item.type == 'folder' and item.name == part:
                    current_folder_id = item.id
                    found = True
                    break
            if not found:
                logger.info(f"Subfolder '{part}' not found in the specified path.")
                return

        # List files in the final subfolder
        items = self.client.folders.get_folder_items(current_folder_id).entries
        files = []
        for item in items:
            if item.type == 'file':
                if item.name.endswith(self.file_type):
                    logger.info(f"File: {item.name}")
                    files.append(os.path.join(self.folder_path, item.name))
        print(files)
        return files

    def read_file(self, file_id):
        logger.info(f"Reading file with ID: {file_id}")
        file_content = self.client.file(file_id).content()
        return BytesIO(file_content)

    def download_file(self, file_path):
        if not self.client:
            self.connect()

        download_folder = 'tempfile_downloads'
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info("Downloading files from Box...")
        print("==========")
        print(file_path)

        local_file_path = os.path.join(download_folder, file_path.split('/')[-1])

        path_parts = file_path.strip('/').split('/')
        file_name = path_parts.pop()

        # Start from the root folder
        current_folder_id = '0'

        # Traverse the path
        for part in path_parts:
            items = self.client.folders.get_folder_items(current_folder_id).entries
            found = False
            for item in items:
                if item.type == 'folder' and item.name == part:
                    current_folder_id = item.id
                    found = True
                    break
            if not found:
                logger.info(f"Folder '{part}' not found in the specified path.")
                return

        # Find the file in the final folder
        items = self.client.folders.get_folder_items(current_folder_id).entries
        file_id = None
        for item in items:
            if item.type == 'file' and item.name == file_name:
                file_id = item.id
                break

        if file_id:
            file_content_stream: BufferedIOBase = self.client.downloads.download_file(
                file_id=file_id
            )
            with open(local_file_path, 'wb') as f:
                shutil.copyfileobj(file_content_stream, f)
            logger.info(f"File '{file_name}' downloaded successfully.")
        else:
            logger.info(f"File '{file_name}' not found in the specified folder.")

        logger.info(f'File downloaded to {local_file_path}')

    def delete_directory(self, path):

        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(path)
