import dropbox
import logging
from io import BytesIO
import os
from .file_loader import FileBaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DropboxSource(FileBaseSource):
    def __init__(self, config):
        super().__init__(config)
        self.dbx = None
        self.folder_path = config['folder_path']
        self.file_type = config.get('file_type', '')

    def connect(self):
        logger.info("Connecting to Dropbox...")
        self.dbx = dropbox.Dropbox(self.config["key"])
        logger.info("Connected to Dropbox successfully.")

    def list_files(self):
        if not self.dbx:
            self.connect()

        files = []
        response = self.dbx.files_list_folder(self.folder_path, recursive=True)
        while True:
            for entry in response.entries:
                if isinstance(entry, dropbox.files.FileMetadata):
                    print("======================")
                    print(entry)
                    if entry.path_lower.endswith(self.file_type):
                        files.append(entry.path_lower)
            if response.has_more:
                response = self.dbx.files_list_folder_continue(response.cursor)
            else:
                break
        return files

    def read_file(self, file_path):
        _, response = self.dbx.files_download(file_path)
        return BytesIO(response.content)

    def download_file(self, file_path):
        if not self.dbx:
            self.connect()

        download_folder = 'tempfile_downloads'
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        logger.info("Downloading files from Dropbox...")
        local_path = os.path.join(download_folder, os.path.basename(file_path))
        with open(local_path, "wb") as f:
            metadata, res = self.dbx.files_download(path=file_path)
            f.write(res.content)

    def delete_directory(self, path):

        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(path)