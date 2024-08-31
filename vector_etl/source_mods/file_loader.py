import logging
import pandas as pd
from abc import abstractmethod
import os
from unstructured.partition.auto import partition
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError
import nltk
nltk.download('averaged_perceptron_tagger')
from .base import BaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileBaseSource(BaseSource):
    def __init__(self, config):
        self.config = config
        self.chunk_size = config.get('chunk_size', 1000)
        self.chunk_overlap = config.get('chunk_overlap', 0)

    @abstractmethod
    def list_files(self):
        """List all files to be processed."""
        pass

    # @abstractmethod
    # def read_file(self, file_path):
    #     """Read a single file and return its content."""
    #     pass

    @abstractmethod
    def download_file(self, file_path):
        """Read a single file and return its content."""
        pass

    @abstractmethod
    def delete_directory(self, path):
        """Delete temporary directory."""
        pass

    def parse_text_files(self, file_path, file_extension):
        elements = partition(file_path, mode="elements", strategy="fast")

        ids = []
        document = []
        category = []
        file_type = []
        file_name = []
        parent_id = []

        coordinates = [] if file_extension == "pdf" else None
        page_number = [] if file_extension == "pdf" else None

        for el in elements:
            ids.append(el.id)
            document.append(el.text)
            category.append(el.category)
            file_type.append(el.metadata.to_dict()["filetype"])
            file_name.append(el.metadata.to_dict()["filename"])
            parent_id.append(el.metadata.to_dict()["parent_id"] if 'parent_id' in el.metadata.to_dict() else None)

            if file_extension == "pdf":# and el.metadata.to_dict()["filetype"].lower() == 'pdf':
                coordinates.append(el.metadata.to_dict()["coordinates"])
                page_number.append(el.metadata.to_dict()["page_number"])

        data = {
            'id': ids,
            'parent_id': parent_id,
            'text': document,
            'category': category,
            'file_type': file_type,
            'file_name': file_name
        }

        if file_extension == "pdf":
            data['page_number'] = page_number
            data['coordinates'] = coordinates

        return pd.DataFrame(data)


    def parse_text_files_unstructured(self, file_path, file_extension):
        logger.info("Using Unstructured API...")

        client = UnstructuredClient(
            api_key_auth=self.config.get('unstructured_api_key', ''),
            server_url=self.config.get('unstructured_url', '')
        )

        file = open(file_path, "rb")
        req = shared.PartitionParameters(
            files=shared.Files(
                content=file.read(),
                file_name=file_path,
            ),
            strategy="auto",
            coordinates=True,
        )

        try:
            res = client.general.partition(req)
            elements = res.elements
            ids = []
            document = []
            type = []
            file_type = []
            file_name = []
            parent_id = []

            coordinates = [] if file_extension == "pdf" else None
            page_number = [] if file_extension == "pdf" else None

            for el in elements:
                ids.append(el['element_id'])
                document.append(el['text'])
                type.append(el['type'])
                file_type.append(el['metadata']["filetype"])
                file_name.append(el['metadata']["filename"])
                parent_id.append(el['metadata']["parent_id"] if 'parent_id' in el['metadata'] else None)

                if file_extension == "pdf":  # and el.metadata.to_dict()["filetype"].lower() == 'pdf':
                    coordinates.append(el['metadata']["coordinates"])
                    page_number.append(el['metadata']["page_number"])

            data = {
                'element_id': ids,
                'parent_id': parent_id,
                'text': document,
                'type': type,
                'file_type': file_type,
                'file_name': file_name
            }

            if file_extension == "pdf":
                data['page_number'] = page_number
                data['coordinates'] = coordinates
            return pd.DataFrame(data)
        except SDKError as e:
            logger.error(f"Unstructured process failed: {str(e)}")
            raise


    def process_file(self, file_path):
        file_type = file_path.split('.')[-1].lower()
        content = self.download_file(file_path)

        local_file_path = f"tempfile_downloads/{file_path.split('/')[-1]}"

        if file_type == 'csv':
            df = pd.read_csv(local_file_path)
        elif file_type in ['xlsx', 'xls']:
            df = pd.read_excel(local_file_path)
        elif file_type == 'json':
            df = pd.read_json(local_file_path)
        elif file_type in ['txt', 'pdf', 'doc', 'docx']:
            if self.config.get('use_unstructured'):
                df = self.parse_text_files_unstructured(local_file_path, file_type)
            else:
                df = self.parse_text_files(local_file_path, file_type)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        return df

    def fetch_data(self):
        logger.info("Fetching data from files...")
        files = self.list_files()

        if not files: #if len(files) == 0:
            logger.info("No files to process. Exiting...")
            raise ValueError("No files found to process")

        else:
            df = pd.DataFrame()
            new_files = []

            for file_path in files:
                new_files.append(file_path)
                temp_df = self.process_file(file_path)

                if df.empty:
                    df = temp_df
                else:
                    df = pd.concat([df, temp_df], ignore_index=True)

            if not df.empty:
                df = self.split_dataframe_column(df, self.chunk_size, self.chunk_overlap)

            delete_temp_dir = self.delete_directory('tempfile_downloads')

            return df

    def split_dataframe_column(self, df, chunk_size, chunk_overlap, column='text'):
        logger.info("Splitting dataframe into chunks...")

        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in dataframe. Skipping splitting.")
            return df

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

