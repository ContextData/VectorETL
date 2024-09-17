import yaml
from .orchestrator import run_etl_process_py

class Flow:
    def __init__(self, config=None):
        self.config = config or {}

    def set_source(self, source_config):
        self.config['source'] = source_config

    def set_embedding(self, embedding_config):
        self.config['embedding'] = embedding_config

    def set_target(self, target_config):
        self.config['target'] = target_config

    def set_embed_columns(self, embed_columns_config):
        self.config['embed_columns'] = embed_columns_config

    def load_yaml(self, yaml_path):
        with open(yaml_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def execute(self):
        if not all(key in self.config for key in ['source', 'embedding', 'target', 'embed_columns']):
            raise ValueError("Configuration is incomplete. Make sure source, embedding, and target are set.")

        run_etl_process_py(self.config)

def create_flow(config=None):
    return Flow(config)
