import logging
import yaml
import os
import json
import argparse
from vector_etl import __version__, run_etl_process, run_etl_process_py
from vector_etl.orchestrator import run_etl_process, run_etl_process_py

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(file_path):
    with open(file_path, 'r') as file:
        if file_path.endswith('.yaml') or file_path.endswith('.yml'):
            return yaml.safe_load(file)
        elif file_path.endswith('.json'):
            return json.load(file)
        else:
            raise ValueError("Unsupported configuration file format. Use .yaml, .yml, or .json")

def main():
    parser = argparse.ArgumentParser(description="Run ETL process")
    parser.add_argument('-c', '--config', type=str, default='config/config.yaml',
                        help="Path to configuration file")
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}')

    args = parser.parse_args()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, args.config)

    try:
        config = load_config(config_path)

        run_etl_process(config['source'],
                        config['embedding'],
                        config['target'],
                        config['embed_columns'])

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
