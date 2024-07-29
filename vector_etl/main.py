import logging
import yaml
import json
import argparse
from orchestrator import run_etl_process

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
    parser.add_argument('-c', '--config', type=str, required=True, help="Path to configuration file")
    args = parser.parse_args()

    try:
        config = load_config(args.config)

        run_etl_process(config['source'],
                        config['embedding'],
                        config['target'],
                        config['embed_columns'])

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
