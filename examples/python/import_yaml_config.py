from vector_etl import create_flow


flow = create_flow()
flow.load_yaml('/examples/config_files/postgres_to_pinecone.yaml')
flow.execute()