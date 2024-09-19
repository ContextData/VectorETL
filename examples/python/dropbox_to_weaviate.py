from vector_etl import create_flow

source = {
    "source_data_type": "Dropbox",
    "key": "",
    "folder_path": "/root/ContextData/",
    "file_type": "csv",
    "chunk_size": 1000,
    "chunk_overlap": 0
}

embedding = {
    "embedding_model": "Google Gemini",
    "api_key": "my-gemini-api-key",
    "model_name": "embedding-001"
}

target = {
    "target_database": "Weaviate",
    "weaviate_url": "my-clustername.region.gcp.weaviate.cloud",
    "weaviate_api_key": "my-weaviate-api-key",
    "class_name": "my-weaviate-class-name"
}

embed_columns = [] #Empty Array: File based sources do not require embedding columns

flow = create_flow()
flow.set_source(source)
flow.set_embedding(embedding)
flow.set_target(target)
flow.set_embed_columns(embed_columns)

# Execute the flow
flow.execute()
