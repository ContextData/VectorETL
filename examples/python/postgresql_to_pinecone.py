from vector_etl import create_flow

source = {
    "source_data_type": "database",
    "db_type": "postgres",
    "host": "localhost",
    "database_name": "mydb",
    "username": "user",
    "password": "password",
    "port": 5432,
    "query": "SELECT * FROM mytable WHERE updated_at > :last_updated_at",
    "batch_size": 1000,
    "chunk_size": 1000,
    "chunk_overlap": 0
}

embedding = {
    "embedding_model": "OpenAI",
    "api_key": "your-openai-api-key",
    "model_name": "text-embedding-ada-002"
}

target = {
    "target_database": "Pinecone",
    "pinecone_api_key": "your-pinecone-api-key",
    "index_name": "my-index",
    "dimension": 1536, #[Optional] Only required if creating a new index
    "metric": "cosine",  #[Optional] Only required if creating a new index
    "cloud": "aws", #[Optional] Only required if creating a new index
    "region": "us-east-1" #[Optional] Pinecone will default to us-east-1
}

embed_columns = [
    "column1",
    "column2",
    "column3"
]

flow = create_flow()
flow.set_source(source)
flow.set_embedding(embedding)
flow.set_target(target)
flow.set_embed_columns(embed_columns)

# Execute the flow
flow.execute()
