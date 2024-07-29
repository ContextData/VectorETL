from .openai import OpenAIEmbedding
from .cohere import CohereEmbedding
from .google_gemini import GoogleGeminiEmbedding
from .azure_openai import AzureOpenAIEmbedding
from .huggingface import HuggingFaceEmbedding

def get_embedding_model(config):
    embedding_type = config['embedding_model']
    if embedding_type == 'OpenAI':
        return OpenAIEmbedding(config)
    elif embedding_type == 'Cohere':
        return CohereEmbedding(config)
    elif embedding_type == 'Google Gemini':
        return GoogleGeminiEmbedding(config)
    elif embedding_type == 'Azure OpenAI':
        return AzureOpenAIEmbedding(config)
    elif embedding_type == 'Hugging Face':
        return HuggingFaceEmbedding(config)
    else:
        raise ValueError(f"Unsupported embedding model: {embedding_type}")
