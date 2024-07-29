from abc import ABC, abstractmethod

class BaseEmbedding(ABC):
    @abstractmethod
    def embed(self, df, embed_column='__concat_final'):
        pass
