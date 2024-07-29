from abc import ABC, abstractmethod

class BaseTarget(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def write_data(self, df, columns, domain=None):
        pass

    @abstractmethod
    def create_index_if_not_exists(self, dimension):
        pass