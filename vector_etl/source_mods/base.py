from abc import ABC, abstractmethod

class BaseSource(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def fetch_data(self):
        pass
