from abc import ABC, abstractmethod


class IngestionInterface(ABC):
    @abstractmethod
    def send_data(self, data):
        pass

    @abstractmethod
    def ingest(self, data):
        pass


class ExecutorInterface(ABC):
    @abstractmethod
    def run_query(self, query):
        pass


class PreprocessingInterfacce(ABC):
    @abstractmethod
    def covert_data(self, data):
        pass
