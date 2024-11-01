from abc import ABC, abstractmethod

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class IngestionInterface(ABC):
    """
    A generic interface for the ingestors
    """

    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        """
        initialitzes an ingestor
        :param vector_path: the vector data
        :param raster_path: te raster data
        :param network_manager: the network manager
        :param benchmark_params: the benchmark parameters
        :param workload: the workload definition as benchi DSL
        """
        pass

    @abstractmethod
    def ingest_raster(self):
        """
        start ingesting the raster data
        :return:
        """
        pass

    @abstractmethod
    def ingest_vector(self):
        """
        start ingesting the vector data
        :return:
        """
        pass


class ExecutorInterface(ABC):
    """
    A generic interface for the executors
    """

    def __init__(self, vector_path: DataLocation,
                 raster_path: DataLocation,
                 network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        """
        initialitzes an executor
        :param vector_path: the vector data
        :param raster_path: te raster data
        :param network_manager: the network manager
        :param benchmark_params: the benchmark parameters
        """
        pass

    @abstractmethod
    def run_query(self, query: dict, warm_start_no: int):
        """
        start running the query on the system-under-test
        :param query: the query as Benchi DSL
        :param warm_start_no: the iteration number of the query
        :return:
        """
        pass

    @abstractmethod
    def post_run_cleanup(self):
        """
        clean up after the query run
        :return:
        """
        pass
