from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector: DataLocation, raster: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector = vector
        self.raster = raster
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params

    @measure_time
    def ingest_raster(self, **kwargs):
        command = self.host_base_path.joinpath("config/omnisci/ingest.sh")
        self.network_manager.run_ssh(f""
                                     f"{command} "
                                     f"-r={self.raster.docker_file_preprocessed} "
                                     f"-n={self.raster.name} ")

    @measure_time
    def ingest_vector(self, **kwargs):
        command = self.host_base_path.joinpath("config/omnisci/ingest.sh")
        self.network_manager.run_ssh(f""
                                     f"{command} "
                                     f"-v={self.vector.docker_file_preprocessed} "
                                     f"-n={self.vector.name}")
