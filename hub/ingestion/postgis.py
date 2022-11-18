from pathlib import Path

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector: DataLocation, raster: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector.docker_file
        self.raster_path = raster.docker_file
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params

    @measure_time
    def ingest_raster(self, **kwargs):
        command = self.host_base_path.joinpath(f"config/postgis/ingest.sh")
        self.network_manager.run_ssh(
            f"{command} "
            f"-r={self.raster_path} "
            f"-s={self.benchmark_params.raster_target_crs.to_epsg()} "
            f"-t={self.benchmark_params.raster_tile_size}"
        )

    @measure_time
    def ingest_vector(self, **kwargs):
        command = self.host_base_path.joinpath(f"config/postgis/ingest.sh")
        self.network_manager.run_ssh(
            f"{command} -v={self.vector_path} -s={self.benchmark_params.vector_target_crs.to_epsg()}"
        )
