from hub.enums.stage import Stage
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector: DataLocation, raster: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector = vector
        self.raster = raster
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params

    @measure_time
    def ingest_raster(self, **kwargs):
        script = self.host_base_path.joinpath(f"config/postgis-vec/ingest.sh")

        command = (f"{script} "
                   f"-r={self.raster.docker_file_preprocessed[0]} " # FIXME
                   f"-n={self.raster.name} "
                   f"-s={self.benchmark_params.raster_target_crs.to_epsg() if self.benchmark_params.align_crs_at_stage == Stage.PREPROCESS else self.raster.get_crs().to_epsg()} ")

        self.network_manager.run_ssh(command)

    @measure_time
    def ingest_vector(self, **kwargs):
        command = self.host_base_path.joinpath(f"config/postgis-vec/ingest.sh")
        self.network_manager.run_ssh(
            f"{command} "
            f"-v={self.vector.docker_file_preprocessed[0]} " # FIXME
            f"-n={self.vector.name} "
            f"-s={self.benchmark_params.vector_target_crs.to_epsg() if self.benchmark_params.align_crs_at_stage == Stage.PREPROCESS else self.vector.get_crs().to_epsg()} "
        )
