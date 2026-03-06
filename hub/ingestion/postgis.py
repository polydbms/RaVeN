from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.stage import Stage
from hub.evaluation.measure_time import measure_time
from hub.utils.network import NetworkManager
from hub.utils.rasterlocation import RasterLocation
from hub.utils.vectorlocation import VectorLocation


class Ingestor:
    def __init__(self, vector: VectorLocation, raster: RasterLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector = vector
        self.raster = raster
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params


    def ingest_raster(self, **kwargs):
        command = self.host_base_path.joinpath(f"config/postgis/ingest.sh")

        raster_relevant = self.raster.get_relevant_status("is_ingested")
        raster_to_ingest = raster_relevant[~raster_relevant["is_ingested"]]

        if raster_to_ingest.empty:
            return

        mode = "a" if len(raster_to_ingest) < len(raster_relevant) else "c"

        # raster_path = self.raster.docker_dir.joinpath(f"*{self.raster.target_suffix.value}") if self.raster.is_multifile() else self.raster.docker_file_preprocessed[0]
        raster_path = " ".join(str(f) for f in self.raster.docker_file_not_ingested())
        self.network_manager.run_ssh(
            f"{command} "
            f"-r=\"{raster_path}\" "
            f"-n={self.raster.name} "
            f"-s={self.benchmark_params.raster_target_crs.to_epsg() if self.benchmark_params.align_crs_at_stage == Stage.PREPROCESS else self.raster.get_crs().to_epsg()} "
            f"-t={self.benchmark_params.raster_tile_size.postgis_str} "
            f"-m={mode}"
        )

    @measure_time
    def ingest_vector(self, **kwargs):
        if self.vector.is_ingested:
            return

        command = self.host_base_path.joinpath(f"config/postgis/ingest.sh")
        self.network_manager.run_ssh(
            f"{command} "
            f"-v={self.vector.docker_file_preprocessed[0]} " # FIXME
            f"-n={self.vector.name} "
            f"-s={self.benchmark_params.vector_target_crs.to_epsg() if self.benchmark_params.align_crs_at_stage == Stage.PREPROCESS else self.vector.get_crs().to_epsg()} "
        )
