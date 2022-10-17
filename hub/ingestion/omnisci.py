from pathlib import Path
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path.docker_file
        self.raster_path = raster_path.docker_file.with_suffix(".shp")
        self.host_base_path = self.network_manager.system_full.host_base_path


    @measure_time
    def ingest_raster(self, **kwargs):
        command = self.host_base_path.joinpath("config/omnisci/ingest.sh")
        self.network_manager.run_ssh(f"{command} -r={self.raster_path}")

    @measure_time
    def ingest_vector(self, **kwargs):
        command = self.host_base_path.joinpath("config/omnisci/ingest.sh")
        self.network_manager.run_ssh(f"{command} -v={self.vector_path}")
