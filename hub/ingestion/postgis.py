from pathlib import Path
from hub.evaluation.main import measure_time
from hub.utils.datalocation import DataLocation


class Ingestor:
    def __init__(self, vector: DataLocation, raster: DataLocation, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector.docker_file
        self.raster_path = raster.docker_file

    @measure_time
    def ingest_raster(self, **kwargs):
        self.network_manager.run_ssh(
            f"~/config/postgis/ingest.sh -r={self.raster_path} -s=4326 -t=100x100"
        )

    @measure_time
    def ingest_vector(self, **kwargs):
        self.network_manager.run_ssh(
            f"~/config/postgis/ingest.sh -v={self.vector_path} -s=4326"
        )
