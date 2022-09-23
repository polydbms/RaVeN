from pathlib import Path
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path.docker_file
        self.raster_path = raster_path.docker_file.with_suffix(".shp")


    @measure_time
    def ingest_raster(self, **kwargs):
        self.network_manager.run_ssh(f"~/config/omnisci/ingest.sh -r={self.raster_path}")

    @measure_time
    def ingest_vector(self, **kwargs):
        self.network_manager.run_ssh(f"~/config/omnisci/ingest.sh -v={self.vector_path}")
