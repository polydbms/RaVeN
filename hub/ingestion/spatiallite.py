from pathlib import Path
from hub.evaluation.measure_time import measure_time


class Ingestor:
    def __init__(self, vector_path, raster_path, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = None
        self.raster_path = None
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
            self.vector_path = str(self.vector_path).replace(
                self.vector_path.suffix, ""
            )
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
                0
            ]
            self.raster_path = str(self.raster_path).replace(
                self.raster_path.suffix, ""
            )

    @measure_time
    def ingest_raster(self, **kwargs):
        self.network_manager.run_ssh(
            f"~/config/ingest.sh -r={self.raster_path} -s=4326"
        )

    @measure_time
    def ingest_vector(self, **kwargs):
        self.network_manager.run_ssh(
            f"~/config/ingest.sh -v={self.vector_path} -s=4326"
        )
