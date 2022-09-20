from pathlib import Path
from hub.evaluation.main import measure_time
from hub.utils.datalocation import DataLocation


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path.docker_file
        self.raster_path = raster_path.docker_file.with_suffix(".shp")
        # if Path(vector_path).exists() and Path(vector_path).is_dir():
        #     self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        # if Path(raster_path).exists() and Path(raster_path).is_dir():
        #     self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
        #         0
        #     ]
        #     self.raster_path = str(self.raster_path).replace(
        #         self.raster_path.suffix, ".shp"
        #     )

    @measure_time
    def ingest_raster(self, **kwargs):
        self.network_manager.run_ssh(f"~/config/omnisci/ingest.sh -r={self.raster_path}")

    @measure_time
    def ingest_vector(self, **kwargs):
        self.network_manager.run_ssh(f"~/config/omnisci/ingest.sh -v={self.vector_path}")
