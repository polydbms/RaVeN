from pathlib import Path

import rasterio
from jinja2 import Template
from osgeo import gdal

from configuration import PROJECT_ROOT
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
import json

from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path
        self.raster_path = raster_path
        self.host_base_path = self.network_manager.system_full.host_base_path
        # self.vector_path = None
        # self.raster_path = None
        self.transporter = FileTransporter(network_manager)
        # if Path(vector_path).exists() and Path(vector_path).is_dir():
        #     self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        # if Path(raster_path).exists() and Path(raster_path).is_dir():
        #     self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
        #         0
        #     ]

    @measure_time
    def ingest_raster(self, **kwargs):

        template_path = Path(PROJECT_ROOT.joinpath("hub/deployment/files/rasdaman/ingestion.json.j2"))
        try:
            with open(template_path) as file_:
                template = Template(file_.read())
                return template
        except FileNotFoundError:
            print(f"{template_path} not found")

        with rasterio.open(self.raster_path.controller_file) as f:
            epsg_crs = f.crs.to_epsg()

        payload = {
            "coverage_id": str(self.raster_path.name),
            "paths": [str(self.raster_path.docker_file)],
            "epsg_crs": str(epsg_crs)
        }

        rendered = template.render(**payload)
        ingest_def_path = Path("hub/deployment/files/rasdaman/ingredients.json")

        with open(ingest_def_path, "w") as f:
            f.write(rendered)

        self.transporter.send_file(
            ingest_def_path,
            self.host_base_path.joinpath("config/rasdaman/ingredients.json")
        )
        self.network_manager.run_ssh(
            str(self.host_base_path.joinpath("config/rasdaman/ingest.sh"))
        )
        ingest_def_path.unlink()

    @measure_time
    def ingest_vector(self, **kwargs):
        print("cannot ingest vectors")
