from pathlib import Path
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
import json

from hub.utils.network import NetworkManager

INGREDIENTS = {
    "config": {
        "service_url": "http://localhost:8080/rasdaman/ows",
        "tmp_directory": "/tmp/",
        "crs_resolver": "http://localhost:8080/def/",
        "default_crs": "http://localhost:8080/def/OGC/0/Index2D",
        "automated": True,
        "track_files": False,
        "mock": False,
    },
    "input": {"coverage_id": "", "paths": ""},
    "recipe": {
        "name": "general_coverage",
        "options": {
            "coverage": {
                "crs": "EPSG/0/4326",
                "metadata": {"type": "xml"},
                "slicer": {
                    "type": "gdal",
                    "axes": {
                        "Long": {
                            "min": "${gdal:minX}",
                            "max": "${gdal:maxX}",
                            "gridOrder": 0,
                            "resolution": "${gdal:resolutionX}",
                        },
                        "Lat": {
                            "min": "${gdal:minY}",
                            "max": "${gdal:maxY}",
                            "gridOrder": 1,
                            "resolution": "${gdal:resolutionY}",
                        },
                    },
                },
            }
        },
    },
}


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
        INGREDIENTS["input"] = {
            "coverage_id": str(self.raster_path.name),
            "paths": [str(self.raster_path.docker_file)],
        }
        with open("hub/deployment/files/rasdaman/ingredients.json", "w") as f:
            json.dump(INGREDIENTS, f)
        self.transporter.send_file(
            Path("hub/deployment/files/rasdaman/ingredients.json"),
            self.host_base_path.joinpath("config/rasdaman/ingredients.json")
        )
        self.network_manager.run_ssh(
            # f"/opt/rasdaman/bin/wcst_import.sh ~/config/rasdaman/ingredients.json"
            str(self.host_base_path.joinpath("config/rasdaman/ingest.sh"))
        )
        Path("hub/deployment/files/rasdaman/ingredients.json").unlink()

    @measure_time
    def ingest_vector(self, **kwargs):
        print("cannot ingest vectors")
