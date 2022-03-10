from pathlib import Path
from hub.evaluation.main import measure_time
from hub.utils.preprocess import FileTransporter
import json

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
    def __init__(self, vector_path, raster_path, configurator) -> None:
        self.logger = {}
        self.configurator = configurator
        self.vector_path = None
        self.raster_path = None
        self.transporter = FileTransporter(configurator)
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
                0
            ]

    @measure_time
    def ingest_raster(self, **kwargs):
        raster_name = f"{self.raster_path.stem}".split(".")[0]
        raster_path = f"/home/azureuser/{self.raster_path}"
        INGREDIENTS["input"] = {
            "coverage_id": raster_name,
            "paths": [raster_path],
        }
        with open("ingredients.json", "w") as f:
            json.dump(INGREDIENTS, f)
        self.transporter.send_file("ingredients.json", "~/config/ingredients.json")
        self.configurator.run_ssh(
            f"/opt/rasdaman/bin/wcst_import.sh ~/config/ingredients.json"
        )
        Path("ingredients.json").unlink()

    @measure_time
    def ingest_vector(self, **kwargs):
        print("cannot ingest vectors")
