from pathlib import Path
from hub.evaluation.main import measure_time
from jinja2 import Template
import os

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))


class Ingestor:
    def __init__(self, vector_path, raster_path, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = None
        self.raster_path = None
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
                0
            ]
        rendered = self.render_template()
        self.__save_template(rendered)

    @measure_time
    def ingest_raster(self, **kwargs):
        print("Ingestion and execution will be executed in the same time.")

    @measure_time
    def ingest_vector(self, **kwargs):
        print("Ingestion and execution will be executed in the same time.")

    def __read_template(self, path):
        try:
            with open(path) as file_:
                template = Template(file_.read())
                return template
        except FileNotFoundError:
            print(f"{path} not found")

    def render_template(self):
        template_path = Path(f"{CURRENT_PATH}/../deployment/files/sedona/sedona.py.j2")
        template = self.__read_template(template_path)
        raster_name = f"{self.raster_path.stem}".split(".")[0]
        vector_name = f"{self.vector_path.stem}".split(".")[0]
        payload = {
            "vector_path": self.vector_path.parent,
            "raster_path": self.raster_path.parent,
            "vector_name": vector_name,
            "raster_name": raster_name,
            "query": "{{query}}",
        }
        rendered = template.render(**payload)
        return rendered

    def __save_template(self, template):
        template_path = Path(f"sedona_ingested.py.j2")
        with open(template_path, "w") as f:
            f.write(template)
