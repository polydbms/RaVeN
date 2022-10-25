from pathlib import Path

from configuration import PROJECT_ROOT
from hub.evaluation.measure_time import measure_time
from jinja2 import Template

from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path.docker_file
        self.raster_path = raster_path.docker_file
        self.host_base_path = self.network_manager.system_full.host_base_path

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
        template_path = Path(PROJECT_ROOT.joinpath("hub/deployment/files/sedona/sedona.py.j2"))
        template = self.__read_template(template_path)
        raster_name = self.raster_path.stem
        vector_name = self.vector_path.stem
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
        template_path = Path(f"hub/deployment/files/sedona/sedona_ingested.py.j2")
        with open(template_path, "w") as f:
            f.write(template)
