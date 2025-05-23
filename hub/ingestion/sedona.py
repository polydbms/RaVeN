from pathlib import Path

from hub.enums.rasterfiletype import RasterFileType
from hub.configuration import PROJECT_ROOT
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.vectorfiletype import VectorFileType
from hub.evaluation.measure_time import measure_time
from jinja2 import Template

from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector = vector_path
        self.raster = raster_path
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params

        self.controller_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona/sedona.py.j2")
        self.controller_ingest_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona/sedona_ingested.py.j2")


    @measure_time
    def ingest_raster(self, **kwargs):
        print("Ingestion and execution will be executed in the same time. rendering template instead.")
        rendered = self.render_template()
        self.save_template(rendered)

    @measure_time
    def ingest_vector(self, **kwargs):
        print("Ingestion and execution will be executed in the same time.")

    def read_template(self, path):
        try:
            with open(path) as file_:
                template = Template(file_.read())
                return template
        except FileNotFoundError:
            print(f"{path} not found")

    def render_template(self):
        template = self.read_template(self.controller_template_path)
        raster_name = self.raster.name
        vector_name = self.vector.name
        vector_reader, vector_method = self.get_vector_reader_from_filetype(self.benchmark_params.vector_target_format)
        raster_reader, raster_method = self.get_raster_reader_from_filetype(self.benchmark_params.raster_target_format)

        payload = {
            "vector_path": self.vector.docker_dir if self.benchmark_params.vector_target_format == VectorFileType.SHP else self.vector.docker_file_preprocessed[0],  # FIXME
            "raster_path": self.raster.docker_file_preprocessed[0],  # FIXMEwe
            "vector_name": vector_name,
            "raster_name": raster_name,
            "vector_reader": vector_reader,
            "vector_method": vector_method,
            "raster_reader": raster_reader,
            "raster_method": raster_method,
            "query": "{{query}}",
        }
        rendered = template.render(**payload)
        return rendered

    def save_template(self, template):
        with self.controller_ingest_template_path.open("w") as f:
            f.write(template)

    @staticmethod
    def get_vector_reader_from_filetype(filetype: VectorFileType) -> (str, str):
        match filetype:
            case VectorFileType.SHP:
                return "ShapefileReader", "readToGeometryRDD"
            case VectorFileType.GEOJSON:
                return "GeoJsonReader", "readToGeometryRDD"
            case VectorFileType.WKT:
                return "WktReader", "readToGeometryRDD"
            case VectorFileType.WKB:
                return "WkbReader", "readToGeometryRDD"
            case _:
                raise Exception(f"Cannot ingest Vector file with format {filetype}")

    @staticmethod
    def get_raster_reader_from_filetype(filetype: RasterFileType) -> (str, str):
        match filetype:
            case RasterFileType.TIFF:
                return None, "RS_FromGeoTiff"
            case _:
                raise Exception(f"Cannot ingest Raster file with format {filetype}")
