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
        template_path = PROJECT_ROOT.joinpath("deployment/files/sedona/sedona.py.j2")
        template = self.__read_template(template_path)
        raster_name = self.raster.name
        vector_name = self.vector.name
        vector_reader, vector_method = self._get_vector_reader_from_filetype(self.benchmark_params.vector_target_format)
        raster_reader, raster_method = self._get_raster_reader_from_filetype(self.benchmark_params.raster_target_format)

        payload = {
            "vector_path": self.vector.docker_dir if self.benchmark_params.vector_target_format == VectorFileType.SHP else self.vector.docker_file_preprocessed,
            "raster_path": self.raster.docker_dir if self.benchmark_params.raster_target_format == VectorFileType.SHP else self.raster.docker_file_preprocessed,
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

    def __save_template(self, template):
        template_path = PROJECT_ROOT.joinpath(f"deployment/files/sedona/sedona_ingested.py.j2")
        with open(template_path, "w") as f:
            f.write(template)

    @staticmethod
    def _get_vector_reader_from_filetype(filetype: VectorFileType) -> (str, str):
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
    def _get_raster_reader_from_filetype(filetype: RasterFileType) -> (str, str):
        match filetype:
            case RasterFileType.TIFF:
                return None, "RS_FromGeoTiff"
            case _:
                raise Exception(f"Cannot ingest Raster file with format {filetype}")
