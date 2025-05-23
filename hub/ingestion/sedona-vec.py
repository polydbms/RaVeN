from pathlib import Path

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.configuration import PROJECT_ROOT
from hub.enums.vectorfiletype import VectorFileType
from hub.ingestion.sedona import Ingestor as SedonaIngestor
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Ingestor(SedonaIngestor):
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        super().__init__(vector_path, raster_path, network_manager, benchmark_params, workload)

        self.controller_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona-vec/sedona.py.j2")
        self.controller_ingest_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona-vec/sedona_ingested.py.j2")

    def ingest_raster(self, **kwargs):
        super().ingest_raster(**kwargs)

    def ingest_vector(self, **kwargs):
        super().ingest_vector(**kwargs)

    def read_template(self, path):
        return super().read_template(path)

    def render_template(self):
        template = self.read_template(self.controller_template_path)
        raster_name = self.raster.name
        vector_name = self.vector.name
        vector_reader, vector_method = super().get_vector_reader_from_filetype(self.benchmark_params.vector_target_format)
        raster_reader, raster_method = super().get_vector_reader_from_filetype(self.benchmark_params.raster_target_format)

        payload = {
            "vector_path": self.vector.docker_dir if self.benchmark_params.vector_target_format == VectorFileType.SHP else self.vector.docker_file_preprocessed[0], # FIXME
            "raster_path": self.raster.docker_dir if self.benchmark_params.raster_target_format == VectorFileType.SHP else self.raster.docker_file_preprocessed[0], # FIXME
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

    def save_template(self, rendered):
        return super().save_template(rendered)