import re
import subprocess
from pathlib import Path

from configuration import PROJECT_ROOT
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.vectorfiletype import VectorFileType
from hub.evaluation.measure_time import measure_time
from jinja2 import Template

from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager
import geopandas as gpd


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters, workload=None) -> None:
        if workload is None:
            workload = dict()
        self.workload = workload
        self.logger = {}
        self.network_manager = network_manager
        self.vector = vector_path
        self.raster = raster_path
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params
        self.raptor_scala_path = Path(f"hub/deployment/files/beast/scala-beast/src/main/scala/benchi/RaptorScala.scala")

        rendered = self.render_template()
        self.__save_template(rendered)

        file_transporter = FileTransporter(self.network_manager)
        file_transporter.send_configs()

        self.network_manager.run_ssh(str(self.host_base_path.joinpath("config/beast/compile.sh")))

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
        template_path = Path(
            PROJECT_ROOT.joinpath("hub/deployment/files/beast/RaptorScala.scala.j2"))
        template = self.__read_template(template_path)

        raster_conditions = list(
            map(lambda c: {"condition": self.__parse_condition(c, "Number")},
                self.workload["condition"].get("raster", [])))

        def __parse_vector_cond(condition):
            field = re.search("([^<>!=]*)", condition).group(1).strip()
            datatype = self.__dtype_to_scala(field)
            cond = self.__parse_condition(condition, datatype)

            return {
                "datatype": datatype,
                "field": field,
                "condition": cond
            }

        vector_conditions = list(map(__parse_vector_cond, self.workload["condition"].get("vector", [])))

        raster_type_raw = subprocess.run(f'gdalinfo {self.raster.controller_file} | grep -Po "(?<=Type=)(\w+)"',
                                         shell=True, capture_output=True).stdout.decode("utf-8").strip()
        raster_type = "Float" if "Float" in raster_type_raw else "Int"
        raster_field = re.search("([^<>!=]*)", list(self.workload["get"]["raster"][0].keys())[0]).group(1).strip()

        aggregations = list(self.workload["get"]["raster"][0].values())[0]["aggregations"]
        payload = {
            "vector_path": self.vector.docker_dir if self.benchmark_params.vector_target_format == VectorFileType.SHP else self.vector.docker_file_preprocessed,
            "raster_geotiff_path": self.raster.docker_file_preprocessed.with_suffix(".geotiff"),
            "vector_conditions": vector_conditions,
            "raster_conditions": raster_conditions,
            "get": {
                "field": self.workload["get"]["vector"][0],
                "datatype": self.__dtype_to_scala(self.workload["get"]["vector"][0])
            },
            "raster": {
                "field": raster_field,
                "datatype": raster_type
            },
            "aggregate": {
                "avg": "avg" in aggregations,
                "min": "min" in aggregations,
                "max": "max" in aggregations,
                "cnt": "count" in aggregations,
                "sum": "sum" in aggregations
            }
        }
        rendered = template.render(**payload)
        return rendered

    def __save_template(self, template):
        template_path = self.raptor_scala_path
        with open(template_path, "w") as f:
            f.write(template)

    @staticmethod
    def __parse_condition(condition, dtype):
        m = re.search("([^<>!=]*)([<>!=]+)(.*)", condition)
        op = "==" if m.group(2).strip() == "=" else m.group(2).strip()
        if dtype == "String":
            if any(s in m.group(3) for s in ["\"", "'"]):  # fixme this does not correctly detect escaped strings
                target = f'"{m.group(3).strip()[1:-1]}"'
            else:
                target = f'"{m.group(3).strip()}"'
        else:
            target = m.group(3).strip()
        return f"{op} {target}"

    def __dtype_to_scala(self, name):
        vector_dtypes = gpd.read_file(self.vector.controller_file, rows=1).dtypes
        match vector_dtypes[name].name:
            case "object":
                return "String"
            case "float64":
                return "Float"
            case "int64":
                return "Int"
            case "bool":
                return "Boolean"
            case _:
                raise Exception(f"dtype {vector_dtypes[name].name} has not been implemented yet")
