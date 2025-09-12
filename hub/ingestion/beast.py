import json
import logging
import re
import subprocess
from pathlib import Path

from click import command
from jinja2 import Template

from hub.enums.stage import Stage
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.configuration import PROJECT_ROOT
from hub.enums.vectorfiletype import VectorFileType
from hub.evaluation.measure_time import measure_time
from hub.executor.sqlbased import SQLBased
from hub.utils.datalocation import VectorLocation, RasterLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.interfaces import IngestionInterface
from hub.utils.network import NetworkManager
from hub.utils.query import extent_to_geom


# import geopandas as gpd


class Ingestor(IngestionInterface):
    def __init__(self, vector_path: VectorLocation, raster_path: RasterLocation, network_manager: NetworkManager,
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
        self.raptor_scala_path = PROJECT_ROOT.joinpath(
            f"deployment/files/beast/scala-beast/src/main/scala/benchi/RaptorScala.scala")

        rendered = self.render_template()
        self.__save_template(rendered)

        file_transporter = FileTransporter(self.network_manager)
        file_transporter.send_configs()

        command = self.host_base_path.joinpath(f"config/beast/compile.sh "
                                               f"-v={self.vector.docker_dir_preprocessed} "
                                               f"-r={self.raster.docker_dir_preprocessed} ")

        self.network_manager.run_ssh(command)

    @staticmethod
    def __handle_aggregations(type, features):
        feature_key, feature = list(features.items())[0]
        return ", ".join(
            [
                f"{aggregation}(raptorjoined.{feature_key}) as {feature_key}_{aggregation}"
                for aggregation in feature["aggregations"]
            ]
        )

    def __parse_get(self, get):
        return SQLBased.parse_get(self.__handle_aggregations, get, vector_table_name="raptorjoined",
                                  raster_table_name="raptorjoined")

    def __parse_condition(self, condition):
        return SQLBased.parse_condition(condition, vector_table_name="raptorjoined", raster_table_name="raptorjoined")

    def __parse_group(self, group):
        return SQLBased.parse_group(group, vector_table_name="raptorjoined", raster_table_name="raptorjoined")

    def __parse_order(self, order):
        return SQLBased.parse_order(order, vector_table_name="raptorjoined", raster_table_name="raptorjoined")

    def __parse_extent(self, extent):
        geom = extent_to_geom(extent, self.benchmark_params)
        return str(geom.wkt)



    def __translate(self, workload):
        selection = self.__parse_get(workload["get"]) if "get" in workload else ""
        group = self.__parse_group(workload["group"]) if "group" in workload else ""
        order = self.__parse_order(workload["order"]) if "order" in workload else ""
        limit = f'limit {workload["limit"]}' if "limit" in workload else ""
        query = f"{selection} from df_raw as raptorjoined {group} {order} {limit}"

        return query

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
            PROJECT_ROOT.joinpath("deployment/files/beast/RaptorScala.scala.j2"))
        logging.info(template_path)
        template = self.__read_template(template_path)

        raster_conditions = list(
            map(lambda c: {"condition": self.__parse_condition_scala(c, "Number")},
                self.workload.get("condition", {}).get("raster", [])))

        def __parse_vector_cond(condition):
            field = re.search("([^<>!=]*)", condition).group(1).strip()
            datatype = self.__dtype_to_scala_vector(field)
            cond = self.__parse_condition_scala(condition, datatype)

            return {
                "datatype": datatype,
                "field": field,
                "condition": cond
            }

        def __build_condition(condition, operator):
            if isinstance(condition, str):
                c = __parse_vector_cond(condition)
                return f'''f.getAs[{c['datatype']}]("{c['field']}") {c['condition']}'''
            if isinstance(condition, list):
                result = ""
                for idx, c in enumerate(condition):
                    match operator:
                        case "and":
                            beast_op = "&&"
                        case "or":
                            beast_op = "||"
                        case _:
                            raise ValueError(f"Operator {operator} is not supported")
                    result += f"{__build_condition(c, beast_op)} {beast_op if idx + 1 < len(condition) else ''} "
                return f"{result}"
            if isinstance(condition, dict):
                if len(condition) > 1:
                    raise ValueError("Only one condition is allowed")
                for o, c in condition.items():
                    return f"({__build_condition(c, o)})"

        vector_condition = __build_condition(self.workload.get("condition", {}).get("vector", []), "and")

        raster_type = self.__dtype_to_scala_raster()
        # raster_field = re.search("([^<>!=]*)",
        #                          list(
        #                              next(
        #                                  filter(lambda v: type(v) is dict, self.workload["get"]["raster"])
        #                              ).keys())[0]).group(1).strip()
        raster_field = "sval"


        sql_query = self.__translate(self.workload)

        extent = self.__parse_extent(self.workload.get("extent")) if self.workload.get("extent") else None

        print(f"query to run: {sql_query}")

        tile_size = self.benchmark_params.raster_tile_size.__dict__ if self.benchmark_params.raster_tile_size.width > 0 else None

        vector_path = self.vector.docker_dir if self.benchmark_params.vector_target_format == VectorFileType.SHP else self.vector.docker_file_preprocessed[0]
        raster_path = self.raster.docker_file_preprocessed[0].with_suffix(".geotiff")
        output_path = "/data/beast_result"

        if self.benchmark_params.parallel_machines > 1:
            vector_path = f"hdfs://namenode:9000" + str(vector_path)
            raster_path = f"hdfs://namenode:9000" + str(raster_path)
            output_path = f"hdfs://namenode:9000" + str(output_path)

        payload = {
            "spark_master": "local[*]" if self.benchmark_params.parallel_machines == 1 else f"spark://beast:7077",
            "vector_path": vector_path,
            "raster_geotiff_path": raster_path,
            "vector_conditions": vector_condition,
            "raster_conditions": raster_conditions,
            "extent": extent if extent and self.benchmark_params.vector_filter_at_stage == Stage.EXECUTION else None,
            "get": {
                "field": self.workload["get"]["vector"][0],
                "datatype": self.__dtype_to_scala_vector(self.workload["get"]["vector"][0])
            },
            "raster": {
                "field": raster_field,
                "datatype": raster_type
            },
            "raster_tile": tile_size,
            "sql_query": sql_query,
            "output_path": output_path
        }

        print(f"Payload: {payload}")

        rendered = template.render(**payload)
        return rendered

    def __save_template(self, template):
        template_path = self.raptor_scala_path
        with open(template_path, "w") as f:
            f.write(template)

    @staticmethod
    def __parse_condition_scala(condition, dtype):
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

    def __dtype_to_scala_vector(self, name):
        vectortypes = json.loads(
            subprocess.check_output(f"ogrinfo -nocount -json -nomd {self.vector.controller_file[0]}", shell=True).decode(
                "utf-8"))["layers"][0]["fields"]
        fieldtype = next(filter(lambda c: c["name"] == name, vectortypes))["type"]
        # vector_dtypes = gpd.read_file(self.vector.controller_file, rows=1).dtypes
        match fieldtype:
            case "String":
                return "String"
            case "Real":
                precision = next(filter(lambda c: c["name"] == name, vectortypes)).get("precision", 0)
                return "Float" if precision < 7 else "Double"
            case "Integer64":
                return "Long"
            case "Integer":
                return "Int"
            case "Boolean":
                return "Boolean"
            case _:
                raise Exception(f"type {fieldtype} has not been implemented yet")

    def __dtype_to_scala_raster(self):
        rastertypes = \
        json.loads(subprocess.check_output(f'gdalinfo -json -nomd -norat -noct -nogcp {self.raster.controller_file[0]}', # Fixme?
                                           shell=True).decode("utf-8"))["bands"][0]["type"]

        match rastertypes:
            case "Byte" | "Int8" | "UInt16" | "Int16" | "Int32":
                return "Int"
            case "UInt32" | "UInt64" | "Int64":
                return "Long"
            case "Float64":
                return "Double"
            case "Float32":
                return "Float"
            case _:
                raise Exception(f"type {rastertypes} has not been implemented yet")
