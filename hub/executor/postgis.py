import re
from pathlib import Path

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.stage import Stage
from hub.evaluation.measure_time import measure_time
from hub.executor._sqlbased import SQLBased
from hub.utils.datalocation import DataLocation
from hub.enums.datatype import DataType
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Executor:
    def __init__(self, vector_path: DataLocation,
                 raster_path: DataLocation,
                 network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        self.table_vector = vector_path.name
        self.table_raster = raster_path.name
        self.host_base_path = network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params

    def __handle_aggregations(self, type, features):
        return SQLBased.handle_aggregations(type, features)

    def __parse_get(self, get):
        return SQLBased.parse_get(self.__handle_aggregations, get)

    def __parse_join(self, join):
        return SQLBased.parse_join(join)

    def __parse_condition(self, condition):
        return SQLBased.parse_condition(condition)

    def __parse_group(self, group):
        return SQLBased.parse_group(group)

    def __parse_order(self, order):
        return SQLBased.parse_order(order)

    def __translate(self, workload):
        selection = self.__parse_get(workload["get"]) if "get" in workload else ""
        join = self.__parse_join(workload["join"]) if "join" in workload else ""
        condition = (
            self.__parse_condition(workload["condition"])
            if "condition" in workload
            else ""
        )
        group = self.__parse_group(workload["group"]) if "group" in workload else ""
        order = self.__parse_order(workload["order"]) if "order" in workload else ""
        limit = f'limit {workload["limit"]}' if "limit" in workload else ""
        query = f"{selection} {join} {condition} {group} {order} {limit}"

        raster_geom = "raster.rast"
        vector_geom = "vector.geom"
        if self.benchmark_params.align_crs_at_stage == Stage.EXECUTION:
            match self.benchmark_params.align_to_crs:
                case DataType.RASTER:
                    vector_geom = f"ST_Transform({vector_geom}, {self.benchmark_params.vector_target_crs.to_epsg()})"
                case DataType.VECTOR:
                    raster_geom = f"ST_Transform({raster_geom}, {self.benchmark_params.raster_target_crs.to_epsg()})"

        if "intersect" in query:
            query = re.sub(
                "(intersect\(\w*, \w*\))",
                f"ST_Intersects({raster_geom}, {vector_geom}), ST_ValueCount(st_clip({raster_geom}, {vector_geom}), 1) as pvc",
                query,
            )
            query = re.sub(
                "(raster.sval)",
                "pvc.value",
                query,
            )
        if "contains" in query:
            query = re.sub(
                "(contains\(\w*, \w*\))",
                f"ST_Intersects({raster_geom}, {vector_geom})",
                query,
            )
            query = re.sub(
                "(raster.sval)",
                f"ST_Value({raster_geom}, {vector_geom}, true)",
                query,
            )
        return query

    @measure_time
    def run_query(self, workload, warm_start_no: int, **kwargs) -> Path:
        query = self.__translate(workload)
        query = query.replace("{self.table1}", self.table_vector)
        query = query.replace("{self.table2}", self.table_raster)
        print(f"query to run: {query}")

        relative_results_file = Path(f"data/results/{self.network_manager.measurements_loc.file_prepend}.{'cold' if warm_start_no == 0 else f'warm-{warm_start_no}'}.csv")
        results_path_host = self.host_base_path.joinpath(relative_results_file)
        query = f"""\copy ({query}) To '{Path("/").joinpath(relative_results_file)}' CSV HEADER;"""
        with open("query.sql", "w") as f:
            f.write(query)
        self.transporter.send_file(Path("query.sql"), self.host_base_path.joinpath("data/query.sql"), **kwargs)
        self.network_manager.run_ssh(self.host_base_path.joinpath("config/postgis/execute.sh"), **kwargs)
        Path("query.sql").unlink()

        result_path = self.network_manager.host_params.controller_result_folder.joinpath(
            f"results_{self.network_manager.measurements_loc.file_prepend}.csv")
        self.transporter.get_file(
            results_path_host,
            result_path,
            **kwargs,
        )

        self.network_manager.run_remote_rm_file(results_path_host)

        self.transporter.get_folder(self.network_manager.measurements_loc.host_measurements_folder,
                                    self.network_manager.measurements_loc.controller_measurements_folder)

        return result_path
