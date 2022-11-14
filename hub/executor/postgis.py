import re
from datetime import datetime
from pathlib import Path

from hub.executor._sqlbased import SQLBased
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.evaluation.measure_time import measure_time
from hub.utils.network import NetworkManager


class Executor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        self.table_vector = Path(vector_path.docker_file).stem
        self.table_raster = Path(raster_path.docker_file).stem
        self.host_base_path = network_manager.system_full.host_base_path

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
        if "intersect" in query:
            query = re.sub(
                "(intersect\(\w*, \w*\))",
                "ST_Intersects(raster.rast, vector.geom), ST_ValueCount(st_clip(raster.rast, vector.geom), 1) as pvc",
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
                "ST_Intersects(raster.rast, vector.geom)",
                query,
            )
            query = re.sub(
                "(raster.sval)",
                "ST_Value(raster.rast, vector.geom, true)",
                query,
            )
        return query

    @measure_time
    def run_query(self, workload, **kwargs) -> Path:
        query = self.__translate(workload)
        query = query.replace("{self.table1}", self.table_vector)
        query = query.replace("{self.table2}", self.table_raster)
        results_path_host = self.host_base_path.joinpath(f'data/results_postgis.csv')
        query = f"\copy ({query}) To '/data/results_postgis.csv' CSV HEADER;"
        with open("query.sql", "w") as f:
            f.write(query)
        self.transporter.send_file(Path("query.sql"), self.host_base_path.joinpath("data/query.sql"), **kwargs)
        self.network_manager.run_ssh(self.host_base_path.joinpath("config/postgis/execute.sh"), **kwargs)
        Path("query.sql").unlink()

        result_path = self.network_manager.system_full.controller_result_folder.joinpath(
            f"results_{self.network_manager.file_prepend}.csv")
        self.transporter.get_file(
            results_path_host,
            result_path,
            **kwargs,
        )

        self.transporter.get_folder(self.network_manager.host_measurements_folder,
                                    self.network_manager.controller_measurements_folder)

        return result_path
