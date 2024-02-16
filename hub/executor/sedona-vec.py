import re

from configuration import PROJECT_ROOT
from hub.enums.datatype import DataType
from hub.enums.stage import Stage
from hub.executor._sqlbased import SQLBased
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.executor.sedona import Executor as SedonaExecutor
from hub.utils.datalocation import DataLocation
from hub.utils.network import NetworkManager


class Executor(SedonaExecutor):

    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        super().__init__(vector_path, raster_path, network_manager, benchmark_params)
        self.controller_ingest_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona-vec/sedona_ingested.py.j2")
        self.controller_query_path = PROJECT_ROOT.joinpath("deployment/files/sedona-vec/sedona_prep.py")

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

        raster_geom = "raster.geometry"
        vector_geom = "vector.geometry"
        if self.benchmark_params.align_crs_at_stage == Stage.EXECUTION:
            match self.benchmark_params.align_to_crs:
                case DataType.RASTER:
                    vector_geom = f"ST_Transform(" \
                                  f"{vector_geom}, " \
                                  f"'epsg:{self.vector.get_crs().to_epsg()}', " \
                                  f"'epsg:{self.benchmark_params.vector_target_crs.to_epsg()}'" \
                                  f")"
                case DataType.VECTOR:
                    raster_geom = f"ST_Transform(" \
                                  f"{raster_geom}, " \
                                  f"'epsg:{self.raster.get_crs().to_epsg()}', " \
                                  f"'epsg:{self.benchmark_params.raster_target_crs.to_epsg()}'" \
                                  f")"

        if "intersect" in query:
            query = re.sub(
                "(intersect\(\w*, \w*\))",
                f"ST_Intersects({raster_geom}, {vector_geom})",
                query,
            )
        if "contains" in query:
            query = re.sub(
                "(contains\(\w*, \w*\))",
                f"ST_Contains({raster_geom}, {vector_geom})",
                query,
            )
        if "bestrasvecjoin" in query:
            query = re.sub(
                "(bestrasvecjoin\(\w*, \w*\))",
                f"ST_Within({raster_geom}, {vector_geom}) "
                f"OR ST_Contains({raster_geom}, {vector_geom}) "
                f"OR ST_Crosses({raster_geom}, {vector_geom}) "
                f"OR ST_Overlaps({raster_geom}, {vector_geom})",
                query,
            )

        query = re.sub(
            "(raster.sval)",
            "raster.values",
            query,
        )

        return query

    @measure_time
    def run_query(self, workload, warm_start_no: int, **kwargs):
        return super().run_query(workload, warm_start_no, **kwargs)

    def post_run_cleanup(self):
        super().post_run_cleanup()

    def read_template(self, path):
        return super().read_template(path)

    def render_template(self, query):
        return super().render_template(query)

    def save_template(self, template):
        super().save_template(template)
