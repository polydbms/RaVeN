import re
from pathlib import Path

from jinja2 import Template

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
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.vector = vector_path
        self.raster = raster_path
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
        query = self.__translate(workload)
        query = query.replace("{self.table1}", self.vector.name)
        query = query.replace("{self.table2}", self.raster.name)
        print(f"query to run: {query}")

        rendered = self.__render_template(query)
        self.__save_template(rendered)
        self.transporter.send_file(
            Path("hub/deployment/files/sedona/sedona_prep.py"),
            self.host_base_path.joinpath("config/sedona/executor.py"),
            **kwargs
        )
        self.network_manager.run_ssh(str(self.host_base_path.joinpath("config/sedona/execute.sh")), **kwargs)
        Path("hub/deployment/files/sedona/sedona_prep.py").unlink()
        Path("hub/deployment/files/sedona/sedona_ingested.py.j2").unlink()

        result_path = self.network_manager.host_params.controller_result_folder.joinpath(
            f"results_{self.network_manager.measurements_loc.file_prepend}.{'cold' if warm_start_no == 0 else f'warm-{warm_start_no}'}.csv")
        result_file = self.host_base_path.joinpath("data/results/results_sedona.csv")
        self.transporter.get_file(
            result_file,
            result_path,
            **kwargs,
        )

        self.network_manager.run_remote_rm_file(result_file)

        return result_path

    def post_run_cleanup(self):
        pass

    def __read_template(self, path):
        try:
            with open(path) as file_:
                template = Template(file_.read())
                return template
        except FileNotFoundError:
            print(f"{path} not found")

    def __render_template(self, query):
        template_path = Path("hub/deployment/files/sedona/sedona_ingested.py.j2")
        template = self.__read_template(template_path)
        payload = {
            "query": query,
        }
        rendered = template.render(**payload)
        return rendered

    def __save_template(self, template):
        template_path = Path(f"hub/deployment/files/sedona/sedona_prep.py")
        with open(template_path, "w") as f:
            f.write(template)
