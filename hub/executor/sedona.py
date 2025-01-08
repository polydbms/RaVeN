import re
from pathlib import Path

from jinja2 import Template

from hub.configuration import PROJECT_ROOT
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.stage import Stage
from hub.evaluation.measure_time import measure_time
from hub.executor.sqlbased import SQLBased
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

        self.controller_ingest_template_path = PROJECT_ROOT.joinpath("deployment/files/sedona/sedona_ingested.py.j2")
        self.controller_query_path = PROJECT_ROOT.joinpath("deployment/files/sedona/sedona_prep.py")

    def __handle_aggregations(self, type, features):
        return ", ".join(
            [
                f"RS_ZonalStats(raster.rast, vector.geometry, '{aggregation}') as {feature}_{aggregation}"
                for feature in features
                for aggregation in features[feature]["aggregations"]
            ]
        )

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
        query = f"{selection} {join} {condition} {order} {limit}" # for sedona zs you don't have to group by…

        raster_rast = "raster.rast"
        vector_geom = "vector.geometry"

        if "intersect" in query:
            query = re.sub(
                "(intersect\(\w*, \w*\))",
                f"RS_Intersects({vector_geom}, {raster_rast})",
                query,
            )

        return query

    @measure_time
    def run_query(self, workload, warm_start_no: int, **kwargs):
        query = self.__translate(workload)
        query = query.replace("{self.table_vec}", self.vector.name)
        query = query.replace("{self.table_ras}", self.raster.name)
        print(f"query to run: {query}")

        rendered = self.render_template(query)
        self.save_template(rendered)
        self.transporter.send_file(
            self.controller_query_path,
            self.host_base_path.joinpath("config/sedona/executor.py"),
            **kwargs
        )
        self.network_manager.run_query_ssh(str(self.host_base_path.joinpath("config/sedona/execute.sh")), **kwargs)

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
        self.controller_ingest_template_path.unlink()
        self.controller_query_path.unlink()

    def read_template(self, path):
        try:
            with open(path) as file_:
                template = Template(file_.read())
                return template
        except FileNotFoundError:
            print(f"{path} not found")

    def render_template(self, query):
        template = self.read_template(self.controller_ingest_template_path)
        payload = {
            "query": query,
        }
        rendered = template.render(**payload)
        return rendered

    def save_template(self, template):
        with self.controller_query_path.open("w") as f:
            f.write(template)
