import re
from pathlib import Path

from jinja2 import Template

from hub.evaluation.measure_time import measure_time
from hub.executor._sqlbased import SQLBased
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Executor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.table_vector = vector_path.name
        self.table_raster = raster_path.name

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
                "ST_Intersects(raster.geometry, vector.geometry)",
                query,
            )
        if "contains" in query:
            query = re.sub(
                "(contains\(\w*, \w*\))",
                "ST_Contains(raster.geometry, vector.geometry)",
                query,
            )
        query = re.sub(
            "(raster.sval)",
            "raster.values",
            query,
        )

        return query

    @measure_time
    def run_query(self, workload, **kwargs):
        query = self.__translate(workload)
        query = query.replace("{self.table1}", self.table_vector)
        query = query.replace("{self.table2}", self.table_raster)
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
            f"results_{self.network_manager.measurements_loc.file_prepend}.csv")
        result_file = self.host_base_path.joinpath("data/results/results_sedona.csv")
        self.transporter.get_file(
            result_file,
            result_path,
            **kwargs,
        )

        self.network_manager.run_remote_rm_file(result_file)

        self.transporter.get_folder(self.network_manager.measurements_loc.host_measurements_folder,
                                    self.network_manager.measurements_loc.controller_measurements_folder)

        return result_path

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
