import re
from pathlib import Path
from hub.utils.preprocess import FileTransporter
from hub.evaluation.main import measure_time


class Executor:
    def __init__(self, vector_path, raster_path, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][0]
        self.table1 = f"{vector_path.stem}".split(".")[0].lower()
        self.table2 = f"{raster_path.stem}".split(".")[0].lower()

    def __handle_aggregations(self, type, features):
        return ", ".join(
            [
                f"{aggregation}({type}.{feature}) as {feature}_{aggregation}"
                for feature in features
                for aggregation in features[feature]["aggregations"]
            ]
        )

    def __parse_get(self, get):
        vector = []
        raster = []
        if "vector" in get:
            for feature in get["vector"]:
                if isinstance(feature, dict):
                    vector.append(self.__handle_aggregations("vector", feature))
                else:
                    vector.append(f"vector.{feature}")
            vector = ", ".join(vector)
        else:
            vector = ""
        if "raster" in get:
            for feature in get["raster"]:
                if isinstance(feature, dict):
                    raster.append(self.__handle_aggregations("raster", feature))
                else:
                    raster.append(f"raster.{feature}")
            raster = ", ".join(raster)
        else:
            raster = ""
        raster = f", {raster}" if raster else ""
        return f"select {vector} {raster}"

    def __parse_join(self, join):
        table1 = "{self.table1}" + f' as {join["table1"]}'
        table2 = "{self.table2}" + f' as {join["table2"]}'
        condition = f'on {join["condition"]}'
        return f"from {table1} JOIN {table2} {condition}"

    def __parse_condition(self, condition):
        vector = (
            "and ".join(["vector." + feature for feature in condition["vector"]])
            if "vector" in condition
            else ""
        )
        raster = (
            "and ".join(["raster." + feature for feature in condition["raster"]])
            if "raster" in condition
            else ""
        )
        raster = f"and {raster}" if raster else ""
        return f"where {vector} {raster}"

    def __parse_group(self, group):
        vector = (
            ", ".join(["vector." + feature for feature in group["vector"]])
            if "vector" in group
            else ""
        )
        raster = (
            ", ".join(["raster." + feature for feature in group["raster"]])
            if "raster" in group
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"group by {vector} {raster}"

    def __parse_order(self, order):
        vector = (
            ", ".join(["vector." + feature for feature in order["vector"]])
            if "vector" in order
            else ""
        )
        raster = (
            ", ".join(["raster." + feature for feature in order["raster"]])
            if "raster" in order
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"order by {vector} {raster}"

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
    def run_query(self, workload, **kwargs):
        query = self.__translate(workload)
        query = query.replace("{self.table1}", self.table1)
        query = query.replace("{self.table2}", self.table2)
        query = f"\copy ({query}) To '/data/results.csv' CSV HEADER;"
        with open("query.sql", "w") as f:
            f.write(query)
        self.transporter.send_file("query.sql", "~/data/query.sql", **kwargs)
        self.network_manager.run_ssh("~/config/execute.sh", **kwargs)
        Path("query.sql").unlink()
        self.transporter.get_file(
            "~/data/results.csv",
            f"~/results_{self.network_manager.system}.csv",
            **kwargs,
        )
