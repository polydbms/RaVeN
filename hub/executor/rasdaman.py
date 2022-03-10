import urllib.parse as parser
from pathlib import Path
import requests
import operator
import re
import json
import csv
import os
from hub.evaluation.main import measure_time
from hub.utils.preprocess import FileTransporter


class Executor:
    def __init__(self, vector_path, raster_path, configurator) -> None:
        self.logger = {}
        self.configurator = configurator
        self.transporter = FileTransporter(configurator)
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
                0
            ]
        self.transporter.get_file(
            f"~/data/{self.vector_path.stem}.json", f"{self.vector_path.stem}.json"
        )
        ssh_ip = self.configurator.ssh_connection.split("@")[-1]
        self.url = f"http://{ssh_ip}:8080/rasdaman/ows"
        self.coverage = f"{Path(self.raster_path).stem}".split(".")[0]
        wkt = Path(f"{Path(self.vector_path).stem}.json").read_bytes()
        self.vector_data = json.loads(wkt)
        self.aggregations = {
            "avg": self.__get_avg,
            "sum": self.__get_sum,
            "max": self.__get_max,
            "min": self.__get_min,
            "count": self.__get_count,
        }
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}

    def __get_avg(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return add(clip( c,{geometry}))/count(clip( c,{geometry}) > 0)"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_sum(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return add(clip( c,{geometry}))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_max(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return max(clip( c,{geometry}))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_min(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return min(clip( c,{geometry}))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_count(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return count(clip( c,{geometry}) > 0)"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_point_value(self, properties, **kwargs):
        if "selection" in kwargs:
            selection = kwargs["selection"]
        geometry = properties["wkt"]
        vector_feature = [properties[feature] for feature in selection[0]]
        search = re.search("\((\W*\d*\W*\d*) (\W*\d*\W*\d*)\)", geometry)
        result = vector_feature
        if "point" in geometry.lower():
            lat = search.group(1)
            long = search.group(2)
            url = (
                self.url + "?SERVICE=WCS"
                "&VERSION=2.0.1"
                "&REQUEST=GetCoverage"
                f"&COVERAGEID={self.coverage}"
                f"&SUBSET=Lat({lat})"
                f"&SUBSET=Long({long})"
                "&FORMAT=application/json"
            )
            response = requests.request("GET", url, headers=self.headers)
            result.append("error" if "xml" in response.text else response.text)
        else:
            result.append("No poit")
            print("Geometry contains no points")
        yield result

    def __do_aggregation(self, properties, **kwargs):
        if "selection" in kwargs:
            selection = kwargs["selection"]
        geometry = properties["wkt"]
        vector_feature = [properties[feature] for feature in selection[0]]
        aggregations = [
            ras_feature
            for ras_feature in selection[1]
            if ras_feature in self.aggregations
        ]
        result = vector_feature
        for aggregation in aggregations:
            parsed_payload = self.aggregations[aggregation](geometry)
            response = requests.request(
                "POST", self.url, headers=self.headers, data=parsed_payload
            )
            result.append("error" if "xml" in response.text else response.text)
        yield result

    def __handle_aggregations(self, features):
        return [
            aggregation
            for feature in features
            for aggregation in features[feature]["aggregations"]
        ]

    def __parse_get(self, get):
        vector = []
        raster = []
        if "vector" in get:
            for feature in get["vector"]:
                if isinstance(feature, dict):
                    vector = vector + self.__handle_aggregations(feature)
                else:
                    vector.append(feature)

        if "raster" in get:
            for feature in get["raster"]:
                if isinstance(feature, dict):
                    raster = raster + self.__handle_aggregations(feature)
                else:
                    raster.append(feature)
        return vector, raster

    def __parse_condition(self, condition):
        vector = (
            {
                feature.split(" ")[0]: [
                    " ".join(feature.split(" ")[1:-1]),
                    feature.split(" ")[-1].replace("'", ""),
                ]
                for feature in condition["vector"]
            }
            if "vector" in condition
            else {}
        )
        for k, v in vector.items():
            if "=" in v[0]:
                v.insert(1, operator.__eq__)
            if "is not" in v[0]:
                v.insert(1, operator.__ne__)
            if "null" in v[-1]:
                v[-1] = None
        raster = (
            {
                feature.split(" ")[0]: [
                    " ".join(feature.split(" ")[1:-1]),
                    feature.split(" ")[-1],
                ]
                for feature in condition["raster"]
            }
            if "raster" in condition
            else {}
        )
        for k, v in raster.items():
            if "=" in v[0]:
                v.insert(1, operator.__eq__)
            if "is not" in v[0]:
                v.insert(1, operator.__ne__)
            if "null" in v[-1]:
                v[-1] = None
        return vector, raster

    def __parse_order(self, order):
        vector = [feature for feature in order["vector"]] if "vector" in order else []
        raster = [feature for feature in order["raster"]] if "raster" in order else []
        return vector, raster

    def __translate(self, workload):
        selection = self.__parse_get(workload["get"]) if "get" in workload else ""
        condition = (
            self.__parse_condition(workload["condition"])
            if "condition" in workload
            else ""
        )
        order = self.__parse_order(workload["order"]) if "order" in workload else ""
        limit = workload["limit"] if "limit" in workload else ""
        has_aggregations = (
            len(
                [
                    ras_feature
                    for ras_feature in selection[1]
                    if ras_feature in self.aggregations
                ]
            )
            > 0
        )
        return selection, condition, order, limit, has_aggregations

    @measure_time
    def run_query(self, workload, **kwargs):
        selection, condition, order, limit, has_aggregations = self.__translate(
            workload
        )
        header = selection[0] + selection[1]
        operation = self.__get_point_value
        if has_aggregations:
            operation = self.__do_aggregation

        vector_features = [
            entry
            for entry in self.vector_data["features"]
            if all(
                [
                    value[1](entry["properties"][key], value[-1])
                    for key, value in condition[0].items()
                ]
            )
        ]
        for o in order[0]:
            vector_features.sort(key=lambda x: x["properties"][o])

        if limit:
            vector_features = vector_features[: int(limit)]
        f = open(
            f"{os.environ['HOME']}/results_{self.configurator.system}.csv",
            "w",
            encoding="UTF8",
            newline="",
        )
        writer = csv.writer(f)
        writer.writerow(header)
        for entry in vector_features:
            writer.writerow(
                next(
                    operation(
                        properties=entry["properties"],
                        selection=selection,
                        condition=condition,
                        limit=limit,
                        order=order,
                    )
                )
            )
        f.close()
        Path(f"{self.vector_path.stem}.json").unlink()
