import concurrent
import csv
import json
import operator
import re
import urllib.parse as parser
from concurrent.futures import ThreadPoolExecutor

import geopandas as gpd
import requests

from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Executor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        self.vector_path = vector_path
        self.raster_path = raster_path
        socks_proxy_url = self.network_manager.open_socks_proxy()
        self.proxies = dict(http=socks_proxy_url, https=socks_proxy_url)
        self.transporter.get_file(
            vector_path.host_wkt, vector_path.controller_wkt
        )
        self.base_url = f"http://0.0.0.0:48080/rasdaman"
        self.url = f"{self.base_url}/ows"
        self.coverage = self.raster_path.name
        wkt = self.vector_path.controller_wkt.read_bytes()
        self.vector_data = json.loads(wkt)
        self.aggregations = {
            "avg": self.__get_avg,
            "sum": self.__get_sum,
            "max": self.__get_max,
            "min": self.__get_min,
            "count": self.__get_count,
        }
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.crs = gpd.read_file(self.vector_path.controller_file).crs.to_epsg()
        self.crs_url = f"{self.base_url}/def/crs/EPSG/0/{self.crs}"

    def __get_avg(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return add(clip( c,{geometry}, \"{self.crs_url}\"))/count(clip( c,{geometry}, \"{self.crs_url}\") > 0)"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_sum(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return add(clip( c,{geometry}, \"{self.crs_url}\"))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_max(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return max(clip( c,{geometry}, \"{self.crs_url}\"))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_min(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return min(clip( c,{geometry}, \"{self.crs_url}\"))"
        }
        return parser.urlencode(payload, quote_via=parser.quote)

    def __get_count(self, geometry):
        payload = {
            "query": f"for c in ({self.coverage}) return count(clip( c,{geometry}, \"{self.crs_url}\") > 0)"
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
            self.network_manager.write_timings_marker(f"benchi_marker,,start,execution,rasdaman,points,")
            response = requests.request("GET", url, headers=self.headers, proxies=self.proxies)
            self.network_manager.write_timings_marker(f"benchi_marker,,end,execution,rasdaman,points,")
            result.append("0" if "xml" in response.text else response.text)
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
            # print(parsed_payload)
            response = requests.request(
                "POST", self.url, headers=self.headers, data=parsed_payload, proxies=self.proxies
            )
            # print(response.request.url, response.request.headers, parsed_payload, sep=";")
            result.append("0" if "xml" in response.text else response.text)

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
        self.network_manager.run_ssh("""echo "benchi_marker,$(date +%s.%N),now,time_diff_check,rasdaman,," """)

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

        result_path = self.network_manager.host_params.controller_result_folder.joinpath(
            f"results_{self.network_manager.measurements_loc.file_prepend}.csv")

        if limit:
            vector_features = vector_features[: int(limit)]
        f = open(
            result_path,
            "w",
            encoding="UTF8",
            newline="",
        )
        writer = csv.writer(f)
        writer.writerow(header)
        self.network_manager.write_timings_marker(f"benchi_marker,,start,execution,rasdaman,aggregations,")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = {executor.submit(operation,
                                       properties=feature["properties"],
                                       selection=selection,
                                       condition=condition,
                                       limit=limit,
                                       order=order,
                                       ): feature for feature in vector_features}

            for future in concurrent.futures.as_completed(results):
                try:
                    writer.writerow(next(future.result()))
                except Exception as exc:
                    print(f"error while fetching result: {exc}")

        self.network_manager.write_timings_marker(f"benchi_marker,,end,execution,rasdaman,aggregations,")
        f.close()
        self.vector_path.controller_wkt.unlink()
        self.network_manager.stop_socks_proxy()

        self.transporter.get_folder(self.network_manager.measurements_loc.host_measurements_folder,
                                    self.network_manager.measurements_loc.controller_measurements_folder)

        return result_path
