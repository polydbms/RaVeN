from __future__ import annotations

import re
import subprocess
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from duckdb import DuckDBPyConnection, connect
from pandas import DataFrame
from shapely.geometry.polygon import Polygon

from enums.datatype import DataType
from enums.stage import Stage
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from utils.datalocation import DataLocation, RasterLocation
from utils.system import System


class DuckDBConnector:
    """
    the wrapper class around duckdb that abstracts database calls
    """
    _connection: DuckDBPyConnection

    def __init__(self, db_filename: Path):
        """
        initializes the connection
        :param db_filename: the location of the database
        """
        self._connection = connect(database=str(db_filename), read_only=False)
        self._benchmark_set_id = -1
        self._is_initialized = False

        print(f"connected to database: {db_filename}")

    def get_cursor(self) -> DuckDBPyConnection:
        """
        returns a cursor of the database
        :return:
        """
        return self._connection.cursor()

    def get_id_of_parameter(self, param: BenchmarkParameters) -> int:
        """
        returns the id of a given benchmark parameter set
        :param param: the benchmark parameter object
        :return: the parameter id
        """
        if not self._is_initialized:
            raise Exception("the benchmark set has not been initialized yet")

        with self.get_cursor() as c:
            param_df = pd.DataFrame([param.__dict__]).fillna('')
            param_df_cleaned = c.execute("select * from param_df").fetch_df()
            all_params = c.execute("select * from parameters").fetch_df().set_index("id").drop_duplicates(keep="last")
            all_params_dupes = pd.concat([all_params, param_df_cleaned])
            param_db_row = all_params_dupes.duplicated(keep=False)[lambda x: x]

            return int(param_db_row.head(1).index[0])

    def initialize_resource_limits(self, resource_limits: dict) -> tuple[int, dict]:
        """
        inserts the resource limits into the database
        :param resource_limits: the resource limits
        :return:
        """

        resource_limits = dict(sorted(resource_limits.items(), key=lambda item: item[0]))

        limit_id = self._connection.execute("select * from resource_limit rl where rl.limits = ?", [resource_limits]).fetchone()

        if limit_id:
            print(f"resource limits already exist with id {limit_id}")
            return limit_id[0], resource_limits

        limit_id, limits = self._connection.execute("insert into resource_limit (limits) values (?) returning *",
                                                    [resource_limits]).fetchone()

        print(f"initialized resource limits")
        return limit_id, limits

    def initialize_benchmark_set(self, experiment: str, resource_limits: dict) -> int:
        """
        initializes a new benchmark set consisting of one or many benchmark runs
        :param experiment: the experiment name
        :return: the benchmark set id
        """
        if not self._is_initialized:
            with self.get_cursor() as conn:
                resource_limits_id, resource_limits = self.initialize_resource_limits(resource_limits)

                bench_set = conn.execute("insert into benchmark_set (experiment, resource_limits, exec_start) values (?, ?, ?) returning *",
                                         [experiment, resource_limits_id,datetime.now()]).fetchall()[0]

                print(f"created benchmark set: {bench_set}")
                self._benchmark_set_id = bench_set[0]

            self._is_initialized = True

            return self._benchmark_set_id

    def initialize_benchmark_run(self, params: BenchmarkParameters, iteration: int) -> DuckDBRunCursor:
        """
        initializes a benchmark run in the database
        :param params: the benchmark parameters object
        :param iteration: the iteration of the run
        :return:
        """
        param_id = self.get_id_of_parameter(params)

        with self.get_cursor() as conn:
            run = conn \
                .execute(
                f"insert into benchmark_run (parameters, benchmark_set, iteration) values ({param_id}, {self._benchmark_set_id}, {iteration}) returning *"
            ).fetchall()[0]

            print(f"created benchmark run: {run}")
            return DuckDBRunCursor(self._connection, run[0])

    def register_file(self, file: DataLocation, params: BenchmarkParameters, workload: dict, extent: Polygon):
        """
        inserts a file into the database
        :param file: the data location
        :param params: the benchmark parameters object
        :return:
        """
        if params.align_crs_at_stage == Stage.EXECUTION:
            crs = params.raster_target_crs if params.align_to_crs == DataType.RASTER else params.vector_target_crs
        elif params.align_to_crs == DataType.RASTER:
            crs = params.raster_target_crs
        elif params.align_to_crs == DataType.VECTOR:
            crs = params.vector_target_crs

        if params.system == System.POSTGIS:
            system = System.POSTGIS
        elif params.system == System.RASDAMAN and isinstance(file, RasterLocation):
            system = System.RASDAMAN
        else:
            system = "filesystem"

        with self._connection.cursor() as conn:
            dataset = conn.execute("""insert into available_files (name, location, crs, extent, datatype, filter_predicate, vector_simplify, raster_resolution, raster_depth, raster_tile_size, system)
                                      values (?, ?, ?, ?, ?, ?, ?) returning id""",
                                   [file.name, [str(f) for f in file.docker_file], crs, extent, file.target_suffix, workload.get("condition", {}).get("vector", {}),
                                    params.vector_simplify, params.raster_resolution, params.raster_depth, params.raster_tile_size.__dict__, system]).fetchone()

            print(f"registered file: {dataset}")

            file.uuid = dataset[0]

    def delete_file_by_uuid(self, uuid: str):
        """
        deletes a file from the database by its uuid
        :param uuid: the uuid of the file
        :return:
        """
        with self._connection.cursor() as conn:
            conn.execute("delete from available_files where id = ?", [uuid])

    def get_available_files_by_name(self, name: str) -> DataFrame:
        """
        returns a dataframe of all available files in the database
        :return:
        """
        with self._connection.cursor() as conn:
            return conn.execute("select * from available_files where name = ?", [name]).fetch_df()

    def close_connection(self):
        """
        closes the connection
        :return:
        """
        self._connection.close()

    def __del__(self):
        self._connection.close()


class DuckDBRunCursor:
    """
    a duckdb cursor to allow slightly parallel data entry
    """
    def __init__(self, connection: DuckDBPyConnection, run_id: int):
        """

        :param connection: the duckdb connection
        :param run_id: the run id
        """
        self._connection = connection
        self._run_id = run_id



    def write_timings_marker(self, marker: str):
        """
        inserts a timings marker into the database
        :param marker: the timings string
        :return:
        """
        marker, timestamp, event, stage, system, dataset, comment = tuple(marker.split(","))

        with self._connection.cursor() as conn:
            conn.execute("insert into timings values (?, ?, ?, ?, ?, ?, ?, ?)",
                         [
                             self._run_id,
                             marker,
                             datetime.fromtimestamp(float(timestamp)) if timestamp else None,
                             event.strip(),
                             stage.strip(),
                             dataset.strip(),
                             comment.strip(),
                             datetime.now()
                         ])

    def add_resource_utilization(self, util_files: list[Path]):
        """
        inserts a set of resource utilization file into the database
        :param util_files: the resource utilization files
        :return:
        """
        for f in util_files:
            stage = f.stem
            util_df = pd.read_csv(f, delimiter="\t")
            parsed_util_df = self._parse_docker_stats(util_df)
            parsed_util_df.insert(loc=0, column="run_id", value=self._run_id)
            parsed_util_df["stage"] = stage
            with self._connection.cursor() as conn:
                conn.execute("insert into resource_util select * from parsed_util_df")

    def add_results_file(self, filename: Path) -> (Path, bool):
        """
        inserts the location to a results file into the database
        :param filename: the filename
        :return: the path and the information, whether it is empty or not
        """
        match filename.suffixes[0].split("-"):
            case [".cold"]:
                warm_start_no = 0
            case [".warm", *no]:
                warm_start_no = int(no[0])
            case _:
                raise Exception("could not find info on run type in results file name")

        linecount = int(subprocess.run(f"wc -l {filename} | " + "awk '{ print $1 }'",
                                       shell=True, capture_output=True).stdout.decode("utf-8").strip()) \
            if filename.exists() else 0

        with self._connection.cursor() as conn:
            conn.execute("insert into results values (?, ?, ?, ?, ?)",
                         [self._run_id, warm_start_no, str(filename), linecount, filename.exists()])

        return filename, linecount > 0

    @staticmethod
    def _parse_docker_stats(util_df: DataFrame):
        """
        a helper class that parses docker stats strings
        :param util_df:
        :return:
        """
        util_df.replace("--", np.NAN, inplace=True)
        util_df.dropna(inplace=True, axis=0)
        util_df[["MemUsage", "MemLimit"]] = util_df["MemUsage"].str.split(" / ", expand=True)
        util_df[["NetIO_in", "NetIO_out"]] = util_df["NetIO"].str.split(" / ", expand=True)
        util_df[["BlockIO_in", "BlockIO_out"]] = util_df["BlockIO"].str.split(" / ", expand=True)

        util_df["timestamp_host"] = pd.to_datetime(util_df["timestamp"] * (10 ** 9), unit="ns")
        util_df["CPUUsage"] = util_df["CPUPerc"].str.rstrip(" %").astype("float") / 100
        util_df["MemUsage"] = util_df["MemUsage"].apply(DuckDBRunCursor._convert_to_bytes)
        util_df["MemLimit"] = util_df["MemLimit"].apply(DuckDBRunCursor._convert_to_bytes)
        util_df["NetIO_in"] = util_df["NetIO_in"].apply(DuckDBRunCursor._convert_to_bytes)
        util_df["NetIO_out"] = util_df["NetIO_out"].apply(DuckDBRunCursor._convert_to_bytes)
        util_df["BlockIO_in"] = util_df["BlockIO_in"].apply(DuckDBRunCursor._convert_to_bytes)
        util_df["BlockIO_out"] = util_df["BlockIO_out"].apply(DuckDBRunCursor._convert_to_bytes)

        util_df["PIDs"] = util_df["PIDs"].astype("int")

        # util_df.drop(["timestamp", "CPUPerc", "NetIO", "BlockIO", "MemPerc"], inplace=True, axis=1)
        out_df = util_df[
            ["timestamp_host", "ID", "Name", "CPUUsage", "MemUsage", "MemLimit", "NetIO_in", "NetIO_out", "BlockIO_in",
             "BlockIO_out", "PIDs"]]

        return out_df

    @staticmethod
    def _convert_to_bytes(value: str) -> int:
        """
        converts a suffixed value to byres
        :param value:
        :return:
        """
        if "e" in value:
            regex_exp = re.compile("e\+(\d+)")
            exp = int(regex_exp.search(value).group(1))
            value = regex_exp.sub("0" * exp, value)
        regex_str = re.compile("([0-9.]+)([PpTtGgMmKk]?)(i?)([Bb])")
        match = regex_str.match(value)
        val, factor_str, base_str, unit = match.group(1, 2, 3, 4)
        factor_str = "B" if factor_str == "" else factor_str.upper()

        base = 1024 if "i" in base_str else 1000
        factor = {
            "P": base ** 5,
            "T": base ** 4,
            "G": base ** 3,
            "M": base ** 2,
            "K": base ** 1,
            "B": base ** 0
        }[factor_str]

        return int(float(val) * factor)
