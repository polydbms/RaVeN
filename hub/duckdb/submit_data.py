import re
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection
from pandas import DataFrame

from hub.benchmarkrun.benchmark_params import BenchmarkParameters


class DuckDBConnector:
    _connection: DuckDBPyConnection

    def __init__(self, db_filename: Path):
        self._connection = duckdb.connect(database=str(db_filename), read_only=False)
        self._benchmark_set_id = -1
        self._is_initialized = False

    def get_cursor(self) -> DuckDBPyConnection:
        return self._connection.cursor()

    def get_id_of_parameter(self, param: BenchmarkParameters) -> int:
        with self.get_cursor() as c:
            param_df = pd.DataFrame([param.__dict__]).fillna('')
            param_df_cleaned = c.execute("select * from param_df").fetch_df()
            all_params = c.execute("select * from parameters").fetch_df().set_index("id").drop_duplicates(keep="last")
            all_params_dupes = pd.concat([all_params, param_df_cleaned])
            param_db_row = all_params_dupes.duplicated(keep=False)[lambda x: x]

            return int(param_db_row.head(1).index[0])

    def initialize_benchmark_set(self, experiment: str):
        if not self._is_initialized:
            with self.get_cursor() as conn:
                bench_set = conn.execute("insert into benchmark_set (experiment, exec_start) values (?, ?) returning *",
                                         [experiment, datetime.now()]).fetchall()[0]

                print(f"created benchmark set: {bench_set}")
                self._benchmark_set_id = bench_set[0]

            self._is_initialized = True

    def initialize_benchmark_run(self, params: BenchmarkParameters, iteration: int):
        param_id = self.get_id_of_parameter(params)

        with self.get_cursor() as conn:
            run = conn \
                .execute(
                f"insert into benchmark_run (parameters, benchmark_set, iteration) values ({param_id}, {self._benchmark_set_id}, {iteration}) returning *"
            ).fetchall()[0]

            print(f"created benchmark run: {run}")
            return DuckDBRunCursor(self._connection, run[0])

    # def __del__(self):
    #     self._connection.close()


class DuckDBRunCursor:
    def __init__(self, connection: DuckDBPyConnection, run_id: int):
        self._connection = connection
        self._run_id = run_id

    def write_timings_marker(self, marker: str):
        marker, timestamp, event, stage, system, dataset, comment = tuple(marker.split(","))

        with self._connection.cursor() as conn:
            conn.execute("insert into timings values (?, ?, ?, ?, ?, ?, ?, ?)",
                         [
                             self._run_id,
                             marker,
                             datetime.fromtimestamp(float(timestamp)),
                             event.strip(),
                             stage.strip(),
                             dataset.strip(),
                             comment.strip(),
                             datetime.now()
                         ])

    def add_resource_utilization(self, util_files: list[Path]):
        for f in util_files:
            stage = f.stem
            util_df = pd.read_csv(f, delimiter="\t")
            parsed_util_df = self._parse_docker_stats(util_df)
            parsed_util_df.insert(loc=0, column="run_id", value=self._run_id)
            parsed_util_df["stage"] = stage
            with self._connection.cursor() as conn:
                conn.execute("insert into resource_util select * from parsed_util_df")

    def add_results_file(self, filename: Path):
        match filename.suffixes[0].split("-"):
            case [".cold"]:
                warm_start_no = 0
            case [".warm", *no]:
                warm_start_no = int(no[0])
            case _:
                raise Exception("could not find info on run type in results file name")

        with self._connection.cursor() as conn:
            conn.execute("insert into results values (?, ?, ?)", [self._run_id, warm_start_no, str(filename)])

    @staticmethod
    def _parse_docker_stats(util_df: DataFrame):
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
