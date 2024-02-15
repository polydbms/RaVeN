from pathlib import Path

import yaml

from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.benchmarkrun.factory import BenchmarkRunFactory
from hub.benchmarkrun.host_params import HostParameters
from hub.configuration import PROJECT_ROOT
from hub.zsresultsdb.init_duckdb import InitializeDuckDB
from hub.enums.datatype import DataType
from hub.utils.capabilities import Capabilities
from hub.utils.datalocation import DataLocation
from hub.utils.system import System


class FileIO:
    pass

    @staticmethod
    def read_experiments_config(experiments_filename: str, controller_config_filename: str,
                                system=None) -> tuple[list[BenchmarkRun], int]:
        """
        loads the experiment config based on a workload file
        :param experiments_filename: the location of the experiment file
        :param controller_config_filename: the location of the controller config
        :param system: the system, if the benchmark shall only be run for a single system
        :return: a set of benchmark runs and their corresponding IDs in the database
        """

        with PROJECT_ROOT.joinpath(experiments_filename).open(mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)
                experiments = yamlfile["experiments"]
                workload = experiments["workload"]
                parameters = experiments.get("parameters", {})
                data = experiments["data"] if "data" in experiments else None

                system = [system] if system else []

                runs_no_dupes, iterations = FileIO.create_configs(data, experiments, experiments_filename, system,
                                                                  FileIO.get_systems(experiments_filename),
                                                                  workload, controller_config_filename, parameters)

                host_params = FileIO.get_host_params(controller_config_filename)
                InitializeDuckDB(host_params.controller_db_connection, runs_no_dupes, experiments_filename)

                return runs_no_dupes, iterations
            except yaml.YAMLError as exc:
                print(exc)
                return [], -1

    @staticmethod
    def create_configs(data, experiments, experiments_filename, selected_systems, all_systems, workload,
                       controller_config_filename, parameters):
        capabilities = Capabilities.read_capabilities()

        host_params = FileIO.get_host_params(controller_config_filename)
        systems = all_systems
        if len(selected_systems) > 0:
            systems = list(
                filter(lambda s: s.name.lower() in list(map(lambda sel: sel.lower(), selected_systems)), systems))

        iterations = int(experiments.get("iterations", 1))
        warm_starts = int(experiments.get("warm_starts", 0))
        timeout = int(experiments.get("timeout", 60 * 60 * 3))

        runs = []
        brf = BenchmarkRunFactory(capabilities)

        benchmark_params_raw = brf.create_params_iterations(systems, parameters)
        for benchmark_params in benchmark_params_raw:
            raster_dl = DataLocation(data["raster"], DataType.RASTER, host_params, benchmark_params)
            vector_dl = DataLocation(data["vector"], DataType.VECTOR, host_params, benchmark_params)

            benchmark_params.adjust_by_capabilities(capabilities, vector_dl, raster_dl)

            benchmark_params.validate(capabilities)

            runs.append(BenchmarkRun(
                raster_dl,
                vector_dl,
                workload,
                host_params,
                benchmark_params,
                Path(experiments_filename).parts[-1],
                warm_starts,
                timeout
            ))

        runs_no_dupes = list(set(runs))

        return runs_no_dupes, iterations

    @staticmethod
    def get_host_params(config_filename: str) -> HostParameters:
        """
        loads the hsot parameters from a file
        :param config_filename: the location of the host parameter file
        :return:
        """
        with PROJECT_ROOT.joinpath(config_filename).open(mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)

                return [HostParameters(h["host"],
                                       h["ssh_config_path"],
                                       Path(h["base_path"]),
                                       yamlfile["config"]["controller"]["results_folder"],
                                       Path(yamlfile["config"]["controller"]["results_db"]).expanduser())
                        for h in yamlfile["config"]["hosts"]][0]  # FIXME remove [0] eventually

            except yaml.YAMLError as exc:
                raise Exception(f"error while processing host parameters: {exc}")

    @staticmethod
    def get_systems(filename) -> list[System]:
        """
        returns all listed systems in the workload file
        :param filename: the loaction of the workload file
        :return: a list of Systems
        """
        with open(PROJECT_ROOT.joinpath(filename), mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)
                return [System(s["name"], int(s.get("port", 80))) for s in yamlfile["experiments"]["systems"]]
            except yaml.YAMLError as exc:
                print(exc)
                return []
