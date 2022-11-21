from pathlib import Path

import yaml

from configuration import PROJECT_ROOT
from hub.benchmarkrun.factory import BenchmarkRunFactory
from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.utils.capabilities import Capabilities
from hub.utils.datalocation import DataLocation, DataType
from hub.utils.system import System


class FileIO:

    @staticmethod
    def read_experiments_config(filename, system="") -> list[BenchmarkRun]:
        capabilities = Capabilities.read_capabilities()

        with open(PROJECT_ROOT.joinpath(filename), mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)
                experiments = yamlfile["experiments"]
                workload = experiments["workload"]
                data = experiments["data"] if "data" in experiments else None

                host_params = FileIO.get_host_params(filename)
                parameters = experiments.get("parameters", {})
                systems = [s for s in FileIO.get_systems(filename) if s.name == system or system == ""]

                runs = []
                brf = BenchmarkRunFactory(capabilities)

                for benchmark_params in brf.create_params_iterations(systems, parameters):

                    raster_dl = DataLocation(data["raster"], DataType.RASTER, host_params, benchmark_params)
                    vector_dl = DataLocation(data["vector"], DataType.VECTOR, host_params, benchmark_params)

                    if benchmark_params.vector_target_crs is None:
                        benchmark_params.vector_target_crs = vector_dl.get_crs()

                    if benchmark_params.vector_target_format is None:
                        vector_target_format = RasterFileType.TIFF \
                            if benchmark_params.system.name in capabilities["rasterize"] \
                            else vector_dl.suffix

                        benchmark_params.vector_target_format = vector_target_format
                        vector_dl.target_suffix = vector_target_format

                    if benchmark_params.raster_target_crs is None:
                        benchmark_params.raster_target_crs = raster_dl.get_crs()

                    if benchmark_params.raster_target_format is None:
                        raster_target_format = VectorFileType.SHP \
                            if benchmark_params.system.name in capabilities["vectorize"] \
                            else raster_dl.suffix

                        benchmark_params.raster_target_format = raster_target_format
                        raster_dl.target_suffix = raster_target_format

                    benchmark_params.validate(capabilities)

                    runs.append(BenchmarkRun(
                        raster_dl,
                        vector_dl,
                        workload,
                        host_params,
                        benchmark_params
                    ))

                return runs
            except yaml.YAMLError as exc:
                print(exc)
                return []

    @staticmethod
    def get_host_params(filename) -> HostParameters:
        with open(PROJECT_ROOT.joinpath(filename), mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)

                return [HostParameters(h["host"],  # TODO rename to "host"
                                       h["public_key_path"],
                                       Path(h["base_path"]),
                                       yamlfile["experiments"]["controller"]["results_folder"])
                        for h in yamlfile["experiments"]["hosts"]][0]  # FIXME remove [0] eventually

            except yaml.YAMLError as exc:
                raise Exception(f"error while processing host parameters: {exc}")

    @staticmethod
    def get_systems(filename) -> list[System]:
        with open(PROJECT_ROOT.joinpath(filename), mode="r") as c:
            try:
                yamlfile = yaml.safe_load(c)
                return [System(s["name"], int(s.get("port", 80))) for s in yamlfile["experiments"]["systems"]]
            except yaml.YAMLError as exc:
                print(exc)
                return []
